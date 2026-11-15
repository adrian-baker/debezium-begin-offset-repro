package repro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A run that commits the offset of a transaction's BEGIN, and stops before the transaction's
 * first change, should leave every change of that transaction to the next run.
 *
 * <p>The run's first BEGIN is stored with lsn_proc equal to the stream's start LSN. With
 * offset.mismatch.strategy=trust_slot the stream starts at the slot's confirmed_flush_lsn.
 * The test confirms the slot at one of the transaction's two changes, as a keepalive flush
 * under lsn.flush.mode=connector_and_driver can. With the slot at r2, the next run skips both
 * changes and emits the transaction's END alone. With the slot at r1 it delivers both.
 */
class BeginOffsetRestartTest {

    private static final Logger log = LoggerFactory.getLogger(BeginOffsetRestartTest.class);
    private static final String SLOT = "repro_slot";
    private static final String PUBLICATION = "repro_pub";
    private static final String PREFIX = "repro";
    private static final Duration WAIT = Duration.ofSeconds(60);

    private static EmbeddedPostgres postgres;

    @TempDir
    Path dir;

    @BeforeAll
    static void startPostgres() throws Exception {
        postgres = EmbeddedPostgres.builder()
                .setServerConfig("wal_level", "logical")
                .setServerConfig("max_replication_slots", "4")
                .setServerConfig("max_wal_senders", "4")
                // The embedded server turns this off. Logical decoding reads only flushed WAL,
                // so a commit has to flush before the slot can see it.
                .setServerConfig("synchronous_commit", "on")
                .start();
        sql("CREATE TABLE t (id text PRIMARY KEY)");
        sql("CREATE PUBLICATION " + PUBLICATION + " FOR TABLE t");
    }

    @AfterAll
    static void stopPostgres() throws Exception {
        postgres.close();
    }

    @BeforeEach
    void freshSlot() throws Exception {
        sql("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '" + SLOT + "'");
        sql("TRUNCATE t");
        sql("SELECT pg_create_logical_replication_slot('" + SLOT + "', 'pgoutput')");
    }

    @ParameterizedTest(name = "slot confirmed at {0}")
    @ValueSource(strings = {"r1", "r2"})
    void aRestartAfterACommittedBeginDeliversTheTransactionsChanges(String slotAt) throws Exception {
        log.info("Debezium {}, slot confirmed at {}", System.getProperty("debezium.version"), slotAt);

        // Run 1 streams one transaction and commits its offset, so later runs start from a
        // stored offset that is behind the slot.
        sql("INSERT INTO t VALUES ('r0')");
        try (Run run = Run.start(properties(), record -> {})) {
            run.awaitRecord(BeginOffsetRestartTest::isChange);
        }

        // The walsender of run 1 can outlive the engine for a moment. It would confirm the
        // next transaction if it saw it, and the slot can't be advanced while it's active.
        String slotActive = "SELECT active::text FROM pg_replication_slots WHERE slot_name = '" + SLOT + "'";
        for (int tries = 1; "true".equals(stringOf(slotActive)); tries++) {
            if (tries == 100) {
                throw new AssertionError("Run 1 still holds the slot");
            }
            Thread.sleep(100);
        }

        // A two-row transaction, written while nothing streams.
        long xid;
        try (Connection connection = postgres.getPostgresDatabase().getConnection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("INSERT INTO t VALUES ('r1')");
                statement.execute("INSERT INTO t VALUES ('r2')");
                xid = longOf(statement, "SELECT txid_current()");
            }
            connection.commit();
        }
        String peek = "pg_logical_slot_peek_binary_changes('" + SLOT
                + "', NULL, NULL, 'proto_version', '1', 'publication_names', '" + PUBLICATION + "')";
        log.info("The slot holds {}", stringOf("SELECT string_agg(lsn::text || ' ' || chr(get_byte(data, 0)), ', ') FROM " + peek));

        // Confirm the slot at one of the changes, where a keepalive flush can leave it.
        String change = stringOf("SELECT lsn::text FROM " + peek + " WHERE xid = " + xid
                + " AND get_byte(data, 0) = ascii('I') ORDER BY lsn LIMIT 1 OFFSET " + (slotAt.equals("r1") ? 0 : 1));
        sql("SELECT pg_replication_slot_advance('" + SLOT + "', '" + change + "')");
        log.info("Transaction {}: {} at {} ({}); slot confirmed at {}", xid, slotAt, change,
                longOf("SELECT ('" + change + "'::pg_lsn - '0/0'::pg_lsn)::bigint"),
                stringOf("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = '" + SLOT + "'"));

        // Run 2 commits the BEGIN's offset and stops at r1 by failing the batch that holds it.
        List<SourceRecord> secondRun = new CopyOnWriteArrayList<>();
        try (Run run = Run.start(properties(), record -> {
            if (isChange(record)) {
                throw new IllegalStateException("Refusing " + idOf(record) + " to stop the run");
            }
            secondRun.add(record);
        })) {
            run.awaitStop();
        }
        SourceRecord begin = secondRun.stream().filter(r -> "BEGIN".equals(statusOf(r))).findFirst().orElseThrow();
        log.info("Run 2 committed the BEGIN with offset {}", begin.sourceOffset());

        // Run 3 should deliver r1 and r2 before the transaction's END.
        List<String> changes = new ArrayList<>();
        Struct end;
        try (Run run = Run.start(properties(), record -> {})) {
            end = (Struct) run.awaitRecord(r -> "END".equals(statusOf(r)) && idOf(r).startsWith(xid + ":")).value();
            run.records().stream().filter(BeginOffsetRestartTest::isChange).map(BeginOffsetRestartTest::idOf)
                    .forEach(changes::add);
        }
        log.info("Run 3 delivered changes {}, then END with event_count {}", changes, end.get("event_count"));
        assertTrue(changes.containsAll(List.of("r1", "r2")),
                "Run 3 emitted the END of transaction " + xid + " with event_count " + end.get("event_count")
                        + " but delivered only " + changes);
        assertEquals(2L, end.getInt64("event_count"));
    }

    private Properties properties() {
        Properties properties = new Properties();
        properties.setProperty("name", "repro");
        properties.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        properties.setProperty("database.hostname", "localhost");
        properties.setProperty("database.port", Integer.toString(postgres.getPort()));
        properties.setProperty("database.dbname", "postgres");
        properties.setProperty("database.user", "postgres");
        properties.setProperty("database.password", "postgres");
        properties.setProperty("topic.prefix", PREFIX);
        properties.setProperty("plugin.name", "pgoutput");
        properties.setProperty("slot.name", SLOT);
        properties.setProperty("publication.name", PUBLICATION);
        properties.setProperty("publication.autocreate.mode", "disabled");
        properties.setProperty("snapshot.mode", "no_data");
        properties.setProperty("provide.transaction.metadata", "true");
        properties.setProperty("lsn.flush.mode", "connector_and_driver");
        properties.setProperty("offset.mismatch.strategy", "trust_slot");
        properties.setProperty("max.batch.size", "1");
        properties.setProperty("errors.max.retries", "0");
        properties.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        properties.setProperty("offset.storage.file.filename", dir.resolve("offsets.dat").toString());
        properties.setProperty("offset.flush.interval.ms", "0");
        return properties;
    }

    private static boolean isChange(SourceRecord record) {
        return record.topic().equals(PREFIX + ".public.t");
    }

    private static String statusOf(SourceRecord record) {
        return record.topic().equals(PREFIX + ".transaction") ? ((Struct) record.value()).getString("status") : null;
    }

    private static String idOf(SourceRecord record) {
        Struct value = (Struct) record.value();
        return isChange(record) ? value.getStruct("after").getString("id") : value.getString("id");
    }

    private static void sql(String sql) throws SQLException {
        try (Connection connection = postgres.getPostgresDatabase().getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String stringOf(String sql) throws SQLException {
        try (Connection connection = postgres.getPostgresDatabase().getConnection();
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            result.next();
            return result.getString(1);
        }
    }

    private static long longOf(String sql) throws SQLException {
        return Long.parseLong(stringOf(sql));
    }

    private static long longOf(Statement statement, String sql) throws SQLException {
        try (ResultSet result = statement.executeQuery(sql)) {
            result.next();
            return result.getLong(1);
        }
    }

    /** An embedded engine that marks each record processed once the handler accepts it. */
    private static final class Run implements AutoCloseable {
        private final List<SourceRecord> records = new CopyOnWriteArrayList<>();
        private final CompletableFuture<Void> stopped = new CompletableFuture<>();
        private final DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;

        private Run(Properties properties, Consumer<SourceRecord> handler) {
            this.engine = DebeziumEngine.create(Connect.class)
                    .using(properties)
                    .notifying((batch, committer) -> {
                        for (ChangeEvent<SourceRecord, SourceRecord> event : batch) {
                            handler.accept(event.value());
                            records.add(event.value());
                            committer.markProcessed(event);
                        }
                        committer.markBatchFinished();
                    })
                    .using((success, message, error) -> {
                        log.info("Engine stopped: success={}, message={}", success, message);
                        stopped.complete(null);
                    })
                    .build();
        }

        static Run start(Properties properties, Consumer<SourceRecord> handler) {
            Run run = new Run(properties, handler);
            Thread.ofPlatform().name("engine").start(run.engine);
            return run;
        }

        List<SourceRecord> records() {
            return records;
        }

        SourceRecord awaitRecord(Predicate<SourceRecord> wanted) throws InterruptedException {
            long deadline = System.nanoTime() + WAIT.toNanos();
            while (System.nanoTime() < deadline) {
                for (SourceRecord record : records) {
                    if (wanted.test(record)) {
                        return record;
                    }
                }
                Thread.sleep(50);
            }
            throw new AssertionError("No such record within " + WAIT + "; received " + records);
        }

        void awaitStop() throws Exception {
            stopped.get(WAIT.toSeconds(), TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            if (!stopped.isDone()) {
                engine.close();
            }
            awaitStop();
        }
    }
}
