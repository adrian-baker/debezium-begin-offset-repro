# Bug reproduction: Debezium Postgres loses a transaction's changes after a restart from its BEGIN

This repository reproduces a bug in Debezium's Postgres connector, reported as [debezium/dbz#3101](https://github.com/debezium/dbz/issues/3101). It contains a test and no fix. On an affected Debezium version, `./gradlew test` fails, and the failure is the bug.

## The bug

The Postgres connector can store a transaction's BEGIN with an `lsn_proc` that points at a change inside that transaction. If the connector stops right after committing that offset, the next run skips the changes and sends only the transaction's END. This happens when the stream started partway through the transaction, which `offset.mismatch.strategy=trust_slot` with `lsn.flush.mode=connector_and_driver` allows.

[`BeginOffsetRestartTest`](src/test/java/repro/BeginOffsetRestartTest.java) shows it with the embedded engine and an embedded PostgreSQL 17:

1. Run 1 streams one row, commits its offset and stops.
2. With nothing connected, one transaction inserts r1 and r2. The test advances the slot to one of them with `pg_replication_slot_advance`.
3. Run 2 starts from the slot, because it's ahead of the stored offset. It marks the BEGIN processed, then throws on r1, so the BEGIN's offset is the last one committed.
4. Run 3 should send r1 and r2 before the END.

## Running it

```
./gradlew test
./gradlew test -PdebeziumVersion=3.7.1.Final
```

The `slot confirmed at "r2"` case fails while the bug is present. The `slot confirmed at "r1"` case is a control and passes.

You need a JDK. There's no Docker: the Postgres binaries come from Maven Central, for macOS and Linux on x86-64 and arm64. Each run writes the connector's TRACE log to `build/debezium-<version>.log`.

## Results

On macOS (arm64), JDK 25, PostgreSQL 17:

| Debezium | Slot at r1 | Slot at r2 |
| --- | --- | --- |
| 3.6.3.Final | r1 and r2, END `event_count` 2 | no changes, END `event_count` 0 |
| 3.7.1.Final | r1 and r2, END `event_count` 2 | no changes, END `event_count` 0 |
| 3.8.0.Alpha1 | r1 and r2, END `event_count` 2 | no changes, END `event_count` 0 |

With the slot at r2, run 2 stores the BEGIN with `lsn_proc` set to r2's LSN, where the stream started. Run 3's `WalPositionLocator` finds r2 and resumes at the COMMIT, so r1 and r2 are filtered out. BEGIN and COMMIT always get through, so the END still arrives. [`run-3.8.0.Alpha1-excerpt.log`](run-3.8.0.Alpha1-excerpt.log) has that part of the log.
