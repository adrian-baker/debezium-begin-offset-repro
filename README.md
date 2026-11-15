# Debezium PostgreSQL: a restart from a BEGIN's offset skips the transaction's changes

A run of the PostgreSQL connector commits the offset of a transaction's BEGIN and stops before
the transaction's first change. The next run emits the transaction's END and none of its
changes. It happens when the stream started at a change inside that transaction, which
`offset.mismatch.strategy=trust_slot` with `lsn.flush.mode=connector_and_driver` allows.

[`BeginOffsetRestartTest`](src/test/java/repro/BeginOffsetRestartTest.java) runs the embedded
engine three times against an embedded PostgreSQL 17:

1. Run 1 streams one row and commits its offset, then stops.
2. With nothing streaming, one transaction inserts r1 and r2. The test confirms the slot at one
   of the two changes with `pg_replication_slot_advance`, standing in for a keepalive flush.
3. Run 2 starts from the slot, because it is ahead of the stored offset. It marks the BEGIN
   processed, then fails the batch holding r1, so the BEGIN's offset is the last one committed.
4. Run 3 should deliver r1 and r2 before the transaction's END.

## Running it

```
./gradlew test
./gradlew test -PdebeziumVersion=3.7.1.Final
```

It needs a JDK and no Docker. The PostgreSQL binaries come from Maven Central as a dependency,
for macOS and Linux on x86-64 and arm64. Each run writes the connector's TRACE log to
`build/debezium-<version>.log`.

## Results

Run on macOS (arm64) with JDK 25 and PostgreSQL 17.

| Debezium | Slot confirmed at r1 | Slot confirmed at r2 |
| --- | --- | --- |
| 3.6.3.Final | r1 and r2, END `event_count` 2 | no changes, END `event_count` 0 |
| 3.7.1.Final | r1 and r2, END `event_count` 2 | no changes, END `event_count` 0 |
| 3.8.0.Alpha1 | r1 and r2, END `event_count` 2 | no changes, END `event_count` 0 |

With the slot at r2, run 2 stores the BEGIN with `lsn_proc` equal to r2's LSN, the stream's
start LSN. Run 3 resumes from that offset. `WalPositionLocator` matches r2 and resumes at the
COMMIT, so r1 and r2 are filtered. BEGIN and COMMIT are always reprocessed, so the END still
arrives. [`run-3.8.0.Alpha1-excerpt.log`](run-3.8.0.Alpha1-excerpt.log) is that part of the
TRACE log for runs 2 and 3.
