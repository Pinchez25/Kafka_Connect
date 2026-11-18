# Milestone 1 — Kafka Connect: CDC source

Goal: every committed change in the wallet database becomes an event in Kafka, without
touching the wallet code. You'll also learn how Connect stores its own state, and how
converters, Single Message Transforms (SMTs) and predicates reshape records on their way in.

## What's in place

| Piece | Where |
|-------|-------|
| Connect image: Debezium 3.6 + Confluent Avro converter | `connect/Dockerfile` |
| Raw connector: JSON, no transforms, for studying the envelope | `connect/connectors/pesaflow-postgres-raw.json` |
| Main connector: Avro, SMTs, predicates, signals, exactly-once | `connect/connectors/pesaflow-postgres-source.json` |
| REST helper | `connect/connect.sh` |
| Publication + signal table (Flyway) | `V4__create_cdc_publication.sql` |
| Second worker for rebalancing | `connect-2` service, `scale` profile |

## 1. Start everything

```bash
docker compose up -d --build
./gradlew bootRun
curl -X POST localhost:8080/load-generator/start
./connect/connect.sh apply
./connect/connect.sh list
```

`apply` PUTs each `connectors/<name>.json` to `/connectors/<name>/config`. That call is
idempotent: it creates the connector if it's missing and updates it otherwise. Run
`./connect/connect.sh` with no arguments to see the other commands.

### Workers, connectors and tasks

- A **worker** is a JVM process (the `connect` container). Workers that share a `group.id`
  form a cluster.
- A **connector** is a configuration plus a coordinator. It decides how to split the work.
- A **task** does the actual copying. Debezium Postgres always has exactly one task, because
  a replication slot is a single ordered stream.

In distributed mode, all cluster state lives in Kafka:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic connect-configs --from-beginning --formatter-property print.key=true
```

Look at `connect-status` and `connect-offsets` the same way.

## 2. The raw Debezium envelope

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic raw.public.accounts --from-beginning --max-messages 3 --formatter-property print.key=true
```

Each value is an envelope:

| Field | Meaning |
|-------|---------|
| `op` | `r` snapshot read, `c` create, `u` update, `d` delete |
| `before` / `after` | row state before and after the change |
| `source` | where it came from: `lsn`, `txId`, `snapshot`, `table`, `ts_ms` (commit time) |
| `ts_ms` (top level) | when Debezium processed it |

Things to work out:
- Freeze an account (`PUT /accounts/{id}/status`). Why is `before` null on the update? Run
  `ALTER TABLE accounts REPLICA IDENTITY FULL;`, freeze another account and compare.
- `daily_limit` looks like `"as/A"`. That's a `numeric` encoded as base64 bytes of the unscaled
  value. Debezium's `decimal.handling.mode` controls this.
- The key is `{"id": "..."}`, a struct, not a string. Why might a Kafka Streams app prefer a
  plain string?

## 3. The main connector, step by step

Read `pesaflow-postgres-source.json` alongside this section.

**Converters** turn Connect's internal records into bytes. Here both key and value use
`AvroConverter`, which registers each schema in Schema Registry and prefixes every message
with a 5-byte schema id:

```bash
curl -s localhost:8081/subjects | jq
curl -s localhost:8081/subjects/pesaflow.transactions-value/versions/latest | jq -r .schema | jq
docker compose exec schema-registry kafka-avro-console-consumer --bootstrap-server kafka:29092 \
  --topic pesaflow.transactions --from-beginning --max-messages 3 \
  --property schema.registry.url=http://localhost:8081 --property print.key=true
```

Note the Debezium semantic types in the schema: `io.debezium.data.Uuid`,
`io.debezium.time.ZonedTimestamp`, and `decimal` logical types with `precision` and `scale`.

**Transforms** run in order on every record:

| Transform | What it does |
|-----------|--------------|
| `unwrap` (`ExtractNewRecordState`) | replaces the envelope with just `after`, adds `__op` and `__source_ts_ms`, and turns deletes into tombstones |
| `route` (`RegexRouter`) | `pesaflow.public.accounts` → `pesaflow.accounts` |
| `dropSignals` (`Filter` + `TopicNameMatches`) | drops events from the signal table, which Debezium otherwise emits like any other table |
| `extractKey` (`ExtractField$Key` + `TopicNameMatches`) | key `{"id": "…"}` → `"…"` on table topics only, because heartbeat records have no `id` field and would fail |

**Topic creation** groups: `pesaflow.accounts` and `pesaflow.merchants` are created
**compacted**, since they hold the latest state per key and will become KTables.
`pesaflow.transactions` uses delete retention, because those are events.

**Masking**: `phone_number` is replaced with a salted SHA-256 hash by Debezium itself
(`column.mask.hash.v2…`), not by an SMT. The first version used `MaskField$Value` with a
`TopicNameMatches` predicate. It worked until an account was deleted: the tombstone has a
null value, `MaskField` threw `Only Struct objects supported`, and the task failed. A
transform takes only **one** predicate, so "accounts topic AND not a tombstone" can't be
expressed. Masking at the source also covers the snapshot and every operation. And because
the hash is deterministic, you can still detect two accounts sharing one phone number.

**Exactly-once source** (`exactly.once.support: required`, with
`exactly.once.source.support=enabled` on the worker): records and their source offsets are
written in a single Kafka transaction. A crash can no longer produce duplicates between
"record written" and "offset committed". Consumers only benefit if they read with
`isolation.level=read_committed`.

**Config provider**: `database.password` is `${env:PESAFLOW_DB_PASSWORD}`. The worker
resolves it from its environment, so the secret is never stored in `connect-configs`.
Check with `./connect/connect.sh config pesaflow-postgres-source`.

## 4. Incremental snapshot via the signal table

```sql
insert into debezium_signals (id, type, data)
values ('snapshot-merchants-' || now()::text, 'execute-snapshot',
        '{"data-collections": ["public.merchants"], "type": "incremental"}');
```

Watch `docker compose logs -f connect` and the `pesaflow.merchants` offsets. Debezium
re-reads the table in chunks while still streaming live changes. Watermarks in the signal
table let it de-duplicate a row that changes mid-snapshot. Compare with the **initial**
snapshot (`__op: r`), which runs once before streaming starts.

## Experiments

1. **Resume from offsets.** With load running: `docker compose stop connect`, wait 30 s,
   `docker compose start connect`. Did anything get lost or duplicated? Inspect the stored
   position with `./connect/connect.sh offsets pesaflow-postgres-source` and in the
   `connect-offsets` topic (the key is `["connector-name", {partition}]`).
2. **Schema evolution.**
   `ALTER TABLE accounts ADD COLUMN email varchar(120);`, then update an account. A new
   version of `pesaflow.accounts-value` appears. Why is it BACKWARD compatible? Now try
   `ALTER TABLE merchants ALTER COLUMN till_number TYPE integer USING till_number::integer;`
   and update a merchant. Read the task's error in `./connect/connect.sh status`. Revert both
   changes afterwards (`DROP COLUMN email`; change `till_number` back to `varchar(10)`),
   because Hibernate validates the schema on startup.
3. **Lifecycle.** `pause`, `resume`, `stop`, `restart` a connector and watch `status` after
   each. What's the difference between paused and stopped? (Hint: offsets can only be
   altered while stopped.)
4. **Rebalancing.** `docker compose --profile scale up -d connect-2`, then `list`. Stop
   `connect-2` and `list` again. The task stays `UNASSIGNED` for a while: that's incremental
   cooperative rebalancing waiting `scheduled.rebalance.max.delay.ms` (5 min) for the worker
   to come back.
5. **The forgotten slot.** Delete the raw connector (`./connect/connect.sh delete
   pesaflow-postgres-raw`). Its slot is still there:
   ```sql
   select slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as retained
   from pg_replication_slots;
   ```
   Keep the load generator running and re-run the query every minute. That WAL can never be
   recycled, and on a real server it eventually fills the disk. Fix it with
   `select pg_drop_replication_slot('pesaflow_raw');`. Postgres's
   `max_slot_wal_keep_size` setting is the safety net. Then think about what
   `heartbeat.interval.ms` solves: a slot on a quiet table in a busy database.
6. **Tombstones.** Open an account with no transactions, `DELETE` it in psql, and find both
   the delete event and the tombstone in `raw.public.accounts`, but only the tombstone in
   `pesaflow.accounts`. Why does compaction need the tombstone?

## Checklist

- [ ] You can explain worker vs connector vs task, and what each `connect-*` topic stores
- [ ] You can read a Debezium envelope and say which fields come from where
- [ ] You know what a converter does that an SMT doesn't, and vice versa
- [ ] You've seen a schema evolve and a compatibility check reject a change
- [ ] You've watched a slot retain WAL and cleaned it up
