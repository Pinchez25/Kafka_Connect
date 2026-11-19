# Milestone 5 — Kafka Streams: Processor API

Goal: drop below the DSL. Every DSL operator compiles down to processors, state stores and
the records passed between them. Here you write those pieces yourself, for logic the DSL
can't express cleanly.

| Feature | Class | API |
|---------|-------|-----|
| Deduplication of CDC rows | `processor/DeduplicationProcessor` | `FixedKeyProcessor` via `processValues`, a window store you build, a custom metric |
| Dormant accounts | `processor/DormantAccountProcessor` | `Processor` via `process`, a punctuator, a DSL store (`balances`) connected by name |
| Unusual amounts | `topology/fraud/UnusualAmountRule` + `processor/UnusualAmountProcessor` | `Topology#addSource/addProcessor/addStateStore/addSink`, forwarding to a named child |
| Headers | `processor/PesaflowHeaders`, `AlertHeadersProcessor` | reading Debezium headers, writing your own, `recordMetadata()` |

New topics: `pesaflow.accounts.dormant` and `pesaflow.fraud.review`. New endpoint:
`GET /accounts/{id}/last-activity`.

## 1. Run it

If you're upgrading a running Milestone 4 app, **reset it first** (section 3 explains why):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-streams-application-reset.sh \
  --bootstrap-server localhost:9092 --application-id pesaflow-streams --force \
  --input-topics pesaflow.transactions,pesaflow.accounts,pesaflow.merchants --to-earliest
docker compose exec kafka /opt/kafka/bin/kafka-streams-application-reset.sh \
  --bootstrap-server localhost:9092 --application-id pesaflow-streams --force \
  --input-topics pesaflow.transactions.enriched --to-latest
rm -rf pesaflow-streams/build/kafka-streams
```

Then the usual:

```bash
docker compose up -d
./gradlew :wallet-api:bootRun
curl -X POST localhost:8080/load-generator/start
./connect/connect.sh apply          # the source connector now adds headers
./gradlew :pesaflow-streams:bootRun
```

## 2. How the Processor API plugs in

There are three ways to use it, and this milestone uses all three:

- **`processValues(FixedKeyProcessorSupplier)`**: a processor in the middle of a DSL
  chain that can't change the key. Kafka Streams knows the key is untouched, so no
  repartition follows. Deduplication is one of these.
- **`process(ProcessorSupplier, storeNames...)`**: a processor that may change keys, and
  forwards whatever it likes, whenever it likes (including from a punctuator). Dormancy is one.
- **`Topology#addSource/addProcessor/addStateStore/addSink`**: no DSL at all. Spring builds
  the DSL part, then `KafkaStreamsConfiguration` adds `UnusualAmountRule` to the same
  `Topology` through a `KafkaStreamsInfrastructureCustomizer`.

A supplier can bring its own stores by overriding `stores()`. Kafka Streams adds and
connects them for you. That's how the dedup window store and the `last-activity` store are
registered. A store created by the DSL (`balances`) is connected by name instead: pass it to
`process(...)`. It must live in the same sub-topology. Here it does, because both the
aggregate and the dormancy processor read the same `ledger-entries-by-account` repartition
topic. That's why `LedgerTopology` now repartitions explicitly with `repartition()`: two
consumers share it. The topic name is the one `groupByKey` used before, so existing data
still lines up.

## 3. Deduplication

Debezium is at-least-once by nature. A Connect crash, a replication-slot replay or an
incremental snapshot can all deliver a transaction row again. Without protection, a balance
counts that money twice.

`DeduplicationProcessor` keeps transaction ids in a **window store** keyed by the row's event
time (`created_at`). A redelivered row has the same id and the same `created_at`, so one
`fetch(id, t, t)` finds it. Retention is `pesaflow.streams.deduplication.retention` (7 days).
Old segments are dropped automatically, with no cleanup code. Dropped duplicates are
counted by a **custom metric** registered through `context.metrics()`:

```bash
curl -s localhost:8082/actuator/metrics/kafka.stream.pesaflow.deduplication.duplicates.dropped.total | jq
```

Make real duplicates with an incremental snapshot of the whole transactions table:

```bash
./connect/connect.sh snapshot transactions
```

Every row comes out again with `__op: r`, and the metric rises by the row count. Then check
a few balances against Postgres (the SQL is in Milestone 3). They still match.

Two things this taught us when it was first deployed:

- **A new store starts empty.** Deploying the processor into a running app gives it no
  memory of transactions processed earlier. The first snapshot after deploying it only
  caught the ~2,400 transactions processed since the restart; the other 90,000 were
  counted twice. That's why section 1 resets the app. In production you'd backfill the
  store, or deploy dedup before anything can redeliver.
- **Retention is a promise with an end date.** The `dormant-account` scenario's top-up is
  stamped 40 days ago, older than the 7-day retention. Snapshot after running it, and that
  account's balance is 2,500 too high. Every other balance is correct.

## 4. Dormant accounts and punctuation

`DormantAccountProcessor` records each account's latest ledger entry in `last-activity`.
A **punctuator** runs every `scan-interval`, scans the store, and forwards a
`DormantAccount` for anything quiet for longer than `inactivity` (30 days). It includes the
balance, read from the DSL's `balances` store. Each quiet spell is reported once; new
activity starts a new one.

```bash
curl -s -X POST localhost:8080/load-generator/scenarios/dormant-account | jq
# within a minute:
curl -s localhost:8082/accounts/<accountId>/last-activity | jq
```

The punctuation type is configurable (`dormant-account.punctuation-type`):

| Type | `now` is | Fires when |
|------|----------|------------|
| `STREAM_TIME` | the task's stream time | records arrive that move stream time past the next interval |
| `WALL_CLOCK_TIME` (default) | the system clock | the interval passes, whether or not anything arrives |

This rule is about the **absence** of events, so wall-clock time is the right choice. Stream
time only moves when something happens. `DormantAccountProcessorTest` shows both: advancing
the wall clock by 31 days does nothing under `STREAM_TIME`, and fires under `WALL_CLOCK_TIME`.

The scan is a full store iteration, so it costs O(accounts) every interval. That's fine at
this size. With millions of accounts you'd index by last-activity time instead.

## 5. Headers and record metadata

The source connector now adds Debezium headers (`"transforms.unwrap.add.headers":
"op,source.lsn"`), giving `__op` and `__source_lsn`. Debezium 3 also adds
`__debezium.context.*` headers on its own.

- `DeduplicationProcessor` reads `__op` when it logs a dropped duplicate (`r` for a
  snapshot). It also adds `pesaflow-source: <topic>-<partition>@<offset>`, using
  `context.recordMetadata()`.
- Headers **flow through stateless operators, repartitions and stream–table joins**. Every
  record in `pesaflow.transactions.enriched` still carries `__op`, `__source_lsn` and
  `pesaflow-source`:

  ```bash
  docker compose exec schema-registry kafka-avro-console-consumer --bootstrap-server kafka:29092 \
    --topic pesaflow.transactions.enriched --property schema.registry.url=http://localhost:8081 \
    --property print.headers=true \
    --property headers.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --max-messages 1 2>/dev/null | grep '{'
  ```

- Aggregation results carry the headers of **whichever record triggered them**, which is
  meaningless for a count over a window. So `AlertHeadersProcessor` replaces them on every
  fraud alert with `pesaflow-fraud-rule`, `pesaflow-alert-id` and `pesaflow-severity`.
  Downstream consumers (Milestone 6) can route on headers without deserializing the value.

## 6. A rule with no DSL: unusual amounts

`UnusualAmountRule` reads `pesaflow.transactions.enriched` (written by the DSL part of the same
app) and compares each outgoing amount with that account's own average so far, kept in
`spending-baselines`. After at least 10 transactions:

- **≥ 20× the average**: forwarded to the child named `unusual-amount-alerts-sink`, which
  writes to `pesaflow.fraud.alerts`
- **≥ 10×**: forwarded to `unusual-amount-review-sink`, which writes to `pesaflow.fraud.review`

```bash
curl -s -X POST localhost:8080/load-generator/scenarios/unusual-amount | jq
```

The scenario pauses 2 seconds before the large payments, and the reason is the most
important lesson in this milestone. **Kafka only orders records within a partition.** CDC
records are keyed by transaction id, so one account's payments are spread across all three
partitions of `pesaflow.transactions`. When they're re-keyed by account, partitions are
processed side by side and the account's payments can arrive out of order. Without the
pause, the 2,500 payment often arrived fifth and was compared against a history of 4: no
alert. The windowed rules from Milestone 4 don't care, because windows are placed by event
time. This processor uses arrival order, so it does.

## Experiments

1. **Describe both styles.** Compare the `velocity-*` processors in the logged
   `topology.describe()` with `unusual-amount-source → detect-unusual-amount → two sinks`.
   For each DSL operator, find the processor and store it generated. Notice that the
   hand-written rule is its own sub-topology. Why?
2. **Stream time vs wall clock.** Start with
   `--pesaflow.streams.dormant-account.inactivity=2m` and
   `--pesaflow.streams.dormant-account.punctuation-type=stream-time`. Run `dormant-account`,
   then stop the load generator. Nothing more is reported, however long you wait. Restart
   with `wall-clock-time` and do it again.
3. **Throw in a punctuator.** Add `throw new IllegalStateException()` to
   `reportDormantAccounts`. Watch the stream thread die, and the client shut down
   (`SHUTDOWN_CLIENT` is the default). Milestone 7 replaces the thread instead.
4. **Remove the ordering pause.** Delete `letHabitArriveFirst()` from `FraudScenarioService`
   and run `unusual-amount` a few times. Count how often the alert is missing. How would
   you make the rule order-insensitive? (Hint: buffer by event time, or key the CDC topic
   differently.)
5. **Dedup without a reset.** Deploy a change that renames the dedup store, then snapshot
   `transactions` and compare balances with Postgres.
6. **Headers on the wire.** In Kafka UI, open a record in `pesaflow.fraud.alerts` and one in
   `pesaflow.balances`. Which Debezium headers survived the aggregation, and why
   shouldn't you rely on them?

## Checklist

- [ ] You can say when to use `processValues` vs `process` vs the `Topology` builder
- [ ] You can connect a DSL store to a processor and know why it must be in the same sub-topology
- [ ] You can explain why stream-time punctuation can't detect silence
- [ ] You've made real duplicates with a snapshot and watched them dropped
- [ ] You can explain why per-key ordering broke after re-keying
