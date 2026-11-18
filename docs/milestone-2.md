# Milestone 2 — Kafka Streams: stateless processing

Goal: a separate Kafka Streams application that turns raw CDC rows into clean domain events
and routes them by type, using Avro schemas end to end. Every operation here is
**stateless**: each record is handled on its own, with no memory of earlier records.

## What's in place

```
pesaflow/
├── buildSrc/                     convention plugins + GenerateAvroJava task
├── gradle/libs.versions.toml     every dependency version
├── wallet-api/                   Milestones 0–1 (no Kafka dependency at all)
└── pesaflow-streams/
    ├── src/main/avro/
    │   ├── cdc.avdl                    contract of what Connect publishes
    │   └── transaction-events.avdl     domain events this app publishes
    └── src/main/java/com/pinchez/streams/
        ├── config/      properties, Spring wiring, topology logger
        ├── serde/       Avro serde factory
        ├── mapper/      CDC row → domain event
        └── topology/    the stream processing itself
```

The topology:

```
pesaflow.transactions ──▶ filter(status == COMPLETED) ──▶ mapValues(to CompletedTransaction)
                                                              │
                                   split by type ─────────────┼──▶ pesaflow.transactions.p2p
                                                              ├──▶ pesaflow.transactions.top-up
                                                              └──▶ pesaflow.transactions.merchant-payment
```

## 1. Run it

```bash
docker compose up -d
./gradlew :wallet-api:bootRun                 # terminal 1
curl -X POST localhost:8080/load-generator/start
./connect/connect.sh apply
./gradlew :pesaflow-streams:bootRun           # terminal 2
```

The streams app logs its topology on startup (`topology.describe()`). Read it: sources,
processors and sinks, all named. Paste it into https://zz85.github.io/kafka-streams-viz/ to
see it drawn.

```bash
docker compose exec schema-registry kafka-avro-console-consumer --bootstrap-server kafka:29092 \
  --topic pesaflow.transactions.p2p --from-beginning --max-messages 3 \
  --property schema.registry.url=http://localhost:8081 --property print.key=true \
  --key-deserializer org.apache.kafka.common.serialization.StringDeserializer
```

Kafka UI (http://localhost:8090) decodes the Avro values too.

## 2. Avro, schema first

Payloads are defined in Avro IDL (`.avdl`) and compiled to Java by `generateAvroJava`
(`buildSrc/src/main/kotlin/GenerateAvroJava.kt`). That task wraps Avro's own compiler, so the
build doesn't depend on an unmaintained Gradle plugin. Run `./gradlew :pesaflow-streams:generateAvroJava`
and look in `pesaflow-streams/build/generated/sources/avro/main`:

- `uuid` → `java.util.UUID`, `decimal(19, 2)` → `BigDecimal`, `timestamp-micros` → `Instant`
- nullable fields get `Optional` getters: `getFromAccountId()` returns `Optional<UUID>`
- `__op` becomes `getOp()`

There are two kinds of schema here:

| Schema | Owner | Purpose |
|--------|-------|---------|
| `com.pinchez.cdc.Transaction` | the database (via Connect) | a *reader* schema for what Debezium writes. It mirrors columns: snake_case, status as a string, `created_at` as an ISO string |
| `com.pinchez.events.CompletedTransaction` | this app | the domain event: an enum type, an `Instant`, only money that actually moved |

The mapper is the boundary between the two. Downstream consumers should never depend on
database column names.

### How the CDC reader schema resolves

The Confluent deserializer, with `specific.avro.reader=true`, picks the Java class by the
**writer** schema's full name. Debezium names it `pesaflow.public.transactions.Value`, and
`public` can't be a Java package. So the connector now renames it with `SetSchemaMetadata`
to `com.pinchez.cdc.Transaction`. Avro **schema resolution** then reconciles the writer
schema (from the registry) with the reader schema (compiled into the class):

- A writer field the reader doesn't have is skipped.
- A reader field missing from the writer takes its default (which is why optional fields have `= null`).
- A writer `string` becomes a reader `uuid`, because a logical type is just an annotation on `string`.

## 3. Why the keys changed to plain strings

The connector's key converter is now `StringConverter`. Kafka assigns partitions by hashing
the **serialized key bytes**. Connect wrote Avro keys: a magic byte, a schema id, then the
string. A Streams app that later re-keys by account id and writes with `Serdes.String()`
produces different bytes, and therefore a different partition, for the same account. Joins
in Milestone 3 rely on co-partitioning: the same key must land in the same partition number
on both topics. Mixed key formats break that silently, with no error, just missing join
results. A UUID gains nothing from a schema, so plain strings it is.

## 4. Testing

```bash
./gradlew :pesaflow-streams:test
```

- `TransactionRoutingTopologyTest` uses **`TopologyTestDriver`**: the topology runs
  synchronously in-process, with no broker. `mock://` schema registry URLs give each test an
  in-memory registry. This is where topology logic gets tested.
- `StreamsApplicationIntegrationTest` runs the real Spring app against Kafka in
  Testcontainers. It proves the wiring (serdes, topics, config), not the logic.

The topology is a plain class (`TransactionRoutingTopology.addTo(StreamsBuilder)`). Spring
only wires it, which is why it's testable without a Spring context.

## Experiments

1. **Partitions and parallelism.** Each input partition becomes a **task**. Start a second
   instance in another terminal with `./gradlew :pesaflow-streams:bootRun` (there's no web
   server, so ports don't clash). Watch both logs rebalance, then run
   `kafka-consumer-groups.sh --describe --group pesaflow-streams --members` to see the 3
   partitions split between them. Start a 3rd and a 4th instance: what does the 4th get?
2. **Consumer group view.** `kafka-consumer-groups.sh --describe --group pesaflow-streams`.
   The `application.id` *is* the consumer group. Stop the app, let load run, check the lag,
   and start it again.
3. **Reprocess from scratch.** Stop the app and run:
   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-streams-application-reset.sh \
     --bootstrap-server localhost:9092 --application-id pesaflow-streams \
     --input-topics pesaflow.transactions --to-earliest
   ```
   Restart. Every event is written again, so the outputs now contain duplicates. Why is that
   fine for this milestone but would hurt a balance aggregate?
4. **`read_committed`.** `application.yaml` sets `consumer.isolation.level: read_committed`
   because Connect writes in exactly-once transactions. What could a `read_uncommitted`
   consumer see if Connect crashed mid-transaction?
5. **Poison pill.** Produce a non-Avro value to `pesaflow.transactions` with
   `kafka-console-producer.sh`. Watch the app hit `LogAndFailExceptionHandler` and shut
   down. (Milestone 7 makes it resilient.)
6. **Schema evolution, end to end.** Add a nullable column to `transactions` with a Flyway
   migration, restart the wallet, and check that the streams app keeps working without
   regenerating code. Then add the field to `cdc.avdl` and map it through.

## Checklist

- [ ] You can read `topology.describe()` and map it back to the DSL calls
- [ ] You know the difference between a writer schema and a reader schema
- [ ] You can explain why key serialization matters for partitioning
- [ ] You can test a topology without a broker
