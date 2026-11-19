# Milestone 6 — Kafka Connect: sinks

Goal: get data **out** of Kafka. Two kinds of sink: a JDBC sink that keeps reporting tables
up to date, and an HTTP sink that turns fraud alerts into an action that feeds back into the
start of the pipeline.

| Connector | Plugin | Reads | Writes |
|-----------|--------|-------|--------|
| `pesaflow-analytics-transactions` | Debezium JDBC sink | `pesaflow.transactions.enriched` | `analytics.transactions` |
| `pesaflow-analytics-accounts` | Debezium JDBC sink | `pesaflow.accounts`, `pesaflow.balances` | `analytics.accounts`, `analytics.balances` |
| `pesaflow-fraud-webhook` | Aiven HTTP sink | `pesaflow.fraud.alerts` | `POST /webhooks/fraud-alerts` on wallet-api |

Both plugins are Apache 2.0. Confluent's JDBC and HTTP sinks need a Confluent licence, so
they aren't used. The Debezium JDBC sink already ships in the Debezium image. The Aiven
HTTP sink is downloaded in `connect/Dockerfile`, with its checksum pinned like the Avro converter.

## 1. Run it

```bash
docker compose build connect && docker compose up -d   # new image: HTTP sink, host.docker.internal
./gradlew :wallet-api:bootRun                          # Flyway V5 creates the analytics schema
curl -X POST localhost:8080/load-generator/start
./connect/connect.sh apply
./gradlew :pesaflow-streams:bootRun
./connect/connect.sh list
```

`docker-compose.yml` maps `host.docker.internal` to the host, so Connect can reach the wallet
running outside Docker.

## 2. The JDBC sink

```sql
select count(*) from analytics.transactions;
select count(*) from transactions where status = 'COMPLETED';   -- the same, give or take in-flight rows
```

| Setting | Value | Why |
|---------|-------|-----|
| `insert.mode` | `upsert` | `INSERT ... ON CONFLICT DO UPDATE`. Writing a record twice leaves one row |
| `primary.key.mode` | `record_value` / `record_key` | transactions are keyed by account in Kafka, so the row key has to come from the value (`transaction_id`). Accounts and balances use the record key |
| `schema.evolution` | `basic` | creates the table, and adds columns for new fields. It never drops or changes them |
| `delete.enabled` | `true` (accounts) | a tombstone becomes a `DELETE` |

(Confluent's JDBC sink calls the same ideas `pk.mode` and `auto.create`/`auto.evolve`.)

**Upserts make the sink idempotent.** After Milestone 5's dedup incident,
`pesaflow.transactions.enriched` still contains about 80,000 duplicate copies of transactions.
The table has exactly one row per transaction, because every copy upserts the same primary key.
That's also why the sink can restart mid-batch and re-write rows without harm.

**Tombstones delete rows.** Open an account with no transactions, check it appears in
`analytics.accounts`, then `delete from accounts where id = '...'` in psql. Debezium emits a
tombstone, and the sink deletes the row a few seconds later.

### Shaping records at the edge

The topics were designed for Kafka consumers, not for tables. The SMTs on the sink adapt them
without touching the topics:

| SMT | Does |
|-----|------|
| `ReplaceField$Value` (`renames`) | `transactionId` → `transaction_id`. Unquoted Postgres identifiers are lower-cased, so camelCase would become `transactionid` |
| `ReplaceField$Value` (`exclude`, with a predicate) | drops `id`, `__op` and `__source_ts_ms` from accounts. The key already gives `account_id` |
| `TimestampConverter$Value` (`unix.precision=microseconds`) | Avro `timestamp-micros` arrives as a plain `long`; this makes it a timestamp |
| `HoistField$Key` | the Debezium sink only understands **Struct** keys. Our keys are plain strings (Milestone 2's co-partitioning decision), so the sink wraps them in `{account_id: ...}` |
| `DropHeaders` | see below |
| `RegexRouter` + `collection.name.format` | `pesaflow.balances` → `balances` → `analytics.balances` |

What you get in Postgres:
- `uuid` becomes `text`.
- `decimal(19,2)` becomes `numeric(19,2)`.
- Debezium's own `ZonedTimestamp` (accounts) becomes `timestamptz` with full microseconds. The
  sink recognises Debezium's semantic types.
- The converted Avro timestamps become `timestamp` with **millisecond** precision: Connect's
  `Timestamp` type is a `java.util.Date`, so the microseconds are lost.

### The header that crashed the sink

The first run of the transactions sink loaded 84,000 rows, then all three tasks failed with
`fieldSchema for field __source_lsn cannot be null`. What happened:

1. Milestone 5 added Debezium headers. `__source_lsn` is null for incremental-snapshot reads.
2. Kafka Streams carried the headers through to `pesaflow.transactions.enriched`.
3. The Debezium JDBC sink converts **every** header into a schema, even when it doesn't use
   them, and a null value has none.

`errors.tolerance=all` didn't help, because the failure was inside the connector's `put()`,
not in a converter or SMT. The fix is `DropHeaders` for `__op,__source_lsn` on both JDBC sinks.
Headers are part of a topic's contract. Things you add upstream reach every sink downstream.

## 3. The closed loop

```
wallet ─▶ Postgres ─▶ Debezium ─▶ pesaflow.transactions ─▶ Streams ─▶ pesaflow.fraud.alerts
   ▲                                                                       │
   └──── POST /webhooks/fraud-alerts ◀── pesaflow-fraud-webhook (HTTP sink) ┘
           (status = FROZEN) ─▶ Postgres ─▶ Debezium ─▶ pesaflow.accounts ─▶ Streams + analytics
```

```bash
R=$(curl -s -X POST localhost:8080/load-generator/scenarios/new-account-large-transfer)
A=$(echo "$R" | jq -r '.accountIds[0]')
sleep 5
curl -s localhost:8080/accounts/$A | jq .status                 # FROZEN
curl -s localhost:8082/accounts/$A/summary | jq .status         # FROZEN, via CDC
psql ... -c "select status from analytics.accounts where account_id = '$A'"   # FROZEN
```

From then on, the wallet declines that account's transfers with `SENDER_FROZEN`.

The webhook (`fraud/controller/FraudAlertWebhookController`) is **idempotent**: freezing a frozen
account changes nothing, and the response says `ALREADY_FROZEN`. An unknown account gets a
`200` with `ACCOUNT_NOT_FOUND` rather than an error, because an error would make the sink
retry something that can never succeed.

The connector starts from the **latest** offset (`consumer.override.auto.offset.reset=latest`,
allowed by the worker's `connector.client.config.override.policy=All`). Deploying it doesn't
retroactively freeze every account that ever triggered a rule.

## 4. Error handling

Errors fall into two kinds, and they need opposite treatment:

| Kind | Example | Retrying helps? | What we do |
|------|---------|-----------------|------------|
| Deterministic | bytes that aren't Avro | never | dead letter queue |
| Transient | wallet restarting | yes | retry until it's back |

All three sinks set:

```json
"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "pesaflow.dlq.<connector>",
"errors.deadletterqueue.context.headers.enable": "true"
```

Send a poison message and find it:

```bash
echo 'this is not avro' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic pesaflow.fraud.alerts
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic pesaflow.dlq.fraud-webhook --from-beginning --property print.headers=true
```

The task keeps running. The DLQ record has the original bytes plus `__connect.errors.topic`,
`.partition`, `.offset`, `.stage` (`VALUE_CONVERTER`), `.class.name` and `.exception.message`.

**The surprise: the DLQ also catches delivery failures.** The Aiven sink reports a failed
HTTP call to the same DLQ (stage `TASK_PUT`) once its own retries run out. With the
original `max.retries=3` × 2 s, stopping the wallet for a minute sent a real alert to the
DLQ, and that account was never frozen. A freeze that silently doesn't happen is the worst
outcome here. So the connector now retries 48 × 5 s: about 4 minutes, just under the
consumer's 5-minute `max.poll.interval.ms` (block longer than that and the consumer is
kicked out of its group). Tested: with the wallet down for 1 min 43 s, the task stayed
`RUNNING` and the account froze 4 seconds after the wallet came back. A longer outage still
ends in the DLQ. That's what experiment 3 is about.

## 5. Reporting

The point of the analytics schema is plain SQL over live data:

```sql
-- Merchant spend by category, last 24 hours
select merchant_category, count(*) payments, sum(amount) total
from analytics.transactions
where type = 'MERCHANT_PAYMENT' and occurred_at > now() - interval '1 day'
group by 1 order by 3 desc;

-- Money held in active vs frozen accounts
select a.status, count(*) accounts, coalesce(sum(b.amount), 0) money_held
from analytics.accounts a left join analytics.balances b using (account_id)
group by 1;
```

## Experiments

1. **Poison the JDBC sink.** Produce garbage to `pesaflow.balances` (Streams doesn't read
   it, so only the sink notices) and find it in `pesaflow.dlq.analytics-accounts`. Then set
   `errors.tolerance` to `none`, apply, repeat, and look at the task's `trace`
   (`./connect/connect.sh status pesaflow-analytics-accounts`).
2. **Schema evolution.** Add a nullable field to `EnrichedTransaction` in
   `transaction-events.avdl`, restart Streams, and watch `\d analytics.transactions` grow a
   column. Then rename a field: what does `schema.evolution=basic` do with the old column?
3. **Replay the DLQ.** Stop the wallet for 5 minutes and trigger an alert, so it lands in
   `pesaflow.dlq.fraud-webhook` (stage `TASK_PUT`). Write a second HTTP sink connector that reads
   the DLQ topic and posts to the same webhook, with its own DLQ for records that fail again.
   Why is it safe to replay the whole DLQ, poison included? (Hint: what happens to the
   poison at the converter, and what does an idempotent webhook do with a redelivered alert?)
4. **Delete without `delete.enabled`.** Turn it off, delete an account in psql, and see what
   the sink does with the tombstone.
5. **Upsert vs insert.** Switch the transactions sink to `insert.mode=insert`, reset its
   consumer group offsets (`connect.sh stop`, then `DELETE /connectors/<name>/offsets`, then
   `resume`), and watch the primary key violations.
6. **Scale the sink.** With `connect-2` running (`--profile scale`), watch the three
   `pesaflow-analytics-transactions` tasks spread across workers (`./connect/connect.sh list`).

## Checklist

- [ ] You can explain why upserts make a sink idempotent, and why that matters for replays
- [ ] You know which errors `errors.tolerance` covers, and which ones kill the task anyway
- [ ] You've found a poison message in a DLQ and read its error headers
- [ ] You've watched a tombstone delete a row downstream
- [ ] You can trace a fraud alert all the way round the loop and back into CDC
