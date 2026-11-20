# Milestone 7 — Production concerns

Goal: make the pipeline survive the things that happen in production: crashes, bad data,
more instances, replays, and someone asking "is it healthy?". Every number below was measured
on this project's stack. Re-measure on yours.

| Concern | Where |
|---------|-------|
| Exactly-once processing | `application.yaml` (`processing.guarantee`), sinks read committed |
| Error handling | `handler/` (deserialization, processing, production, uncaught) |
| Scaling and failover | standby replicas, session timeout, leave group on close |
| Observability | `/actuator/prometheus`, `connect/metrics.yaml`, `observability/` |
| Stable names, optimization | `ensure.explicit.internal.resource.naming`, `topology.optimization` |
| Explicit repartitioning | `partitioner/AccountPartitioner`, `LedgerTopology` |
| End-to-end test | `pesaflow-e2e` |

## 1. Exactly-once

```yaml
processing.guarantee: ${STREAMS_PROCESSING_GUARANTEE:exactly_once_v2}
```

Each task reads, updates its state and writes its outputs in **one Kafka transaction**,
committed every `commit.interval.ms`. A crash aborts the open transaction, and nothing it
wrote becomes visible to `read_committed` consumers. That's why the three sink connectors now
set `consumer.override.isolation.level=read_committed`. Without it they could load rows from
transactions that were later aborted.

**The experiment.** Run the load generator, `kill -9` the app three times at random points,
restart it each time, then compare every balance with Postgres:

```sql
with db as (
  select a.id::text account_id,
         coalesce(sum(case when t.to_account_id = a.id then t.amount
                           when t.from_account_id = a.id then -t.amount end), 0) balance
  from accounts a left join transactions t
    on t.status = 'COMPLETED' and (t.from_account_id = a.id or t.to_account_id = a.id)
  group by a.id)
select count(*) filter (where b.amount <> db.balance) mismatched,
       coalesce(sum(b.amount - db.balance), 0) total_difference
from db join analytics.balances b using (account_id);
```

| Guarantee | Record cache | Crashes | Accounts compared | Wrong | Difference |
|-----------|--------------|---------|-------------------|-------|------------|
| `exactly_once_v2` | 10 MB | 3 × `kill -9` | 1,360 | 0 | 0.00 |
| `at_least_once` | 10 MB | 3 × `kill -9` | 1,370 | 0 | 0.00 |
| `at_least_once` | off (`STREAMS_CACHE_MAX_BYTES=0`) | 3 × `kill -9` | 1,378 | **9** | **+997.00** |

The middle row is the interesting one. At-least-once only double-counts when state has been
written but its input offset hasn't been committed. With the record cache on, aggregate
updates stay in memory until commit, so a crash loses them along with the uncommitted offsets.
Reprocessing then applies each record once. Turn the cache off, and every update hits RocksDB
and the changelog immediately. Now a crash between commits double-counts. Under at-least-once,
correctness depended on a cache size. Exactly-once doesn't depend on it.

The repair is the same as for any corrupted state: stop, reset, replay (section 5). Replaying
under exactly-once took 10.5 minutes and brought all 1,378 balances back into line.

**What an unclean shutdown looks like under exactly-once.** After the machine went down
overnight, the app logged `TaskCorruptedException: Tasks [3_2] are corrupted and hence need to
be re-initialized`. Exactly-once only trusts local state that was checkpointed on a clean
close. Without that, Kafka Streams wipes the task's stores and restores them from the
changelogs. It was RUNNING again 10 seconds later. That's the cost of the guarantee: a crash
means a restore, not a quick resume.

**Lag never reaches zero.** Transactional writes end each partition with a commit marker,
which takes an offset. A fully caught-up consumer still shows a lag of 1 per partition.

## 2. Error handling

Streams has four places to decide what an error means. The policy here: **skip bad input from
outside, never skip money.**

| Handler | Class | Policy |
|---------|-------|--------|
| `deserialization.exception.handler` | `CdcDeserializationExceptionHandler` | external topics: dead-letter and continue. This app's own internal topics: fail (a bug or corruption) |
| `processing.exception.handler` | `BestEffortProcessingExceptionHandler` | fraud-rule processors (by name prefix): dead-letter and continue. Dedup, ledger, balances, joins: fail |
| `production.exception.handler` | `OversizedRecordProductionExceptionHandler` | `RecordTooLargeException`: drop and log (a dead letter would be just as big). Anything else: fail |
| uncaught exception handler | `RestartLimitingUncaughtExceptionHandler` | `REPLACE_THREAD`, up to 3 times in 10 minutes, then `SHUTDOWN_CLIENT` |

Dead letters go to `errors.dead.letter.queue.topic.name` (`pesaflow.dlq.streams`, KIP-1034),
with the original bytes and `__streams.errors.*` headers. The processing handler relies on
processor names, which works because every processor is named (section 4). Punctuator
exceptions reach the processing handler too, with a null record. That changes Milestone 5's
third experiment.

```bash
echo 'poison:this is not avro' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic pesaflow.transactions \
  --property parse.key=true --property key.separator=:
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic pesaflow.dlq.streams --from-beginning --property print.headers=true
```

The record lands in the DLQ with its topic, partition, offset and the processor that failed
(`cdc-transactions`), and `/actuator/health` stays `UP`. Before this milestone, the default
`LogAndFailExceptionHandler` would have stopped the app.

## 3. Scaling and failover

`num.standby.replicas: 1` keeps a warm copy of each task's state on another instance. Three
instances, with one shown here as an example:

```bash
./gradlew :pesaflow-streams:bootJar
cd pesaflow-streams
SERVER_PORT=8086 java -Xmx512m -jar build/libs/pesaflow-streams-0.0.1-SNAPSHOT.jar
```

Start 8086 and 8087 next to the usual 8082, and read `New active tasks` / `New standby tasks`
in each log. At first 8082 kept **every** active task, and the others got only standbys. The
high-availability assignor won't move a task to an instance that would have to restore it
first. The new standbys started 1,068,149 changelog records behind. Once they caught up, a
**probing rebalance** (every `probing.rebalance.interval.ms`, 10 minutes) spread the active
tasks out.

Then kill whichever instance owns an account, and time how long until another one serves its
balance (`X-Pesaflow-Forwarded: true` stops it forwarding to the dead instance):

| Stop | Defaults | After the changes below |
|------|----------|-------------------------|
| `kill -9` (crash) | 48.1 s | 12.8 s |
| `kill -15` (graceful) | 49.3 s | 3.6 s |

The balance was identical before and after every time. The standby took over almost
instantly. The waiting was all group membership:

- **A crash** is only noticed when `session.timeout.ms` expires (45 s by default). It's now
  10 s, with a 3 s heartbeat. Lower means faster failover, but a long GC pause can look like
  a crash and trigger a needless rebalance.
- **A graceful stop was just as slow**, because Kafka Streams deliberately doesn't send
  LeaveGroup on close (`internal.leave.group.on.close=false`). That's so a quick rolling
  restart doesn't cause two rebalances. With standbys, a rebalance is cheap, so
  `KafkaStreamsConfiguration` now calls `setLeaveGroupOnClose(true)`. For rolling restarts
  without any rebalance, look up static membership (`group.instance.id`).

## 4. Stable names, optimization, explicit repartitioning

**Names.** `ensure.explicit.internal.resource.naming: true` makes the app refuse to start if
any repartition topic, changelog or store would get a generated name like
`KSTREAM-AGGREGATE-STATE-STORE-0000000001`. Generated names are numbered by position, so
adding an operator early in the topology renames everything after it, and that state is
effectively lost. Every test builds the topology with this check on (`TopologyHarness`). The
whole topology was already explicitly named, so it passed first time.

**Optimization.** `topology.optimization: all` was measured, not assumed:

| | Repartition topics | Changelogs |
|-|--------------------|------------|
| `none` | 7 | `accounts` has its own |
| `all` | 7 | `accounts` reuses `pesaflow.accounts` |

No repartitions merge: each one follows a different key change. The only effect is reusing
Debezium's compacted topic as the accounts changelog, and that's a bad trade here for two reasons:
- **Restore uses the wrong timestamps.** It reads Kafka's record timestamps, not the
  `updated_at` our extractor assigns, so the versioned store's history would differ after a
  restore.
- **Restore skips the error handlers.** It bypasses the deserialization handler, so one bad
  record in the source topic stops the restore.

So it's explicitly `none`.

**Explicit repartitioning.** The ledger repartition now uses
`Repartitioned.withStreamPartitioner(new AccountPartitioner<>()).withNumberOfPartitions(3)`.
The partitioner hashes the account id exactly as Kafka's default partitioner hashes the
string key Connect writes to `pesaflow.accounts`, and `AccountPartitionerTest` checks that
against Kafka's own `BuiltInPartitioner` for 1,000 ids. Balances are joined with accounts, and
interactive queries assume default partitioning. Any other rule would silently break both.
The partition count is pinned because the join needs equal counts. See experiment 4.

## 5. Reprocessing

```bash
# stop every instance first
docker compose exec kafka /opt/kafka/bin/kafka-streams-application-reset.sh \
  --bootstrap-server localhost:9092 --application-id pesaflow-streams --force \
  --input-topics pesaflow.transactions,pesaflow.accounts,pesaflow.merchants --to-earliest
docker compose exec kafka /opt/kafka/bin/kafka-streams-application-reset.sh \
  --bootstrap-server localhost:9092 --application-id pesaflow-streams --force \
  --input-topics pesaflow.transactions.enriched --to-latest
rm -rf pesaflow-streams/build/kafka-streams
```

The tool rewinds the input offsets and deletes the internal topics; you delete the local
state. `pesaflow.transactions.enriched` is both an output and an input (the unusual-amount
rule reads it), so it goes to the **end**. The replay will write it again.

Three things replays taught us:
- **Outputs are written again.** Fraud alerts fire a second time, with the same alert ids,
  because the ids are deterministic. The webhook is idempotent and the JDBC sink upserts, so
  downstream absorbs it.
- **Windowed rules lose data on replay.** A full replay raised `dropped-records-total` by about
  222,000. Each repartition topic is written by several upstream tasks at different speeds.
  Stream time on the downstream task jumps ahead with the fastest writer, and records from
  slower ones arrive past their window's grace. Live traffic doesn't show it, because
  everything stays close to real time. Raising `max.task.idle.ms` or the grace period, or
  replaying with fewer partitions, all reduce it. None of them removes it.
- **New state starts empty** (Milestone 5's deduplication lesson). A replay is how you fill it.

## 6. Observability

Both apps expose `/actuator/prometheus`. Connect runs the Prometheus JMX exporter agent on
port 9404 (`ENABLE_JMX_EXPORTER=true`), with rules in `connect/metrics.yaml` covering worker,
connector, task, error/DLQ, sink consumer lag and Debezium streaming metrics.

```bash
docker compose --profile observability up -d     # Prometheus :9090, Grafana :3001
```

Grafana opens without a login, and the **PesaFlow** dashboard is provisioned
(`http://localhost:3001/d/pesaflow`):

| Row | Panels |
|-----|--------|
| Kafka Streams | records processed, consumer lag by input topic, end-to-end latency, late records dropped, duplicates dropped, restoration/standby lag |
| Kafka Connect | Debezium lag behind Postgres, source and sink throughput, sink lag, DLQ records, failed tasks |
| Wallet API | requests and latency by URI |

Two traps from setting up the exporter:
- **`/kafka/config` is a volume** that the image only fills with files that don't exist yet
  (`cp -n`). A rules file baked into the image never replaced the stale copy, so it's
  bind-mounted instead.
- **Rules match `domain<properties><>attribute: value`.** A greedy `(.+)` for the attribute
  swallowed the value into the metric name (`..._write_total__1076_0`). Attribute groups use
  `([^:]+)`.

## 7. The end-to-end test

```bash
./gradlew :pesaflow-e2e:e2eTest      # about 2.5 minutes; not part of ./gradlew build
```

`PesaflowEndToEndTest` starts Postgres, Kafka, Schema Registry and Connect (built from
`connect/Dockerfile`) in Testcontainers. It then runs **both boot jars as real processes**,
configured only through environment variables, as they'd run in production. It applies the
real connector files, triggers `new-account-large-transfer`, and asserts that:

1. the recipient's balance appears in Kafka Streams (Postgres → Debezium → Streams)
2. the sender is FROZEN in the wallet (alert → HTTP sink → webhook)
3. the freeze reaches Streams (wallet → CDC → Streams)

Once everything is up, the whole loop takes about 5 seconds. Logs land in
`pesaflow-e2e/build/e2e-logs`.

Getting it green surfaced real ordering bugs:
- **The sink subscribed before its topic existed.** The broker auto-created
  `pesaflow.fraud.alerts` with one partition. Streams later grew it to three, and the sink
  didn't see the new partitions until its next metadata refresh, so the alert was missed. The
  test now applies the webhook connector after Streams has created its topics. In
  production, create topics explicitly, and consider `auto.create.topics.enable=false`.
- **Streams needs its source topics to exist.** Debezium only creates a topic on its first
  record, so the test creates the CDC topics up front.
- **`plugin.path` entries are parent directories.** Listing a plugin's own directory makes
  Connect treat every jar in it as a separate plugin (`ClassNotFoundException`).

## Experiments

1. **Make exactly-once fail.** Set `STREAMS_PROCESSING_GUARANTEE=at_least_once` and
   `STREAMS_CACHE_MAX_BYTES=0`, crash the app under load, and run the balance query. Then
   reset and replay to repair it.
2. **Failover without standbys.** `STREAMS_STANDBY_REPLICAS=0`, three instances, kill the
   busiest. How much longer does it take, and which Grafana panel shows why?
3. **Thread replacement.** Throw from the balance aggregator for one specific account. Watch the
   processing handler fail, the thread be replaced three times, and the client shut down.
4. **Break co-partitioning.** Set `pesaflow.streams.output-topics.partitions=6`, reset, and
   start. What happens to the balances–accounts join, and why can't Kafka Streams simply
   repartition `pesaflow.accounts` for you?
5. **Remove a name.** In `BalanceTopology`, replace `Materialized.as(StoreNames.BALANCES)` with
   `Materialized.with(...)` and run any test. Then turn `ensure.explicit.internal.resource.naming`
   off and look at the generated store and changelog names. (Removing a `Named.as` only renames
   a processor. The check covers stores, changelogs and repartition topics.)
6. **Static membership.** Give each instance a `group.instance.id` and do a rolling
   restart. Count the rebalances in the logs, with and without it.

## Checklist

- [ ] You can explain what a Kafka transaction covers in exactly-once Streams, and what consumers need to do
- [ ] You can say when at-least-once double-counts, and why the record cache hid it
- [ ] You know which errors each Streams handler sees, and the policy each one applies
- [ ] You can account for every second of a failover
- [ ] You've reset and replayed the app, and know what it does to windowed rules and downstream systems
- [ ] You can read the dashboard and say whether the pipeline is healthy
