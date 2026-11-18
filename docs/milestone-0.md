# Milestone 0 — Plumbing

Goal: a running platform (Kafka, Connect, Schema Registry, Postgres, Kafka UI), the wallet API
writing realistic traffic into Postgres, and enough Kafka CLI practice to understand brokers,
topics, partitions, offsets and consumer groups before Connect and Streams enter the picture.

## 1. Start the platform

```bash
docker compose up -d
docker compose ps
```

| Service         | Host address                           |
|-----------------|----------------------------------------|
| Kafka           | `localhost:9092`                       |
| Schema Registry | `http://localhost:8081`                |
| Kafka Connect   | `http://localhost:8083`                |
| Kafka UI        | `http://localhost:8090`                |
| Postgres        | `localhost:5434` (pesaflow / pesaflow) |

Inside the Docker network everything reaches Kafka at `kafka:29092`. The broker advertises a
different address per listener. That's why there are two listeners, and it's the most common
"why can't my client connect?" problem in Kafka.

## 2. Run the wallet API

```bash
./gradlew :wallet-api:bootRun
```

Flyway creates `accounts`, `merchants` and `transactions`. Try the requests in
[`http/wallet-api.http`](../http/wallet-api.http), then turn on traffic:

```bash
curl -X POST localhost:8080/load-generator/start
curl localhost:8080/load-generator          # counters
curl -X POST localhost:8080/load-generator/stop
```

The generator seeds 200 accounts and 30 merchants, then submits 5 transactions a second (60% P2P, 30% merchant payments,
10% top-ups). Tune it under `pesaflow.load-generator` in
`wallet-api/src/main/resources/application.yaml`.

Things worth noticing in the data:

- There is **no balance column**. Balances will be derived by Kafka Streams in Milestone 3.
- Declined attempts are stored with `status = FAILED`, not thrown away. Fraud rules want them.
- IDs are UUIDv7, so they sort by creation time. That keeps the primary-key index append-only
  and gives Kafka a good message key later.

## 3. Kafka CLI exercises

Open a shell in the broker. All tools live in `/opt/kafka/bin`:

```bash
docker compose exec kafka bash
cd /opt/kafka/bin
```

### 3.1 KRaft

```bash
./kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
./kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --replication
```

There's no ZooKeeper. Cluster metadata is itself a replicated log (`__cluster_metadata`), and
`HighWatermark` is how far that log has been committed. Restart the container and run it again.
What changed?

### 3.2 Topics and partitions

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic playground --partitions 3 --replication-factor 1
./kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic playground
```

Why can't `--replication-factor` be 2 here?

### 3.3 Produce with keys

```bash
./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic playground \
  --reader-property parse.key=true --reader-property key.separator=:
```

Type a dozen lines like `wanjiku:sent 500`, `otieno:paid till 512345`, reusing a few keys.
Then press Ctrl-D.

### 3.4 Consume and inspect partitions and offsets

```bash
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic playground --from-beginning \
  --formatter-property print.key=true \
  --formatter-property print.partition=true \
  --formatter-property print.offset=true
```

- Every record with the same key is in the same partition. Why does that matter for a
  per-account balance?
- Ordering is guaranteed within a partition, not across the topic. Can you see this in the
  output?

### 3.5 Consumer groups

Open two more terminals and start the same consumer in each with `--group playground-readers`
added. Then produce more records.

- How are the 3 partitions split between 2 consumers? Kill one: what happens? Start a 4th
  consumer: what does it get?

```bash
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group playground-readers
```

Stop all consumers, produce 5 more records, and describe again to see **LAG**. Then rewind:

```bash
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group playground-readers \
  --reset-offsets --to-earliest --topic playground --execute
```

Where are those committed offsets stored? (Hint: list topics with `--exclude-internal` off and
look for `__consumer_offsets`.)

### 3.6 Look around

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --list
```

You'll see `connect-configs`, `connect-offsets`, `connect-status` (Kafka Connect keeps all its
state in Kafka) and `_schemas` (Schema Registry does the same). Remember these for Milestone 1.

Open Kafka UI at http://localhost:8090 and find the same topics, partitions, messages and
consumer groups.

## Checklist

- [ ] All five containers are healthy
- [ ] The wallet API is running and the load generator fills `transactions`
- [ ] You can explain: broker, topic, partition, offset, key, consumer group, lag, KRaft controller
- [ ] You've watched a consumer-group rebalance happen
