# Milestone 3 — Kafka Streams: tables, joins & state

Goal: turn the stream of transactions into **state**: balances, profiles and summaries kept
in local stores, joined against account and merchant tables, and queryable over REST from
any instance.

## The topology

```
pesaflow.accounts ─────────▶ KTable accounts (versioned store)  ─────┬──────────────┬──────────────┐
pesaflow.merchants ────────▶ GlobalKTable merchants ─────────┐       │              │              │
pesaflow.transactions ─▶ completed-only ─▶ to-completed ─┬───┼───────┼──────────────┼──────────────┼──▶ routing (Milestone 2)
                                                        │   │       │              │              │
             selectKey(account) ─▶ leftJoin accounts ◀──┤   │       │              │              │
                         ─▶ leftJoin merchants ◀────────┼───┘       │              │              │
                         ─▶ pesaflow.transactions.enriched          │              │              │
                         ├─▶ reduce        → largest-transactions   │              │              │
                         └─▶ cogroup ×3    → account-profiles       │              │              │
                                                        │           │              │              │
          flatMap(debit/credit) ─▶ groupByKey ─▶ aggregate → balances ──▶ pesaflow.balances       │
                                                        │                │                        │
                                                        │      accounts LEFT JOIN balances → account-summaries
                                                        │                                         │
          toTable(transactions-by-id) ── FK join on fromAccountId ─────────────────────────────────┘
                                         → pesaflow.transactions.with-sender
```

Each feature lives in its own class under `topology/`. `CdcSources` registers the three
inputs once, because Kafka Streams allows only one source per topic, and `PesaflowTopology`
wires the features together.

| Feature | Class | Store | Output topic |
|---------|-------|-------|--------------|
| Stream–table + stream–GlobalKTable join | `TransactionEnrichmentTopology` | — | `pesaflow.transactions.enriched` |
| `aggregate` | `BalanceTopology` | `balances` | `pesaflow.balances` (compacted) |
| `reduce` | `AccountInsightsTopology` | `largest-transactions` | — |
| `cogroup` | `AccountInsightsTopology` | `account-profiles` | — |
| Table–table join | `AccountInsightsTopology` | `account-summaries` | — |
| Foreign-key join | `TransactionSenderTopology` | `transaction-senders` | `pesaflow.transactions.with-sender` (compacted) |

## 1. Run it

```bash
docker compose up -d
./gradlew :wallet-api:bootRun                   # :8080
curl -X POST localhost:8080/load-generator/start
./connect/connect.sh apply
./gradlew :pesaflow-streams:bootRun             # :8082
```

Then use `http/pesaflow-streams.http`, or:

```bash
ID=$(curl -s 'localhost:8080/accounts?size=1' | jq -r '.content[0].id')
curl -s localhost:8082/balances/$ID | jq
curl -s localhost:8082/accounts/$ID/summary | jq
curl -s localhost:8082/accounts/$ID/profile | jq
curl -s localhost:8082/accounts/$ID/largest-transaction | jq
```

Check a balance against the database:

```sql
select sum(case when to_account_id = :id then amount else -amount end)
from transactions
where status = 'COMPLETED' and (from_account_id = :id or to_account_id = :id);
```

Balances can go negative: the wallet doesn't check funds, by design.

**State lives in `pesaflow-streams/build/kafka-streams/<port>`.** That checkpoint data
belongs to one Kafka cluster. After `docker compose down -v`, delete that directory as well,
or the GlobalKTable thread finds offsets that no longer exist and the app shuts down.

## 2. Concepts, mapped to the code

**KStream vs KTable vs GlobalKTable.** `pesaflow.transactions` is a stream of facts: every
record matters. `pesaflow.accounts` is a changelog: only the latest value per key matters,
which is why the topic is compacted. Each instance holds the accounts for *its* partitions.
A **GlobalKTable** (merchants) is fully replicated to every instance: no co-partitioning
needed and any key can be looked up, at the cost of every instance storing and reading all
of it. Use it for small, slowly-changing reference data.

**Repartitioning.** `selectKey` changes the key, so the stream must be re-shuffled before it
can be joined with `accounts` (same key, same partition). Kafka Streams writes it to
`pesaflow-streams-join-account-repartition` and reads it back. `flatMap` before
`groupByKey` does the same (`...-ledger-entries-by-account-repartition`). Joining with the
GlobalKTable needs no repartition.

**`aggregate` vs `reduce`.** `reduce` combines values of the *same* type: the largest
`EnrichedTransaction` is still an `EnrichedTransaction`. A balance is a different type from
a ledger entry, and needs a starting value (zero), so it takes `aggregate`.

**`cogroup`.** Top-ups, P2P sends and merchant payments each update different fields of one
`AccountProfile`. Without cogroup that's three aggregates and two table joins, each with
its own store. `cogroup` feeds all three grouped streams into one aggregate and one store.

**Versioned store.** `pesaflow.streams.accounts-table.versioned=true` keeps account history.
A stream–table join then uses the account **as of the transaction's timestamp**, not its
latest state. `TransactionEnrichmentTopologyTest` proves it: an account is BASIC at 08:00
and FULL at 10:00, and a late transaction stamped 09:00 joins as BASIC (versioned) or FULL
(latest).

**Foreign-key join.** `transactions-by-id` is keyed by transaction id. Its *value* points at
an account. When an account changes, Kafka Streams must find every transaction that
references it, which is why it creates `-subscription-registration` and
`-subscription-response` internal topics. Note that `toTable()` keeps every transaction
forever. FK joins suit table-shaped data; an unbounded event stream turned into a table is
expensive.

**Interactive queries.** Each instance only has the keys for its own partitions.
`StateStoreQueryService` asks Kafka Streams which instance (`application.server`) owns a key:
- If it's this one, read the local store.
- If not, `RemoteQueryClient` forwards the request.

The `X-Pesaflow-Forwarded` header stops ping-pong while partitions are moving; you get a 503
problem detail instead. Queries also return 503 while stores restore or rebalance.

## Experiments

1. **Internal topics.** `kafka-topics.sh --list | grep pesaflow-streams`. For each
   `-repartition` and `-changelog` topic, find the DSL call that created it.
   `pesaflow-streams-accounts-changelog` duplicates `pesaflow.accounts`, which is already a
   compacted log of the same data. Read about `topology.optimization` (Milestone 7) and
   work out why the source topic could serve as the changelog instead.
2. **Restore from changelog.** Stop the app, `rm -rf pesaflow-streams/build/kafka-streams`,
   and start it. Watch for `Restoration` in the log and time it. Balances are back without
   re-reading `pesaflow.transactions`.
3. **Two instances.** Start a second instance on another port:
   ```bash
   java -jar pesaflow-streams/build/libs/pesaflow-streams-0.0.1-SNAPSHOT.jar --server.port=8086
   ```
   (run from `pesaflow-streams/` after `./gradlew :pesaflow-streams:bootJar`). Query the same
   account on 8082 and 8086 and check both answers match. Add `-H 'X-Pesaflow-Forwarded: true'`
   to see which instance owns it: the other one answers 503.
4. **Account doesn't exist yet.** Produce a transaction for a random account id that has no
   account row. The enrichment is a *left* join, so the event still flows with empty
   account fields. What would an inner join do? Why do both sides have to be co-partitioned
   for this to work at all?
5. **Tombstones.** Open an account with no transactions, `DELETE` it in psql, and follow it:
   tombstone in `pesaflow.accounts` → removed from the `accounts` store → its
   `account-summaries` entry disappears (`/accounts/{id}/summary` returns 404).
6. **Caching.** Hammer one account (top it up in a loop) and watch `pesaflow.balances` in
   Kafka UI. Fewer updates come out than go in. Set `statestore.cache.max.bytes: 0` and
   repeat; then set it large with `commit.interval.ms: 30000`. Interactive queries always
   see the latest value, because they read through the cache. Downstream joins and topics
   only see flushed values. The integration test has to wait for exactly this.
7. **Versioned vs latest, on real data.** Upgrade an account's KYC level, then reset the app
   and replay (`kafka-streams-application-reset.sh ... --input-topics
   pesaflow.transactions,pesaflow.accounts,pesaflow.merchants --to-earliest`, and delete the
   state dir). Compare `kycLevel` in `pesaflow.transactions.enriched` for transactions made
   before the upgrade, with `accounts-table.versioned` true and then false.
8. **FK join fan-out.** Freeze an account that has sent many transactions
   (`PUT /accounts/{id}/status`) and count the records that appear in
   `pesaflow.transactions.with-sender`.

## Checklist

- [ ] You can say when to use a KTable vs a GlobalKTable
- [ ] You can point at each internal topic and name the operation that created it
- [ ] You know why `reduce` couldn't compute a balance
- [ ] You've watched a store restore from its changelog
- [ ] You've queried a key owned by another instance
- [ ] You can explain what a versioned store changes about a join
