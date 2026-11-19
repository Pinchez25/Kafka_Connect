# Milestone 4 — Windowing & fraud detection

Goal: reason about **time**. Five fraud rules, each built on a different windowing or join
primitive, all driven by when things *happened* rather than when Kafka saw them. Alerts land
in `pesaflow.fraud.alerts`.

## The rules

| Rule | Primitive | Fires when | Emits |
|------|-----------|------------|-------|
| `TRANSFER_VELOCITY` | tumbling window + `count` + `suppress` | > 5 P2P transfers from one account in a 1-minute window | once, after window end + 30 s grace |
| `ROLLING_SPEND` | sliding window + `aggregate` + `emitStrategy(onWindowClose)` + stream–table join | outgoing total in any 10-minute span > 50% of the account's daily limit | once per window, after it closes |
| `NEW_ACCOUNT_LARGE_TRANSFER` | stream–table join (done in enrichment) + `filter` | an account < 24 h old sends > 10,000 | immediately |
| `NIGHT_BETTING_BURST` | session window + `count` + `suppress` | ≥ 3 betting payments in one session between 00:00 and 05:00 Nairobi time | once the session has been quiet for 10 min + 2 min grace |
| `ROUND_TRIP_TRANSFER` | stream–stream windowed join | A→B then B→A within 5 minutes, with the return within 10% of the original amount | immediately, when the return leg arrives |

Every threshold lives under `pesaflow.streams.fraud` in `application.yaml`.

Two of these deliberately differ from the original plan:
- **Rolling spend** uses 50% of the daily limit rather than 100%. The wallet already declines
  anything over the limit, so "completed spend exceeds the limit" could never happen. Draining
  half your allowance in 10 minutes is the takeover signal.
- **Round trip** requires similar amounts. Without that, random P2P traffic among 200 accounts
  produced about 3 false alerts a minute.

Code layout: `topology/fraud/` has one class per rule, all implementing the sealed
`FraudRuleTopology`. `FraudDetectionTopology` merges their outputs. `AccountActivityTopology`
keeps the per-minute counts behind the activity endpoint.

## 1. Run it

```bash
docker compose up -d
./gradlew :wallet-api:bootRun
curl -X POST localhost:8080/load-generator/start
./connect/connect.sh apply
./gradlew :pesaflow-streams:bootRun
```

Random traffic rarely trips these rules, so the load generator can inject a scenario.
Each response names the accounts involved and what to expect:

```bash
curl -s -X POST localhost:8080/load-generator/scenarios/velocity-burst | jq
curl -s -X POST localhost:8080/load-generator/scenarios/rolling-spend | jq
curl -s -X POST localhost:8080/load-generator/scenarios/new-account-large-transfer | jq
curl -s -X POST localhost:8080/load-generator/scenarios/round-trip | jq
curl -s -X POST localhost:8080/load-generator/scenarios/night-betting-burst | jq
curl -s -X POST localhost:8080/load-generator/scenarios/late-events | jq
```

Watch the alerts (Kafka UI works too):

```bash
docker compose exec schema-registry kafka-avro-console-consumer --bootstrap-server kafka:29092 \
  --topic pesaflow.fraud.alerts --from-beginning --property schema.registry.url=http://localhost:8081 \
  2>/dev/null | grep '^{' | jq -c '{rule, accountId, description}'
```

`night-betting-burst` only alerts if *now* is inside the night window. To try it in the
day, start the streams app with
`--pesaflow.streams.fraud.night-betting.start=00:00 --pesaflow.streams.fraud.night-betting.end=23:59`.

If you `docker compose down -v`, also delete `pesaflow-streams/build/kafka-streams`.

## 2. Event time

`CdcTimestampExtractor` gives every CDC record the time it happened in the wallet:
- `created_at` for transactions
- `updated_at` for accounts

Without it, Kafka Streams would use the record timestamp. That's when Connect produced the
record, which is minutes off after a Connect outage and wrong for every row of a snapshot.
The Milestone 3 versioned join now also uses wallet time.

**Stream time** is the highest event time seen so far, *per task*. It only moves when records
arrive. Windows close, suppressed results are released and grace expires as stream time
advances, never on the wall clock. That's why the scenarios need the load generator running,
and why the tests send a "tick" transaction from another account.

## 3. Grace, suppress and emit strategies

A window `[09:00, 09:01)` with 30 s grace accepts records stamped inside it until stream time
reaches 09:01:30. Later ones are **dropped**: they're logged as `Skipping record for expired
window` and counted in `kafka.stream.task.dropped.records.total`.

Without `suppress`, a windowed `count` emits on every update: 1, 2, 3 … 6 for the same window.
`suppress(untilWindowCloses(...))` buffers them and emits only the final count, which is what
an alert needs. The buffer here is `unbounded()`. In production you'd cap it
(`maxRecords`/`maxBytes`) and choose `shutDownWhenFull()` or `emitEarlyWhenFull()`.

`emitStrategy(EmitStrategy.onWindowClose())` (rolling spend) gets the same "final result
only" behaviour inside the aggregation itself, with no separate buffer. It checks for closed
windows at most once per `emit.interval.ms.kstreams.windowed.aggregation` of wall-clock time
(1 s by default), which is why `RollingSpendRuleTest` advances the wall clock.

## 4. Window stores and retention

`account-activity` is a 1-minute tumbling count, kept for `retention: 1h`. Retention must be
at least window size + grace. Here it's much longer, because it exists to be queried:

```bash
ID=$(curl -s 'localhost:8080/accounts?size=1' | jq -r '.content[0].id')
curl -s localhost:8082/accounts/$ID/activity | jq                               # last hour
curl -s "localhost:8082/accounts/$ID/activity?from=2026-11-19T08:00:00Z&to=2026-11-19T09:00:00Z" | jq
```

That's `ReadOnlyWindowStore.fetch(key, from, to)`, routed to the owning instance exactly like
the Milestone 3 queries. Old windows are dropped segment by segment as stream time moves on.
This store has **caching disabled**: with the cache on, a window could still be read from the
cache after its retention expired, until the cache evicted it. `AccountActivityTopologyTest`
caught exactly that.

## 5. Testing time

`TopologyTestDriver` controls time completely. The fraud tests under `topology/fraud/` assert
exactly *when* a result appears, not just *that* it appears. For example,
`TransferVelocityRuleTest`:
- nothing while the window is open
- nothing at 09:01:25 (grace still running)
- the final count at 09:01:35
- a 09:00:40 record arriving after that is dropped

## Experiments

1. **Late events.** `POST /load-generator/scenarios/late-events` sends 3 transfers stamped
   10 minutes ago and 3 stamped 20 seconds ago. Check
   `GET localhost:8082/actuator/metrics/kafka.stream.task.dropped.records.total` before and
   after: it rises by 9, because 3 records are dropped by each of 3 windowed aggregations.
   Then query that account's `/activity`: only the 3 within-grace transfers are there.
2. **With and without `suppress`.** Remove the `.suppress(...)` line from
   `TransferVelocityRule`, run `velocity-burst`, and watch the alerts. Then put it back. What
   would a downstream system that freezes accounts do with the un-suppressed version?
3. **Grace trade-off.** Set `transfer-velocity.grace: 0s` and rerun `late-events`. Then set it
   to `5m`. Alerts become more complete but slower. Which matters more for fraud?
4. **Retention.** Set `activity.retention: 2m`, restart, and query `/activity` for a range 10
   minutes ago: empty. Try setting retention below window + grace and read the startup error.
5. **Session merging.** Run `night-betting-burst` twice, 5 minutes apart, with the night window
   widened. Two sessions within the 10-minute gap merge into one. Look at the
   `night-betting-…-changelog` topic to see the old session being deleted.
6. **Out-of-order joins.** In `RoundTripTransferRuleTest`, swap the arrival order of the two
   legs. The alert still fires once. Why? (Look at `alertIfReturnLeg`.)

## Checklist

- [ ] You can explain event time vs processing time vs stream time
- [ ] You know which window type each rule uses and why
- [ ] You can say exactly when a suppressed window result is emitted
- [ ] You've watched a late record get dropped and found it in the metrics
- [ ] You know how retention relates to window size and grace
