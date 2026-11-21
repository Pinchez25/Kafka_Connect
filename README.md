# PesaFlow

A mobile-money wallet with a real-time ledger and fraud detection, built to learn **Kafka
Connect** and **Kafka Streams** by doing. A Spring Boot wallet writes to Postgres. Debezium
streams every change into Kafka. A Kafka Streams application turns those changes into
balances, account insights and fraud alerts. Sink connectors write the results back to
Postgres, and the fraud alerts go to a webhook that freezes the account, closing the loop.

```
 wallet-api ──▶ Postgres ──▶ Debezium (Connect) ──▶ pesaflow.transactions / .accounts / .merchants
     ▲                                                          │
     │                                                          ▼
     │                                              pesaflow-streams (Kafka Streams)
     │                                    balances · profiles · activity windows · dedup
     │                                    fraud rules · dormancy · interactive queries
     │                                                          │
     │          ┌─────────────── pesaflow.fraud.alerts ◀────────┤
     │          ▼                                               ▼
     └── HTTP sink (freeze)                 JDBC sinks ──▶ Postgres analytics.*
```

## Stack

- **Apps:** Java 25, Spring Boot 4.1, Gradle (multi-module, `buildSrc` conventions)
- **Kafka:** Kafka 4.3 (KRaft); Kafka Streams 4.2 on the client side
- **Schemas:** Avro, with schemas in `.avdl` files; Confluent Schema Registry
- **Connect:** Debezium 3.6, with the Postgres source and JDBC sink connectors, plus the Aiven HTTP sink
- **Database:** Postgres 18, with Flyway migrations
- **Monitoring:** Prometheus and Grafana
- **Tests:** JUnit, `TopologyTestDriver` and Testcontainers

## Repository layout

| Path | What |
|------|------|
| `wallet-api/` | REST API for accounts, merchants and transactions; load generator; fraud scenarios; webhook (port 8080) |
| `pesaflow-streams/` | The Kafka Streams application and its query endpoints (port 8082) |
| `common-web/` | Shared `ProblemDetail` error handling |
| `pesaflow-e2e/` | End-to-end test of the whole pipeline against real containers |
| `connect/` | Connect image (`Dockerfile`), connector configs, `connect.sh`, JMX exporter rules |
| `observability/` | Prometheus config and the provisioned Grafana dashboard |
| `Dockerfile` | One layered, non-root image per application (`--build-arg MODULE=...`) |
| `k8s/` | Kustomize manifests: Strimzi, CloudNativePG, Schema Registry, both apps ([guide](k8s/README.md)) |
| `.github/` | CI, image publishing, nightly end-to-end run, Dependabot |
| `docs/` | One guide per milestone, with experiments |
| `http/` | Ready-made requests for both apps (IntelliJ HTTP client) |

## Quick start

Prerequisites: Docker, JDK 25, `jq`.

```bash
docker compose up -d                              # Kafka, Schema Registry, Connect, Postgres, Kafka UI
./gradlew :wallet-api:bootRun                     # terminal 1, :8080 (runs the Flyway migrations)
curl -X POST localhost:8080/load-generator/start  # random traffic: transfers, top-ups, payments
./connect/connect.sh apply                        # deploy every connector in connect/connectors
./gradlew :pesaflow-streams:bootRun               # terminal 2, :8082
```

Then look around:

```bash
ID=$(curl -s 'localhost:8080/accounts?size=1' | jq -r '.content[0].id')
curl -s localhost:8082/balances/$ID | jq                     # live balance from a state store
curl -s localhost:8082/accounts/$ID/activity | jq            # per-minute windows
curl -s -X POST localhost:8080/load-generator/scenarios/velocity-burst | jq   # trigger a fraud rule
```

Open Kafka UI at http://localhost:8090 to browse topics, with Avro decoded.

To start over at any time, run `docker compose down -v`, then delete
`pesaflow-streams/build/kafka-streams`. The local Streams state belongs to the old cluster.

### Everything in containers

The applications can run as containers too, instead of with `bootRun`:

```bash
docker compose --profile apps up -d --build      # adds wallet-api and pesaflow-streams to the stack
./connect/connect.sh apply
```

The image is built from the repository root, once per module:

```bash
docker build --build-arg MODULE=wallet-api -t pesaflow/wallet-api .
docker build --build-arg MODULE=pesaflow-streams --build-arg PORT=8082 -t pesaflow/pesaflow-streams .
```

It's a multi-stage build: Gradle builds the boot jar, which is then extracted into layers
(dependencies, loader, snapshots, application). A code change only rebuilds the last layer. The
image runs as uid 10001 on a glibc base, because RocksDB's native library needs glibc.

### Kubernetes

`k8s/` deploys the whole platform with Kustomize, using Strimzi for Kafka and Connect and
CloudNativePG for Postgres. It has `local` and `production` overlays. See
[k8s/README.md](k8s/README.md).

```bash
kubectl apply -k k8s/overlays/local
```

## Ports

| Service | Port |
|---------|------|
| wallet-api | 8080 |
| pesaflow-streams | 8082 (more instances on any free port) |
| Kafka | 9092 |
| Schema Registry | 8081 |
| Kafka Connect | 8083 (second worker on 8084 with `--profile scale`) |
| Postgres | 5434 (user, password and database are all `pesaflow`) |
| Kafka UI | 8090 |
| Prometheus | 9090 (`--profile observability`) |
| Grafana | 3001 (`--profile observability`), dashboard at `/d/pesaflow` |

## Milestones

Each guide explains what was built, why, and what was learned along the way. Each one ends
with experiments and a checklist.

| # | Guide | Covers |
|---|-------|--------|
| 0 | [Plumbing](docs/milestone-0.md) | wallet API, Postgres, Flyway, load generator, Docker Compose |
| 1 | [Kafka Connect: CDC source](docs/milestone-1.md) | Debezium, SMTs and predicates, Avro converters, signals, exactly-once source |
| 2 | [Kafka Streams: stateless processing](docs/milestone-2.md) | topologies, Avro schemas, routing, `TopologyTestDriver` |
| 3 | [Kafka Streams: tables, joins & state](docs/milestone-3.md) | KTable, GlobalKTable, joins (including foreign-key and versioned), aggregates, interactive queries |
| 4 | [Windowing & fraud detection](docs/milestone-4.md) | tumbling, sliding and session windows, suppress, grace, stream–stream joins, event time |
| 5 | [Kafka Streams: Processor API](docs/milestone-5.md) | deduplication, punctuators, headers, a hand-built `Topology` |
| 6 | [Kafka Connect: sinks](docs/milestone-6.md) | JDBC upserts and deletes, the HTTP sink feedback loop, dead letter queues |
| 7 | [Production concerns](docs/milestone-7.md) | exactly-once, error handlers, standbys and failover, metrics, reprocessing, end-to-end test |

## Testing

```bash
./gradlew build                         # unit and integration tests for all modules (needs Docker)
./gradlew :pesaflow-e2e:e2eTest         # the whole pipeline end to end, about 2.5 minutes
```

The end-to-end test starts five containers and both applications. Stop the Compose stack
before running it on a machine with limited memory.

## Development workflow

```bash
uvx pre-commit install          # or: pipx install pre-commit && pre-commit install
```

[pre-commit](https://pre-commit.com) runs on every commit:
- **Whitespace and syntax:** whitespace fixers, YAML and JSON syntax checks
- **Leaks:** private keys and secrets (gitleaks)
- **Linters:** shellcheck, hadolint and actionlint
- **Formatting:** Spotless
- **Commit messages:** the `type(scope): subject` format, checked in the commit-msg hook

GitHub Actions:

| Workflow | Runs | Does |
|----------|------|------|
| `ci.yml` | pushes to `main`, tags `v*`, and pull requests | pre-commit, commit message format (PRs), `./gradlew build`, `kustomize build` + `kubeconform` for both overlays, all in parallel; on `v*` tags also calls `e2e.yml`. On pushes, calls `images.yml` once those checks pass (and, for tags, the end-to-end test) |
| `images.yml` | called by `ci.yml`, and PRs that touch the images | builds the app and Connect images; pushes them to GHCR (short commit hash, branch, semver, `latest`), except on PRs |
| `e2e.yml` | pushes to `main`, nightly, manual, and called by `ci.yml` for `v*` tags | the end-to-end test, uploading app and Connect logs |

## Configuration

Both apps read `application.yaml`, and every deployment-specific value can be overridden with
an environment variable. Some examples: `DB_URL`, `SERVER_PORT`, `KAFKA_BOOTSTRAP_SERVERS`,
`SCHEMA_REGISTRY_URL`, `STREAMS_PROCESSING_GUARANTEE`, `STREAMS_STANDBY_REPLICAS` and
`LOAD_GENERATOR_ENABLED`. See each module's `application.yaml` for the full list and defaults.

## Handy commands

```bash
./connect/connect.sh list                       # connectors and task states
./connect/connect.sh status <connector>         # including the stack trace of a failed task
./connect/connect.sh snapshot transactions      # re-emit a table through an incremental snapshot
docker compose --profile observability up -d    # Prometheus and Grafana
docker compose --profile scale up -d            # a second Connect worker
./gradlew spotlessApply                         # format (the build runs spotlessCheck)
```
