# Kubernetes

Kustomize manifests for the whole platform: Kafka and Connect (Strimzi), Postgres (CloudNativePG),
Schema Registry, and the two applications.

```
k8s/
├── base/                    production-shaped defaults: 3 brokers, RF 3, 3 streams instances
│   ├── postgres/            CloudNativePG cluster, logical replication, a debezium role
│   ├── kafka/               Kafka + node pool, topics, Connect (image built by Strimzi), connectors, metrics rules
│   ├── schema-registry/
│   ├── wallet-api/          Deployment
│   ├── pesaflow-streams/    StatefulSet + headless Service
│   └── network-policies.yaml
├── components/monitoring/   PodMonitors for the Prometheus Operator (optional)
└── overlays/
    ├── local/               kind/minikube: one of everything, RF 1, local images, load generator on
    └── production/          pinned images, HPA, ingress with TLS, monitoring
```

## Prerequisites

| Operator | Why |
|----------|-----|
| [Strimzi](https://strimzi.io) | `Kafka`, `KafkaNodePool`, `KafkaTopic`, `KafkaConnect`, `KafkaConnector` |
| [CloudNativePG](https://cloudnative-pg.io) | the Postgres `Cluster` |
| Prometheus Operator | only for `components/monitoring` |
| ingress-nginx, cert-manager | only for the production ingress |

The Kafka `version` in `kafka.yaml` and `connect.yaml` must be one that your Strimzi release
supports.

## Local cluster

```bash
docker build --build-arg MODULE=wallet-api -t pesaflow/wallet-api:local .
docker build --build-arg MODULE=pesaflow-streams --build-arg PORT=8082 -t pesaflow/pesaflow-streams:local .
kind load docker-image pesaflow/wallet-api:local pesaflow/pesaflow-streams:local

kubectl apply -k k8s/overlays/local
kubectl -n pesaflow get kafka,kafkaconnect,kafkaconnector,cluster,pods
kubectl -n pesaflow port-forward svc/wallet-api 8080:80
kubectl -n pesaflow port-forward svc/pesaflow-streams 8082:80
```

Strimzi builds the Connect image (Debezium Postgres and JDBC, the Aiven HTTP sink, the
Confluent Avro converter) and pushes it to `kind-registry:5000`. Run kind with a
[local registry](https://kind.sigs.k8s.io/docs/user/local-registry/), or point
`build.output.image` at one you have.

## Production

Create the secrets first. They are not kept in git:

| Secret | Type | Used by |
|--------|------|---------|
| `pesaflow-debezium` | `kubernetes.io/basic-auth` (`username: debezium`, `password`) | the CloudNativePG managed role and the CDC connector |
| `ghcr-push` | `kubernetes.io/dockerconfigjson` | Strimzi pushing the built Connect image |

`pesaflow-db-app` (the application owner's credentials) is created by CloudNativePG.

```bash
cd k8s/overlays/production
kustomize edit set image ghcr.io/pinchez25/pesaflow-wallet-api:<commit> ghcr.io/pinchez25/pesaflow-streams:<commit>
kubectl apply -k .
```

## Design notes

- **pesaflow-streams is a StatefulSet.** Each pod keeps its RocksDB state on its own volume.
  A restart then resumes from local state instead of restoring every store from the changelogs.
  Each pod also advertises its stable DNS name
  (`pesaflow-streams-N.pesaflow-streams-headless...`) as `application.server`, so instances
  can forward interactive queries to whichever one owns a key.
- **Readiness means Kafka Streams is RUNNING.** While a pod restores state, it reports
  REBALANCING and gets no client traffic. Liveness doesn't consider Kafka Streams, so a long
  restore never gets a pod restarted. The headless Service publishes not-ready pods, so
  forwarding still works during a rollout.
- **No autoscaler on streams.** Every scale event is a rebalance, and instances beyond the
  input partition count (3) sit idle. Scale it deliberately, together with the partitions.
- **Memory.** The streams heap gets 50% of the container, not 75%, because RocksDB lives
  off-heap.
- **`auto.create.topics.enable: false`.** Every topic is a `KafkaTopic`, or is created by
  Kafka Streams or Connect through their admin clients. Milestone 7 showed how an
  auto-created one-partition topic made a sink miss records.
- **Secrets in connector configs** use Strimzi's `KubernetesSecretConfigProvider`
  (`${secrets:pesaflow/<secret>:<key>}`). A Role lets the Connect workers read exactly
  those two secrets.
- **Debezium has its own role** with `REPLICATION` and read-only access to the tables. The
  applications and the JDBC sinks use the database owner created by CloudNativePG.
- **Pods** run as non-root (uid 10001), with a read-only root filesystem, all capabilities
  dropped and the `RuntimeDefault` seccomp profile. The namespace enforces the `restricted`
  Pod Security Standard.
- **NetworkPolicies** deny all ingress by default, then allow each conversation explicitly.
  Strimzi generates its own policies for Kafka and Connect. Adjust the `ingress-nginx`,
  `monitoring` and `cnpg-system` namespace names to your cluster.

## Before relying on this in production

- Pin `sha512sum` on the Connect build artifacts in `kafka/connect.yaml`.
- Use the TLS listener (9093) with `KafkaUser` resources (SCRAM or mTLS) and ACLs, instead of
  the plain internal listener.
- Configure Postgres backups (`spec.backup` in the CloudNativePG cluster).
