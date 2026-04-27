# Kafka Docker Setup Explained

This document explains every configuration decision in our Kafka service — environment variables, volumes, and health check — and why each is needed for a single-node MPA deployment.

---

## Image: `apache/kafka:4.0.2`

We use the **official Apache Kafka Docker image** (`apache/kafka`). We previously attempted to use Bitnami's image (`bitnami/kafka`) but Bitnami moved their images behind a subscription paywall in 2025. The official image is always freely available and maintained by the Apache Kafka project itself.

We pin to `4.0.2` rather than `latest` because Railway requires a specific tag, and a pinned version ensures every deployment gets the exact same binary.

---

## Mode: KRaft (no ZooKeeper)

Kafka historically required a separate ZooKeeper cluster to manage broker metadata and leader election. Since Kafka 3.3, **KRaft mode** replaces ZooKeeper with a built-in Raft consensus protocol baked into the broker itself.

We use KRaft because:
- One fewer service to run and pay for
- Simpler networking (nothing to wire up between Kafka and ZooKeeper)
- ZooKeeper support was fully removed in Kafka 4.0

In KRaft, every node is assigned one or more **roles**: `broker` (handles producer/consumer traffic) and/or `controller` (handles cluster metadata, leader election). In our single-node setup the same process plays both roles.

---

## Environment Variables

### `KAFKA_NODE_ID=1`

Every node in a Kafka cluster must have a unique integer ID. Since we only have one node, `1` is fine. This ID is referenced in `KAFKA_CONTROLLER_QUORUM_VOTERS` below.

---

### `KAFKA_PROCESS_ROLES=broker,controller`

Tells this node to act as both a **broker** and a **controller** simultaneously. In a multi-node cluster you would split these across machines, but for a single-node setup combining them is correct and necessary — there is no other node to be the controller.

---

### `KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093`

Defines the full list of controller nodes in the cluster. The format is `<node-id>@<host>:<port>`.

- `1` — the node ID of the controller (matches `KAFKA_NODE_ID`)
- `localhost:9093` — the address the broker uses to reach itself for controller traffic

We use `localhost` (not `kafka` or the Railway internal hostname) because the controller listener is internal to the process — it never needs to be reached from another machine. Using `localhost` here means the broker talks to its own controller directly without any network hop.

---

### `KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093`

Defines which addresses and ports Kafka binds to inside the container.

- `PLAINTEXT://:9092` — binds to all interfaces on port 9092. This is the port that producers (itch-runner) and consumers (db-consumer) connect to.
- `CONTROLLER://:9093` — binds on port 9093 for internal KRaft controller-to-controller communication. Clients never connect here; it is only used by the broker's own controller role.

---

### `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka.railway.internal:9092`

This is the address Kafka **tells clients to use** when they connect. After a client connects via `bootstrap.servers`, Kafka responds with the advertised listener address for subsequent requests.

- In Railway: `kafka.railway.internal:9092` — Railway's private DNS name for the Kafka service, reachable by other services in the same project (itch-runner, db-consumer).
- In local docker-compose: `kafka:9092` — Docker's internal DNS name for the kafka container.

If this is set incorrectly (e.g., to `localhost`), producers and consumers would connect to bootstrap successfully but then fail when Kafka redirects them to an unreachable address.

---

### `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT`

Maps each listener name to its security protocol.

- `CONTROLLER:PLAINTEXT` — the controller listener uses unencrypted plaintext (fine for internal traffic)
- `PLAINTEXT:PLAINTEXT` — the client-facing listener also uses plaintext

In a production environment with external clients you would use `SSL` or `SASL_SSL` here. For our private Railway deployment where nothing is exposed to the public internet, plaintext is appropriate.

---

### `KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER`

Tells Kafka which listener name is reserved for controller (KRaft metadata) traffic. Kafka uses this to distinguish controller-to-controller communication from regular broker traffic. Must match one of the names defined in `KAFKA_LISTENERS`.

---

### `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`

When a producer publishes to a topic that does not exist yet, Kafka will create it automatically. This means itch-runner can start publishing to `trades`, `tob`, `vwap`, `noii`, `market_events`, and `tradebucket` without any manual topic creation step.

In stricter production setups you would set this to `false` and create topics explicitly to prevent accidental topic creation from typos. For our setup auto-creation is convenient and safe.

---

### `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`

Kafka stores consumer group offsets (i.e. "db-consumer has read up to message X on the trades topic") in an internal topic called `__consumer_offsets`. By default this topic has a replication factor of **3**, meaning it expects 3 brokers. With only 1 broker, Kafka cannot satisfy this and the broker will log errors or refuse to start.

Setting this to `1` tells Kafka to replicate the offsets topic to only 1 broker (our only broker), which is correct for a single-node cluster.

---

### `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1`

Same issue as above but for Kafka's internal transaction coordinator topic (`__transaction_state`). Default is 3; must be 1 for a single-node cluster.

---

### `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1`

ISR stands for **In-Sync Replicas** — the number of replicas that must acknowledge a write before it is considered committed. The default minimum is 2, which is impossible to satisfy with 1 broker.

Setting this to `1` means a write is committed once the single broker acknowledges it, which is the only sensible value for a single-node setup.

---

## Volumes

```yaml
volumes:
  - kafka_data:/var/lib/kafka/data
```

Kafka persists all topic data (messages), the KRaft metadata log, and cluster state to disk. Without a volume this data lives only inside the container and is **lost every time the container restarts**.

With a named volume (`kafka_data`) mounted at `/var/lib/kafka/data` (the default data directory for the `apache/kafka` image):

- Topic messages survive container restarts and redeployments
- KRaft cluster metadata (node ID, cluster ID, controller state) is preserved — without this Kafka would re-initialise with a new cluster ID on every restart, which causes consumer group offset data to become invalid
- db-consumer can resume from its last committed offset rather than re-reading all messages from the beginning

In Railway, the volume is attached to the Kafka service through the Railway dashboard. In local docker-compose, Docker manages the `kafka_data` named volume automatically.

---

## Health Check

```yaml
healthcheck:
  test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"]
  interval: 10s
  timeout: 10s
  retries: 12
  start_period: 15s
```

The health check verifies that Kafka is fully started and accepting client connections — not just that the process is running.

**What it does:** runs `kafka-topics.sh --list` against the broker every 10 seconds. This command succeeds (exit code 0) only when Kafka is ready to serve requests. If it fails, Docker marks the container as unhealthy.

**Why it matters for startup ordering:** `docker-compose.yml` declares `itch-runner` and `db-consumer` as `depends_on: kafka: condition: service_healthy`. This means those services will not start until this health check passes, preventing connection errors during the Kafka boot sequence (which can take 10–20 seconds).

**Parameters:**
- `interval: 10s` — check every 10 seconds
- `timeout: 10s` — if the command takes longer than 10s, treat it as failed
- `retries: 12` — allow up to 12 consecutive failures before marking unhealthy (120 seconds total)
- `start_period: 15s` — do not count failures during the first 15 seconds (gives Kafka time to initialise before the checks start counting against the retry limit)
