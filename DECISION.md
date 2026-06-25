# Design Decisions

This document records the significant architecture and tooling decisions for the
GPU Telemetry Pipeline, and the reasoning behind each. It is written as a set of
lightweight ADRs (Architecture Decision Records): each one captures the
**context**, the **decision**, the **alternatives considered**, and the
**consequences**.

## Table of contents

1. [Persistence: PostgreSQL + TimescaleDB vs MongoDB](#1-persistence-postgresql--timescaledb-vs-mongodb)
2. [Message queue: Kafka first, behind an interface](#2-message-queue-kafka-first-behind-an-interface)
3. [Build & developer workflow: the Makefile](#3-build--developer-workflow-the-makefile)
4. [Deployment: Helm charts on minikube](#4-deployment-helm-charts-on-minikube)
5. [Telemetry Streamer: stateless replay, data on a PVC](#5-telemetry-streamer-stateless-replay-data-on-a-pvc)
6. [Kafka deployment: direct StatefulSet in KRaft mode](#6-kafka-deployment-direct-statefulset-in-kraft-mode)
7. [TimescaleDB database initialisation: post-install Job](#7-timescaledb-database-initialisation-post-install-job)
8. [Custom gRPC queue: hand-written bindings and JSON codec override](#8-custom-grpc-queue-hand-written-bindings-and-json-codec-override)

---

## 1. Persistence: PostgreSQL + TimescaleDB vs MongoDB

**Status:** Accepted

### Context

The pipeline ingests GPU telemetry (DCGM exporter metrics) as a high-rate,
append-heavy stream of timestamped datapoints. The API must answer two
time-series questions:

- *List all GPUs* for which telemetry exists.
- *Telemetry for a GPU, ordered by time*, with optional inclusive `start_time` /
  `end_time` window filters.

So the workload is: **write a lot, in time order; read back per-GPU slices over
time ranges.** That is a textbook time-series access pattern.

### Workload data (measured from the sample dataset)

The numbers below are measured from the provided sample
(`project_docs/dcgm_metrics_20250718_134233.csv`). The Streamer replays this data
in a loop to simulate a continuous stream, so the cardinality is representative
of steady state.

| Property | Value (sample) | Why it matters for the store |
|---|---|---|
| Rows in sample | 2,470 | One row = one datapoint = one insert. |
| Distinct metrics | 10 (`DCGM_FI_DEV_GPU_UTIL`, `_FB_USED`, `_POWER_USAGE`, …) | Fixed, well-typed columns → a relational schema fits. |
| Distinct GPUs (UUIDs) | 247 | The natural partition/lookup key; the API queries *by* this. |
| Distinct hosts | 31 (× 8 GPUs each) | Confirms UUID — not `gpu_id` (0–7, host-local) — is the global key. |
| Cadence | ~1 sample/second/series | High write rate, strictly time-ordered. |
| **Unique time series** | **247 GPUs × 10 metrics = 2,470** | Series cardinality the index/partitioning must serve. |

**Volume projection at 1 Hz** (the streaming cadence): 2,470 datapoints/second ≈
**213 million rows/day**. At a few hundred bytes/row that is tens of GB/day of
*raw* telemetry, growing without bound. A store for this data needs **automatic
time-partitioning, compression, and retention** to stay healthy — exactly what a
TimescaleDB hypertable provides out of the box, and exactly what a general-purpose
document store makes the operator build and manage by hand.

### Decision

Persist telemetry in **PostgreSQL with the TimescaleDB extension**, accessed via
the pure-Go **pgx** driver, behind a `store.TelemetryStore` interface.

The telemetry table is promoted to a TimescaleDB **hypertable** partitioned by
`time`, with a `(uuid, time DESC)` index serving the API's primary query and a
unique `(uuid, metric_name, time)` key providing insert idempotency.

### Why TimescaleDB over MongoDB

| Concern | PostgreSQL + TimescaleDB | MongoDB |
|---|---|---|
| **Time-series fit** | Purpose-built: automatic time partitioning (hypertables/chunks), `time_bucket()` downsampling, continuous aggregates, native retention/compression policies. | Time-series collections exist, but the feature set is younger and less expressive for windowed analytics. |
| **Query model** | SQL — time-window filters, ordering, and future aggregations (avg/max utilisation, percentiles) are natural and declarative. | Aggregation-pipeline syntax; windowed time queries are more verbose. |
| **Schema** | Telemetry is uniform and well-typed (a fixed set of DCGM fields) — a relational schema documents and enforces the shape. | Schema-flexibility is a strength for irregular documents, which this data is not. |
| **Idempotency** | `UNIQUE (uuid, metric_name, time)` + `ON CONFLICT DO NOTHING` gives exactly-once *effect* under at-least-once delivery, in one statement. | Achievable via unique indexes + upserts, but less ergonomic for the batched-insert path. |
| **Operational maturity** | Decades of Postgres tooling, backups, and operators; TimescaleDB is a standard extension. | Mature, but adds a second operational model alongside a relational API. |
| **Static binary** | pgx is pure Go → links with `CGO_ENABLED=0` into the distroless-static image. | The official driver is also pure Go; not a differentiator. |

The deciding factor is **fit for purpose**: this is a time-series problem, and
TimescaleDB is a time-series database that still gives us the full power of SQL
for the API's current and future queries (rollups, percentiles, retention).

**How TimescaleDB features map to the measured workload:**

- **213M rows/day → hypertable chunks.** Time-partitioning keeps each chunk
  small, so inserts touch a hot recent chunk and time-window reads prune to a few
  chunks instead of scanning the whole table.
- **Tens of GB/day → native compression + retention policies.** Old chunks
  compress (columnar, ~10× typical on metric data) and expire automatically — no
  cron jobs or manual archival.
- **2,470 series, queried per-GPU → the `(uuid, time DESC)` index.** Directly
  serves *"telemetry for GPU X, ordered by time, within [start,end]"* as an index
  range scan.
- **`GET /api/v1/gpus` → `SELECT DISTINCT uuid`,** with continuous aggregates
  available later to make dashboards (avg/max utilisation per minute) cheap.

> **Note on the change mid-build.** The repository initially vendored a MongoDB
> driver. The persistence target was switched to PostgreSQL/TimescaleDB partway
> through the Collector work. Because all persistence sits behind the
> `store.TelemetryStore` interface, **only the implementation changed** — the
> Collector engine and the queue layer were untouched. That is the abstraction
> paying for itself.

### Consequences

- **Positive:** SQL queries for the API; automatic time partitioning; built-in
  retention/compression as data grows; idempotent writes in a single statement.
- **Positive:** The store is split into a `TelemetryStore` write interface
  (consumed by the Collector) and a `TelemetryReader` read interface (consumed by
  the API: `ListGPUs`, `QueryTelemetry`). Each side depends only on the methods it
  needs, and either can be re-pointed at another backend independently.
- **Trade-off:** Requires the `timescaledb` extension. We degrade gracefully —
  if the extension is absent the table still works as plain PostgreSQL (with a
  B-tree index and a logged warning), so the pipeline remains runnable.

---

## 2. Message queue: Kafka first, behind an interface

**Status:** Accepted (Kafka retained as an alternative; custom gRPC queue now the primary implementation)

### Context

The project brief's **end goal is a *custom* message queue** (explicitly *not*
Kafka/RabbitMQ/etc.). But building the custom queue and the Collector at the same
time would mean validating two unproven things against each other, with no known-
good reference to isolate bugs.

We need to prove out the **complete end-to-end flow** first — Streamer →
queue → Collector → TimescaleDB → API — with competing-consumer scaling,
at-least-once delivery, offset commits, and rebalancing all behaving correctly.

### Decision

Use **Kafka as the interim queue for staging and testing**, so we can exercise
and validate the full pipeline against a battle-tested, well-understood broker.
Crucially, the Collector depends only on a small **`queue.Consumer` interface**,
never on Kafka directly. Kafka lives behind that interface in
`internal/queue/kafka`.

This means:

- **Now:** Kafka lets us test the whole flow — partitioning by GPU UUID,
  consumer-group rebalancing on scale up/down, manual offset commits, and
  at-least-once semantics — with confidence that the broker itself is correct.
- **Later:** the custom message queue becomes **one more implementation** of
  `queue.Consumer`. Swapping it in is additive; the Collector, batching logic,
  and delivery semantics do not change.

### What "end-to-end" validation covers

Standing the flow up on Kafka first lets us prove each pipeline property against a
correct reference, so any failure is unambiguously *our* bug, not the broker's.
The behaviours validated — and how the dataset exercises them:

| Property | How Kafka lets us validate it | Tied to the data |
|---|---|---|
| **Throughput** | Sustain the ~2,470 msg/s ingest cadence end-to-end. | Matches the sample's series count at 1 Hz. |
| **Ordering per GPU** | Partition by `uuid` → all of a GPU's datapoints land on one partition, consumed in order. | 247 UUIDs spread evenly across the topic's partitions. |
| **Horizontal scaling** | Add/remove collectors in one consumer group; watch partitions rebalance. | Topic provisioned with 10 partitions = the brief's 10-instance cap. |
| **At-least-once + idempotency** | Kill a collector mid-batch; confirm redelivery and that `ON CONFLICT DO NOTHING` prevents duplicate rows. | Re-streamed rows must not double-count in TimescaleDB. |
| **Backpressure / batching** | Verify batch flush by size and by interval under load. | 2,470 msg/s fills 500-row batches ~5×/s. |

Once these hold on Kafka, the custom queue is validated against the *same* assertions
— a like-for-like swap behind `queue.Consumer`, not a leap of faith.

### Alternatives considered

- **Build the custom queue first.** Rejected for now: no reference to validate
  the Collector against, so any bug is ambiguous (queue or collector?). Kafka
  removes that ambiguity.
- **Couple the Collector directly to a Kafka client.** Rejected: it would make
  the brief's required custom-queue swap a rewrite instead of a drop-in.

### Consequences

- **Positive:** The complete flow can be staged and tested today on a proven
  broker; the Kafka client (`segmentio/kafka-go`) is pure Go, so it fits the
  static distroless image.
- **Positive:** The interface boundary is the design insight that satisfies both
  "use Kafka for now" and the brief's "build a custom queue".
- **Follow-up (complete):** The custom `queue.Consumer` / `queue.Producer`
  implementation (`internal/queue/grpc`, `internal/queue/server`, `cmd/queue`) is
  built and running. Swapping was additive — the Collector and Streamer engines
  were not changed. See [QUEUE.md](QUEUE.md) for the design and Stages 2–4 roadmap.

---

## 3. Build & developer workflow: the Makefile

**Status:** Accepted

### Context

The stack has three services (API, Collector, Streamer), three container images,
three Helm charts, a data-loading step (the Streamer's CSV onto a PVC), OpenAPI
generation, tests with coverage, and a minikube deploy flow. These are multi-step,
easy-to-get-wrong commands with specific flags and ordering (e.g. the namespace
must exist before Helm installs into it; the Streamer's data PVC must be populated
before its pods leave `Init`).

### Decision

Centralise every workflow as a **Makefile target**, so each operation is a
single, self-documenting command with the correct flags baked in.

### Benefits

- **One canonical way to do each thing.** `make deploy`, `make deploy-collector`,
  `make deploy-streamer`, `make cover`, `make openapi` — no one has to remember
  long `docker`/`helm`/`kubectl` incantations or their correct order.
- **Encodes ordering and dependencies.** Targets compose (`deploy` runs
  build → load → namespace → install in the right sequence;
  `deploy-streamer` loads the data PVC before installing), so requirements like
  "namespace-before-install" and "data-before-pods" can't be forgotten.
- **Required by the brief, and satisfied here.** The brief mandates generating
  the OpenAPI spec via a Make command (`make openapi`) and measuring code
  coverage via the Makefile (`make cover` prints a `COVERAGE_TOTAL` line a CI
  gate can grep; `make cover-html` produces a browsable report).
- **CI-friendly.** The same targets developers run locally are what CI runs, so
  "works on my machine" gaps shrink.
- **Discoverable.** The targets double as documentation of the project's
  operations; the README's "Make targets" table is essentially the interface.
- **Environment-overridable.** Image names, tags, namespace, and chart dirs are
  `?=` variables, so the same targets work across environments without edits
  (e.g. `make docker-build TAG=1.2.3`).

### Consequences

- **Positive:** Low-friction, reproducible workflows for build, test, coverage,
  OpenAPI, and deploy.
- **Trade-off:** Make is the lowest-common-denominator task runner; for very
  complex logic a richer tool might be warranted, but the current targets are
  simple wrappers and Make keeps the dependency surface minimal.

---

## 4. Deployment: Helm charts on minikube

**Status:** Accepted

### Context

The brief requires Kubernetes deployment via Helm. Services differ in how they
scale and what they expose: the API serves request traffic and needs a Service;
the Collector is a queue consumer that exposes no Service but must scale with
load; the Streamer is a queue producer that likewise exposes no Service, scales
to drive load, and needs its source CSV mounted from a PersistentVolume. All
three must meet a hardened security posture.

### Decision

Package each service as its **own Helm chart** (`deploy/helm/gpu-telemetry-api`,
`deploy/helm/gpu-telemetry-collector`, and `deploy/helm/gpu-telemetry-streamer`)
and deploy onto **minikube** for local end-to-end testing.

### Why Helm

- **Parameterised, repeatable installs.** Values (replica counts, image
  tags, Kafka/Postgres endpoints, autoscaling bounds, network-policy toggles)
  are externalised, so the same chart serves dev, staging, and prod by swapping
  values — no manifest forking.
- **Templated consistency.** Shared helpers generate consistent names and labels
  across every object, and security context, probes, and policies are defined
  once per chart rather than copy-pasted across raw YAML.
- **Lifecycle management.** `helm upgrade --install --wait` gives atomic,
  idempotent rollouts (and easy rollback) instead of hand-applied `kubectl`
  files.
- **Separate charts = independent scaling.** The Collector and Streamer charts
  each ship a `HorizontalPodAutoscaler` (the brief's 1–10 replica cap) and their
  own image, so each scales on load **independently of the API and of each
  other** — the headline elasticity requirement. Splitting the charts (and the
  Dockerfiles) is what makes that independence real.

### Why minikube

- **Faithful local Kubernetes.** Exercises the *real* objects — Deployments,
  HPA, Services, ServiceAccounts, NetworkPolicies, Pod Security Admission — not a
  simulation, so what passes locally behaves the same on a real cluster.
- **Tight feedback loop.** `minikube image load` runs locally-built images
  without a registry, so the whole stage-and-test cycle stays on one machine.
- **Security is testable, not aspirational.** We run minikube with the **Calico**
  CNI so the chart's NetworkPolicies are actually *enforced* (the default CNI
  ignores them). Combined with a dedicated namespace under the **restricted** Pod
  Security Standard, a hardened container (non-root, read-only rootfs, all
  capabilities dropped, seccomp `RuntimeDefault`), and disabled
  ServiceAccount-token mounting, the security model can be verified end-to-end
  before it ever reaches production.

### Consequences

- **Positive:** Each service deploys, scales, and is secured independently;
  values-driven config makes promotion across environments trivial; the security
  posture is genuinely enforced and testable locally.
- **Trade-off:** Three charts to maintain instead of one, and Kafka/TimescaleDB
  are expected to be provided as endpoints (via values) rather than sub-charts —
  keeping the application charts focused on the application. The
  `deploy/docker-compose.yaml` stack covers the batteries-included local case
  (Kafka + TimescaleDB + Streamer + Collector + API).

---

## 5. Telemetry Streamer: stateless replay, data on a PVC

**Status:** Accepted

### Context

The Streamer replays the sample CSV onto the queue to simulate a live DCGM feed.
The brief sets two shaping requirements: each CSV line is an independent
datapoint, replayed in a loop to simulate a continuous stream; and **the time a
datapoint is processed is its timestamp** (the original CSV timestamp column is
ignored). The Streamer must also **scale up and down dynamically**, like the
Collector. Two design questions follow: how does a fleet of Streamers scale
without producing garbage, and how does the CSV reach each replica?

### Decision

**Scaling — stateless, coordination-free replay.** Each replica loads the whole
dataset and loops over it independently, stamping every datapoint with the wall
-clock time at publish. Messages are keyed by **GPU UUID** so each GPU's series
hashes to one partition and stays ordered end-to-end. No sharding, no leader, no
shared cursor.

**Data — mounted from a PersistentVolume at runtime, not baked into the image.**
The CSV lives on a PVC, loaded once via `make load-streamer-data` (a short-lived
helper pod + `kubectl cp`); the Deployment mounts it read-only and an init
container blocks startup until the file is present. Compose mirrors this with a
read-only bind mount.

### Why stamping at publish time makes scaling safe

Because the timestamp is the publish time, two replicas emitting the *same* CSV
row produce two datapoints at *different* times — distinct rows, not duplicates.
So adding replicas simply multiplies the aggregate telemetry rate
(`replicas / interval`) with no coordination, exactly mirroring how the Collector
scales as competing consumers. The store's idempotency key
`(uuid, metric_name, time)` still protects against true redelivery duplicates.

### Why a PVC over the alternatives

| Option | Verdict |
|---|---|
| **Embed the CSV in the binary (`go:embed`)** | Rejected: couples a ~1 MiB dataset to the image, so changing data means rebuilding/redeploying; the data is not independently versioned. (This was the first cut, then revised.) |
| **ConfigMap** | Rejected: the sample CSV exceeds the **1 MiB ConfigMap limit**, and real datasets are larger still. |
| **PersistentVolume (chosen)** | Decouples data lifecycle from the image: load once, mount read-only into every replica. On single-node minikube a `ReadWriteOnce` claim is shared by all replicas on the node; the init-container gate keeps pods out of `CrashLoop` until the data is loaded. |

### Consequences

- **Positive:** Data and code are versioned and provisioned independently; the
  image stays small; scaling is friction-free (no per-replica data wiring).
- **Positive:** The Streamer programs against `queue.Producer`, so the eventual
  custom queue is a drop-in here too.
- **Trade-off:** `ReadWriteOnce` relies on all replicas sharing minikube's single
  node. On a multi-node cluster this becomes `ReadOnlyMany` (with a provisioner
  that supports it) or a per-pod init container that fetches the data — a values
  change, not a redesign.

---

## 6. Kafka deployment: direct StatefulSet in KRaft mode

**Status:** Accepted (dev/test)

### Context

The pipeline needs a single Kafka broker on minikube for dev and testing. The
initial approach used the `bitnami/kafka` Helm chart, which brought in ZooKeeper,
rolling-upgrade complexity, and repeated Helm `SECURITY WARNING` noise because
the Bitnami chart was designed for the Bitnami Kafka image, not the Confluent
Platform image (`confluentinc/cp-kafka`).

Two additional issues surfaced:

1. **`port is deprecated` warning.** Kubernetes automatically injects a
   `KAFKA_PORT` environment variable into every pod in the namespace whenever a
   `Service` named `kafka` exists. The cp-kafka entrypoint interprets all
   `KAFKA_*` env vars as broker config and warns that `port` is a deprecated key.
2. **KRaft config incomplete.** The initial config set `KAFKA_ZOOKEEPER_CONNECT`
   with no ZooKeeper sidecar; cp-kafka 7.6.0 fell back to KRaft mode but was
   missing the required keys (`KAFKA_PROCESS_ROLES`, `KAFKA_NODE_ID`,
   `KAFKA_CONTROLLER_QUORUM_VOTERS`, `CLUSTER_ID`, `KAFKA_CONTROLLER_LISTENER_NAMES`),
   so the broker never finished starting.

### Decision

Replace the Bitnami Helm chart with a **direct YAML StatefulSet**
([`deploy/helm/kafka/kafka-statefulset.yaml`](deploy/helm/kafka/kafka-statefulset.yaml))
using `confluentinc/cp-kafka:7.6.0` in **KRaft mode** (no ZooKeeper):

- All required KRaft config keys are set explicitly in a ConfigMap.
- A fixed `CLUSTER_ID` makes the storage format deterministic across restarts.
- `enableServiceLinks: false` on the pod spec prevents Kubernetes from injecting
  `KAFKA_PORT` (and other service env vars) into the container.

### Alternatives considered

- **Keep bitnami/kafka with Bitnami's Kafka image.** Rejected: Bitnami's image
  tag scheme (`X.Y.Z-debian-12-rX`) diverges from the Confluent versioning the
  rest of the docs and tooling reference. The Bitnami chart also brings ZooKeeper
  by default, adding resource overhead and config surface that is unnecessary for
  a single dev broker.
- **Configure ZooKeeper as a sidecar.** Rejected: KRaft (ZooKeeper-less) is the
  current direction for all Kafka 3.x+ deployments and is fully supported in
  cp-kafka 7.6.0. Adding a ZooKeeper sidecar would be moving against the grain
  for no benefit.

### Consequences

- **Positive:** No ZooKeeper, no Bitnami image substitution warnings, no
  spurious `KAFKA_PORT` deprecation noise. The broker starts cleanly in KRaft
  mode with a single pod.
- **Trade-off:** A hand-maintained YAML StatefulSet rather than a Helm chart;
  acceptable for a single-broker dev/test deployment but would be replaced by the
  Confluent Helm chart (`confluentinc/cp-helm-charts`) for production.
- **`KAFKA_ADVERTISED_LISTENERS` uses the ClusterIP Service, not the headless
  pod FQDN.** The advertised listener is `PLAINTEXT://kafka:9092` (the stable
  ClusterIP) rather than `PLAINTEXT://kafka-0.kafka-headless.…:9092`. Clients
  always resolve the same stable address; the headless Service is still used for
  KRaft quorum voters (port 9093), which the broker contacts via the pod FQDN on
  controller startup.
- **Default-deny NetworkPolicy requires an explicit `kafka-allow` rule.**  The
  API chart applies a namespace-wide default-deny (`podSelector: {}`, Ingress +
  Egress) so that all Calico-enforced NetworkPolicies in the namespace are opt-in.
  The Kafka pod is subject to this policy like any other pod, but it ships no
  Helm chart, so it has no auto-generated allow rule. Without an explicit
  `kafka-allow` NetworkPolicy, Calico silently drops every connection to the
  broker — including the KRaft controller port (9093) the broker uses to contact
  itself for quorum, causing Kafka to enter an unhealthy state. The
  `kafka-allow` policy, co-applied with the StatefulSet, explicitly opens:
  broker port 9092 ingress from any namespace pod, controller port 9093
  self-ingress/egress (KRaft quorum), and DNS egress to `kube-system`.

---

## 7. TimescaleDB database initialisation: post-install Job

**Status:** Accepted

### Context

The Bitnami `bitnami/postgresql` chart creates the application database (and
runs `initdb` scripts) **only on a fresh, empty data directory**. If the
PersistentVolumeClaim already exists from a previous Helm release — for example,
after a failed first deploy or after re-running `helm upgrade` — the initdb
phase is skipped entirely and the `telemetry` database is never created. The
collector and API pods then crash with `FATAL: database "telemetry" does not
exist`.

A second issue: the Bitnami chart generates the postgres superuser password
during first init and stores it in a Kubernetes Secret. If the PVC is reused
across Helm releases with different auth values, the password in the Secret
drifts from the password in PostgreSQL, so any Job connecting as `postgres` with
the Secret's value fails authentication.

### Decision

After every `helm upgrade --install` in
[`deploy/helm/timescaledb/install.sh`](deploy/helm/timescaledb/install.sh),
apply a Kubernetes Job
([`deploy/helm/timescaledb/db-init-job.yaml`](deploy/helm/timescaledb/db-init-job.yaml))
that:

1. Connects as the **`telemetry` application user** (whose password is always
   in sync in the Secret because it is set on every Helm upgrade via `--set
   auth.password`), not the postgres superuser.
2. Checks whether the `telemetry` database exists; creates it if not (`telemetry`
   has the `CREATEDB` role attribute, granted by the Bitnami chart).
3. Runs `CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;` inside the database.
4. Self-cleans via `ttlSecondsAfterFinished: 300`.

The `install.sh` blocks on `kubectl wait --for=condition=complete` before
declaring the deploy successful, so a failed database init surfaces as a deploy
failure rather than a silent crash loop later.

### Alternatives considered

- **Rely solely on the Bitnami `primary.initdb.scripts` mechanism.** Rejected:
  initdb only runs on a fresh PVC, making the deploy fragile when PVCs persist
  across re-deploys (the normal case on minikube).
- **Connect as postgres superuser.** Rejected: the postgres superuser password
  in the Secret drifts from the value stored in PostgreSQL when the PVC is
  reused across Helm releases (a [documented Bitnami behaviour](https://github.com/bitnami/charts/tree/main/bitnami/postgresql#password-update)).
  Using the application user avoids this entirely.

### Consequences

- **Positive:** The database and extension exist after every deploy, regardless
  of PVC history. The pattern is idempotent and safe to re-run.
- **Positive:** No tight coupling to the Bitnami initdb lifecycle; the Job works
  whether the PVC is brand new or years old.
- **Trade-off:** One extra Kubernetes object and one extra blocking step in the
  install script; the overhead is negligible (~5 s) for a dev/test deployment.

---

## 8. Custom gRPC queue: hand-written bindings and JSON codec override

**Status:** Accepted (Stage 1 complete)

### Context

The custom queue exposes a gRPC API (`Produce`, `Consume`, `Commit`). The natural
path is to define a `.proto` schema, run `protoc` to generate Go types, and build
the service on top of the generated code. Two concerns arose:

1. **Toolchain friction.** `protoc` and its Go plugins are not standard Go
   tooling; adding them to the build requires additional install steps, pinned
   versions, and either a `go generate` hook or a separate code-gen step — all
   overhead for a service that will eventually be retired or significantly
   redesigned as Stage 2–4 land.
2. **Proto dependency.** protoc-generated types implement `proto.Message` and
   are tightly coupled to the protobuf wire format, which is not what we want:
   the pipeline already uses JSON between Streamer and Collector, and the queue
   broker is internal infrastructure — wire format flexibility matters more than
   protobuf compatibility.

### Decision

**Write the gRPC service bindings by hand** and **override the gRPC codec** to
use `encoding/json` instead of protobuf.

**Hand-written bindings** (`internal/queue/grpc/api.go`) define the request /
response types as plain Go structs and register the service using gRPC's
low-level `grpc.ServiceDesc` + handler functions. No `protoc` dependency, no
generated files, no `proto.Message` interface required.

**JSON codec override** (`internal/queue/grpc/codec.go`) registers a codec named
`"proto"` via `encoding.RegisterCodec`:

```go
type jsonCodec struct{}
func (jsonCodec) Marshal(v any) ([]byte, error)     { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                       { return "proto" }
func init() { encoding.RegisterCodec(jsonCodec{}) }
```

The init runs after gRPC's built-in `encoding/proto` codec init (because the
grpc package is a transitive import), so `jsonCodec{}` overwrites the default.
grpc-go's `getCodec("proto")` checks `GetCodec` (the v1 registry) before
`GetCodecV2`, finds `jsonCodec{}`, wraps it in a `codecV1Bridge`, and JSON
marshaling is used end-to-end on both client and server.

The proto schema (`proto/queue.proto`) is kept as documentation and a reference
for future code generation (Stages 2–4 may regenerate when the API stabilises),
but is **not used to generate code today**.

### Alternatives considered

- **Use `protoc`-generated types.** Rejected for Stage 1: adds toolchain friction
  and couples the wire format to protobuf for an internal service. Revisit if the
  queue needs to interoperate with non-Go clients.
- **Use a non-`proto` codec name** (e.g. register as `"json"`). Rejected: the
  gRPC content-type header defaults to `application/grpc+proto`; both sides must
  agree on the same codec name. Naming our codec `"proto"` keeps the default
  content-type and avoids requiring explicit codec negotiation on every call.
- **Use `google.golang.org/protobuf/proto` or `github.com/gogo/protobuf`.**
  Rejected: the hand-written approach has zero generated files and zero proto
  dependencies outside of `google.golang.org/grpc` which was already required.

### Consequences

- **Positive:** Zero `protoc` toolchain dependency; `go build ./...` is
  self-contained. Adding new RPC methods is a plain Go struct + handler, no
  code-gen step.
- **Positive:** Wire format (JSON) is human-readable and debuggable with standard
  tools (`kubectl exec … curl`, `grpcurl -plaintext -d '{}'`).
- **Trade-off:** The hand-written `grpc.ServiceDesc` + codec override is not
  idiomatic gRPC Go. Future developers need to understand the codec registration
  trick to debug marshaling issues. The comment in `codec.go` and this ADR
  document the reasoning.
- **Trade-off:** JSON is slower and larger on the wire than protobuf. For Stage 1
  (in-memory, loopback gRPC) this is negligible. If Stage 4 (peer replication)
  introduces cross-node replication at high throughput, re-evaluating the codec
  is worthwhile.
