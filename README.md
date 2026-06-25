# Elastic GPU Telemetry Pipeline with a Custom Message Queue

An elastic, horizontally-scalable telemetry pipeline for an AI/GPU cluster. The
goal is to stream GPU telemetry (DCGM exporter CSV) through a **custom message
queue** (no Kafka/RabbitMQ/etc.), persist it, and expose it over a REST API with
an auto-generated OpenAPI spec.

> **Status — end-to-end pipeline running.** The full path works today: the
> **Telemetry Streamer** replays GPU telemetry from a CSV onto the queue, the
> **Telemetry Collector** consumes and persists it to PostgreSQL/TimescaleDB, and
> the **REST API** (Gin + Swagger/OpenAPI) serves it back, reading from the same
> store. All three are horizontally scalable and ship as hardened containers with
> Helm deployments onto minikube. This README describes **what exists today** and
> grows as the project does.

> **Custom gRPC Message Queue (Stage 1 complete).** The Collector and Streamer
> are written against small `queue.Consumer` / `queue.Producer` interfaces so the
> queue is a swappable implementation detail. **The custom gRPC queue is the
> primary implementation** (`QUEUE_TYPE=grpc`, enabled by default in the Helm
> deploy). Kafka remains available as an alternative (`QUEUE_TYPE=kafka`) and is
> kept for comparison and compose-stack testing. The design and phased roadmap for
> the custom queue are in [QUEUE.md](QUEUE.md).

## Table of contents
- [What's in the repo today](#whats-in-the-repo-today)
- [Tech stack (today)](#tech-stack-today)
- [Telemetry Collector](#telemetry-collector)
- [Telemetry Streamer](#telemetry-streamer)
- [REST API](#rest-api)
- [OpenAPI / Swagger](#openapi--swagger)
- [Reference data](#reference-data)
- [Build](#build)
- [Container image](#container-image)
- [Deploy to minikube (Helm)](#deploy-to-minikube-helm)
- [Security](#security)
- [Accessing the service](#accessing-the-service)
- [Make targets](#make-targets)
- [Roadmap](#roadmap)
- [AI assistance](#ai-assistance)

## What's in the repo today

```
cmd/api/main.go       API gateway entry point (graceful shutdown, slog logging)
cmd/collector/main.go Telemetry Collector entry point (health server, shutdown)
cmd/streamer/main.go  Telemetry Streamer entry point (health server, shutdown)
cmd/queue/main.go     Custom gRPC Message Queue broker (Stateless MVP)
internal/api/         REST API layer (Gin)
  handler.go            request handlers + OpenAPI annotations (presentation)
  router.go             routes, structured logging, Swagger UI route
  service/              business layer between handlers and the store (API-only)
internal/telemetry/   shared on-the-wire telemetry Record (producer <-> consumer)
internal/queue/       queue.Consumer / queue.Producer abstractions (technology-agnostic)
  kafka/                Kafka implementation (segmentio/kafka-go: groups + producer)
  grpc/                 Custom gRPC implementation (Client + API descriptors)
internal/store/       store.TelemetryStore (write) + TelemetryReader (read) abstractions
  postgres/             PostgreSQL/TimescaleDB implementation (pgx)
internal/collector/   the collector engine (batch -> persist -> commit)
internal/streamer/    the streamer engine (CSV -> stamp -> publish)
internal/queue/server/  The in-memory gRPC broker logic (storage, offsets, locking)
internal/config/      env-driven configuration (API + Collector + Streamer)
.dockerignore         build-context exclusions
deploy/
  build/
    Dockerfile.api        API image (multi-stage -> distroless static)
    Dockerfile.collector  Collector image (separate, so it scales independently)
    Dockerfile.streamer   Streamer image (separate, so it scales independently)
    Dockerfile.queue     Queue image (custom gRPC broker, distroless static)
  namespace.yaml        dedicated, security-hardened namespace (restricted PSA)
  streamer-data-pvc.yaml      PVC holding the telemetry CSV (loaded at runtime)
  streamer-data-loader.yaml   helper pod used to copy the CSV onto the PVC
  docker-compose.yaml   local dev stack: Kafka + TimescaleDB + streamer + collector + api
  helm/gpu-telemetry-api/        API Helm chart
  helm/gpu-telemetry-collector/  Collector Helm chart (Deployment + HPA + NetPol)
  helm/gpu-telemetry-streamer/   Streamer Helm chart (Deployment + HPA + NetPol)
  helm/gpu-telemetry-queue/      Custom gRPC Queue Helm chart (Deployment + Service)
  helm/kafka/                    Single-node Kafka StatefulSet (KRaft, cp-kafka:7.6.0, no Zookeeper)
  helm/timescaledb/              TimescaleDB Helm values (bitnami/postgresql + db-init Job)
Makefile              build / test / coverage / deploy targets (see "Make targets")
DECISION.md           design decisions + rationale (DB, queue, Make, Helm)
QUEUE.md              custom gRPC message queue design (stateless broker, phased plan, smart flush)
go.mod / go.sum       Go module (module gpu-telemetry-pipeline)
project_docs/
  AI_PROMPTS.md         how AI assistance was used
  dcgm_metrics_*.csv    sample DCGM telemetry data
  GPU Telemetry Pipeline Message Queue.pdf   the project brief
PROJECT_SETUP.md      bootstrap steps
README.md             this file
```

## Tech stack (today)

- **Language:** Go 1.26
- **HTTP/REST:** [Gin](https://github.com/gin-gonic/gin)
- **OpenAPI:** [swaggo/swag](https://github.com/swaggo/swag) + gin-swagger (spec
  generated from handler annotations)
- **Message queue:** 
    - [Kafka](https://kafka.apache.org/) via the pure-Go [segmentio/kafka-go](https://github.com/segmentio/kafka-go) client.
    - **Custom gRPC Queue** (MVP) utilizing `google.golang.org/grpc` for high-performance binary streaming.
    - Both are behind `queue.Consumer` / `queue.Producer` interfaces.
- **Persistence:** [PostgreSQL](https://www.postgresql.org/) +
  [TimescaleDB](https://www.timescale.com/) via the pure-Go
  [pgx](https://github.com/jackc/pgx) driver (behind `store.TelemetryStore`
  write and `store.TelemetryReader` read interfaces)
- **Container:** multi-stage Docker build on a `distroless/static:nonroot` base
  (`CGO_ENABLED=0`)
- **Orchestration:** Kubernetes via [minikube](https://minikube.sigs.k8s.io/),
  packaged with [Helm](https://helm.sh/), [Calico](https://www.tigera.io/project-calico/)
  CNI for NetworkPolicy enforcement

## Telemetry Collector

The Collector ([`cmd/collector`](cmd/collector/main.go)) is the ingest worker of
the pipeline. Each instance:

1. **Consumes** telemetry from the queue as a member of a shared consumer group.
2. **Parses & validates** each message into a [`telemetry.Record`](internal/telemetry/record.go)
   (records missing a UUID / metric / timestamp are dropped, not stored).
3. **Persists** records to TimescaleDB in batches.
4. **Acknowledges** the queue only *after* a batch is durably stored.

### Architecture

```
                ┌─────────────┐   competing consumers (one Kafka group)
   Kafka topic  │ partition 0 ├───────────────┐
 "gpu-telemetry"│ partition 1 ├──────────┐    │
  (N partitions)│   ...       │          ▼    ▼
                │ partition 9 │     ┌──────────────┐   batch insert   ┌────────────┐
                └─────┬───────┘     │  Collector   ├─────────────────►│ TimescaleDB│
                      └────────────►│  (replica)   │  ON CONFLICT     │ hypertable │
                                    └──────────────┘  DO NOTHING      └────────────┘
```

- **`internal/queue`** — the `Consumer`/`Producer` interfaces. The Collector and
  Streamer depend only on these, never on a specific broker.
- **`internal/queue/grpc`** — the custom gRPC queue implementation (primary, enabled
  via `QUEUE_TYPE=grpc`; hand-written bindings, JSON codec override, in-memory broker).
- **`internal/queue/kafka`** — the Kafka implementation (consumer groups, manual
  offset commits) using the pure-Go `segmentio/kafka-go`.
- **`internal/store`** + **`internal/store/postgres`** — the `TelemetryStore`
  interface and its TimescaleDB implementation.
- **`internal/collector`** — the engine: a single consume$\rightarrow$batch$\rightarrow$persist$\rightarrow$commit
  loop per instance, kept simple so at-least-once semantics are easy to reason
  about.

### Dynamic scaling (the headline requirement)

Scaling is **horizontal and configuration-free**: every replica joins the same
consumer group (`KAFKA_GROUP_ID` for Kafka, `groupID` for gRPC), and the queue
distributes the load across the live members, rebalancing automatically as replicas are
added or removed. To scale:

- **Kubernetes:** the [collector Helm chart](deploy/helm/gpu-telemetry-collector/)
  ships a `HorizontalPodAutoscaler` (default 1–10 replicas on CPU). Or scale
  manually: `kubectl -n gpu-telemetry scale deploy/<release>-gpu-telemetry-collector --replicas=N`.
- **Compose:** `docker compose -f deploy/docker-compose.yaml up --scale collector=3`.

### Delivery semantics

**At-least-once.** A batch's queue offsets are committed only after the batch is
stored. If a Collector crashes mid-batch, the queue redelivers it. Re-insertion
is made **idempotent** by a unique key `(uuid, metric_name, time)` plus
`ON CONFLICT DO NOTHING`, so redelivery never creates duplicates. Unparseable
("poison") messages are dropped *and* committed, so a bad message can't wedge a
partition.

### Storage schema

Telemetry lands in the `gpu_telemetry` TimescaleDB **hypertable** (auto-partitioned
by `time`), indexed on `(uuid, time DESC)` for the API's "telemetry by GPU,
ordered by time, optionally windowed" query. If the `timescaledb` extension is
unavailable, the table still works as plain PostgreSQL (logged as a warning) so
the pipeline stays runnable.

### Configuration (environment variables)

| Variable | Default | Description |
|---|---|---|
| `QUEUE_TYPE` | `kafka` | Type of queue implementation (`kafka` or `grpc`). |
| `QUEUE_ADDR` | `localhost:50051` | Address of the gRPC queue service. |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list (used if `QUEUE_TYPE=kafka`). |
| `KAFKA_TOPIC` | `gpu-telemetry` | Topic to consume. |
| `KAFKA_GROUP_ID` | `telemetry-collectors` | Consumer group (shared by all replicas). |
| `POSTGRES_DSN` | `postgres://telemetry:telemetry@localhost:5432/telemetry?sslmode=disable` | TimescaleDB connection string. |
| `COLLECTOR_BATCH_SIZE` | `500` | Records per flush. |
| `COLLECTOR_FLUSH_INTERVAL` | `1s` | Max time a partial batch waits before flushing. |
| `COLLECTOR_FLUSH_TIMEOUT` | `15s` | Bound on a single persist+commit attempt. |
| `COLLECTOR_HEALTH_ADDR` | `:8081` | Address for the health/stats server. |

### Health & observability

The Collector serves `GET /healthz` (liveness), `GET /readyz` (readiness —
pings the DB), and `GET /stats` (JSON counters: `persisted`, `dropped`,
`batches`, `flush_errors`) on `COLLECTOR_HEALTH_ADDR`.

### Run it locally

The compose stack brings up Kafka (KRaft), TimescaleDB, the 10-partition topic,
the Streamer, and the Collector:

```bash
docker compose -f deploy/docker-compose.yaml up --build
# scale both sides and watch the pipeline absorb load:
docker compose -f deploy/docker-compose.yaml up --build --scale streamer=3 --scale collector=3
```

The Streamer replays its embedded CSV onto the topic and the Collector persists
it to TimescaleDB. Scale either side independently to demonstrate elastic
throughput.

### Deploy to Kubernetes

```bash
make deploy-collector \
  COLLECTOR_CHART_DIR=deploy/helm/gpu-telemetry-collector
# point it at your Queue/Kafka/TimescaleDB via --set or a values file, e.g.:
#   --set queue.type=grpc --set queue.addr=gpu-telemetry-queue:50051
minikube addons enable metrics-server   # required for the HPA
```

## Telemetry Streamer

The Streamer ([`internal/streamer`](internal/streamer/), entrypoint
[`cmd/streamer`](cmd/streamer/)) replays GPU telemetry onto the queue to simulate
a live DCGM feed. It loads records from a CSV, then publishes them one at a time
on a fixed interval, **stamping each datapoint with the time it is published**
(per the brief, processing time *is* the datapoint's timestamp). With `loop`
enabled it replays the dataset endlessly, so the same rows reappear as a
continuous stream of fresh datapoints.

The CSV is **mounted at runtime** (`STREAMER_CSV_PATH`), not baked into the image:
in Kubernetes it lives on a **PersistentVolume** loaded via `make load-streamer-data`,
and in Compose it is a read-only bind mount. This keeps the dataset (which exceeds
the 1 MiB ConfigMap limit) decoupled from the image, so data and code are versioned
and provisioned independently.

Each message is keyed by **GPU UUID**, so a given GPU's datapoints hash to one
partition and stay ordered end-to-end. The Streamer programs against the
`queue.Producer` interface; the **custom gRPC queue is the primary implementation**,
with Kafka available as an alternative.

### Dynamic scaling (the headline requirement)

Scaling is **horizontal and coordination-free**: each replica streams the full
dataset independently. Because every datapoint's timestamp is its publish time,
two replicas emitting the same CSV row produce two *distinct* datapoints rather
than a duplicate — so adding replicas simply multiplies the telemetry rate, with
no sharding or shared state. To scale:

- **Kubernetes:** the [streamer Helm chart](deploy/helm/gpu-telemetry-streamer/)
  ships a `HorizontalPodAutoscaler` (default 1–10 replicas on CPU). Or scale
  manually: `kubectl -n gpu-telemetry scale deploy/<release>-gpu-telemetry-streamer --replicas=N`.
- **Compose:** `docker compose -f deploy/docker-compose.yaml up --scale streamer=3`.

The fleet's aggregate rate is `replicas / STREAMER_INTERVAL`; scale Collectors to
keep pace.

### Configuration (environment variables)

| Variable | Default | Description |
|---|---|---|
| `QUEUE_TYPE` | `kafka` | Type of queue implementation (`kafka` or `grpc`). |
| `QUEUE_ADDR` | `localhost:50051` | Address of the gRPC queue service. |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list (used if `QUEUE_TYPE=kafka`). |
| `KAFKA_TOPIC` | `gpu-telemetry` | Topic to publish to. |
| `STREAMER_CSV_PATH` | *(required)* | Telemetry source file, read at runtime (a PV mount in k8s). |
| `STREAMER_INTERVAL` | `10ms` | Per-replica delay between datapoints. |
| `STREAMER_LOOP` | `true` | Replay the dataset endlessly. |
| `STREAMER_HEALTH_ADDR` | `:8082` | Address for the health/stats server. |

### Health & observability

The Streamer serves `GET /healthz`, `GET /readyz`, and `GET /stats` (JSON
counters: `streamed`, `publish_errors`, `loops`) on `STREAMER_HEALTH_ADDR`.

### Deploy to Kubernetes

`make deploy-streamer` provisions the data PVC, copies the CSV onto it
(`load-streamer-data`), then installs the chart:

```bash
make deploy-streamer
# point it at your Queue/Kafka via --set or a values file, e.g.:
#   --set queue.type=grpc --set queue.addr=gpu-telemetry-queue:50051
minikube addons enable metrics-server   # required for the HPA
```

Pods wait in `Init` (an init container blocks on the CSV) until the data is
present, so you can also load it independently with `make load-streamer-data`.

## REST API

The API ([`internal/api`](internal/api/), entrypoint [`cmd/api`](cmd/api/main.go))
reads telemetry from the same TimescaleDB the Collector writes to. It is layered
**handlers $\rightarrow$ service $\rightarrow$ store**: the handlers
([`handler.go`](internal/api/handler.go)) decode HTTP and map errors to status
codes; a [`service`](internal/api/service/) layer (bundled with the API only)
holds the business logic — input validation, query-limit defaulting/clamping,
and orchestration; the store's read side (`store.TelemetryReader`) does data
access. Base path `/api/v1`:

| Method & path | Description |
|---|---|
| `GET /api/v1/gpus` | List all GPUs that have reported telemetry (uuid, model, hostname, last seen). |
| `GET /api/v1/gpus/{id}/telemetry` | A GPU's telemetry, newest first (default 1000 rows, max 10000). |
| `GET /api/v1/gpus/{id}/telemetry?start_time=…&end_time=…&limit=…` | Same, filtered to an inclusive RFC3339 time window with a row limit. |
| `GET /healthz` | Liveness probe. |
| `GET /readyz` | Readiness probe — pings the datastore. |
| `GET /swagger/*any` | Swagger UI and the raw OpenAPI spec. |

The query endpoint is served by the `(uuid, time DESC)` index on the hypertable.
Invalid `start_time`/`end_time` (non-RFC3339) or a non-positive `limit` return
`400`; a datastore failure returns `500`.

### Configuration (environment variables)

| Variable | Default | Description |
|---|---|---|
| `API_LISTEN_ADDR` | `:8080` | Address the HTTP server listens on. |
| `POSTGRES_DSN` | `postgres://telemetry:telemetry@localhost:5432/telemetry?sslmode=disable` | TimescaleDB connection string (read side). |

### Run it

The API is part of the Compose stack (host `:8080`), reading what the Collector
persists:

```bash
docker compose -f deploy/docker-compose.yaml up --build
curl localhost:8080/api/v1/gpus
curl "localhost:8080/api/v1/gpus/<uuid>/telemetry?limit=20"
```

In Kubernetes the [API Helm chart](deploy/helm/gpu-telemetry-api/) injects
`POSTGRES_DSN` from a Secret and exposes the service via NodePort
(`make deploy`).

## OpenAPI / Swagger

The OpenAPI spec is generated from the annotations on the handlers (the source
of truth), rather than maintained by hand:

```bash
make openapi
```

The Swagger UI is wired into the router and served at `/swagger/index.html` (raw
spec at `/swagger/doc.json`).

## Reference data

- [`project_docs/dcgm_metrics_20250718_134233.csv`](project_docs/dcgm_metrics_20250718_134233.csv)
  — a sample of DCGM exporter telemetry used as the input data set.
- [`project_docs/GPU Telemetry Pipeline Message Queue.pdf`](project_docs/GPU%20Telemetry%20Pipeline%20Message%20Queue.pdf)
  — the project brief.

## Build

Requires Go 1.26+.

```bash
go build ./...
```

## Container images

The Dockerfiles live under [`deploy/build/`](deploy/build/), one per service.
Each is multi-stage: it builds a statically-linked binary (`CGO_ENABLED=0`,
stripped) and copies it into a `gcr.io/distroless/static-debian12:nonroot` runtime
stage — no shell, no package manager, runs as a non-root user (uid `65532`).
The result is a small, low-attack-surface image. The build context is the repo root
(where the Go module lives), selected with `-f`:

```bash
make docker-build            # -f deploy/build/Dockerfile.api       -> gpu-telemetry-api:0.1.0
make docker-build-collector  # -f deploy/build/Dockerfile.collector -> gpu-telemetry-collector:0.1.0
make docker-build-streamer   # -f deploy/build/Dockerfile.streamer -> gpu-telemetry-streamer:0.1.0
make docker-build-queue       # -f deploy/build/Dockerfile.queue   -> gpu-telemetry-queue:0.1.0
```

The Dockerfile paths are overridable (`API_DOCKERFILE`, `COLLECTOR_DOCKERFILE`,
`STREAMER_DOCKERFILE`, `QUEUE_DOCKERFILE`), as are the image names and tag (`IMAGE`,
`COLLECTOR_IMAGE`, `STREAMER_IMAGE`, `QUEUE_IMAGE`, `TAG`).

## Deploy to minikube (Helm)

Prerequisites: `docker`, `minikube`, `kubectl`, and `helm`. If you don't have
them, `make setup-infra` installs them (Ubuntu/Debian). Then bring up a cluster
**with the Calico CNI** so the chart's NetworkPolicies are actually enforced
(the default CNI ignores them):

```bash
make start-minikube                              # docker driver + Calico
# running minikube as root? add: MINIKUBE_EXTRA_ARGS=--force
```

### One command

**Custom gRPC queue (recommended):**
```bash
make deploy QUEUE_TYPE=grpc
```

**Kafka:**
```bash
make deploy-kafka && make deploy QUEUE_TYPE=kafka
```

### Step by step

| Step | Command | What it does |
|---|---|---|
| 1 | `make deploy-timescaledb` | Installs bitnami/postgresql + db-init Job (creates `telemetry` DB, enables TimescaleDB extension) |
| 2a *(gRPC)* | `make deploy-queue` | Builds gRPC queue image, loads into minikube, installs Helm chart + Service |
| 2b *(Kafka)* | `make deploy-kafka` | Deploys Kafka as a KRaft StatefulSet (no Zookeeper) |
| 3 | `make deploy-collector QUEUE_TYPE=grpc` | Builds collector image, loads into minikube, installs chart + HPA |
| 4 | `make deploy-streamer QUEUE_TYPE=grpc` | Loads CSV onto PVC, builds streamer image, loads into minikube, installs chart + HPA |
| 5 | `make deploy-api` | Builds API image, loads into minikube, installs Helm chart |

> For Kafka, omit `QUEUE_TYPE=grpc` from steps 3–4 (or pass `QUEUE_TYPE=kafka` explicitly) — `kafka` is the binary default.

Each `deploy-*` target builds the Docker image, loads it into minikube
(`imagePullPolicy: IfNotPresent`), creates the hardened `gpu-telemetry` namespace
if absent, and runs `helm upgrade --install --wait`.

Check what's running after deploy:

```bash
make status        # pods, services, networkpolicies, namespace labels
make service-url   # print the URL for the API (keep this process running for Windows access)
```

Tear it down:

```bash
make undeploy-all  # remove all releases + namespace
make undeploy      # remove API release + namespace only
```

## Security

The deployment is hardened by default:

- **Dedicated namespace** with **restricted** Pod Security Admission
  (`enforce` + `audit` + `warn`), so no privileged/root/capability-bearing pod
  can be admitted.
- **Hardened container** — `runAsNonRoot`, `readOnlyRootFilesystem`,
  `allowPrivilegeEscalation: false`, all Linux capabilities dropped, and the
  `RuntimeDefault` seccomp profile. A writable `emptyDir` is mounted at `/tmp`.
- **ServiceAccount with token auto-mount disabled** — the API does not talk to
  the Kubernetes API, so no token is projected into the pod (no RBAC granted).
- **NetworkPolicies** (enforced by Calico): the API chart applies a
  namespace-wide default-deny (`podSelector: {}`, Ingress + Egress), then each
  component ships an explicit allow policy. DNS egress is permitted everywhere;
  the API adds Postgres egress, in-namespace HTTP, and external ingress to its
  port; the Collector adds Queue/Kafka/Postgres egress and health ingress; the Streamer
  adds Queue/Kafka egress and health ingress; and the Queue service has an explicit
  allow rule for its gRPC port.

## Accessing the service

The Service is a **NodePort** (`30080`). With the docker driver the cluster
network isn't routable from Windows, so two native paths cover access (no
`kubectl port-forward`, no extra tooling):

```bash
# From WSL/Linux — the minikube node IP is routable:
curl http://$(minikube ip):30080/healthz

# From the Windows host — minikube's own tunnel binds 127.0.0.1 on the WSL
# host, reachable from Windows via WSL2 localhost forwarding. Keep it running:
make service-url      # prints http://127.0.0.1:<port>
# then from Windows: curl http://localhost:<port>/healthz
```

Once reachable, the **Swagger UI** is at `/swagger/index.html` (raw spec at
`/swagger/doc.json`) — e.g. `http://$(minikube ip):30080/swagger/index.html`
from WSL. Swagger's "Try it out" calls the same origin that served the page,
so no extra configuration is needed.

> The tunnel binds `127.0.0.1`, so it reaches the Windows host but not other
> machines on the LAN. For LAN access, use a `LoadBalancer` service +
> `minikube tunnel`.

> **Reference:** the minikube IP, ports, and Swagger URLs are recorded in
> [`deploy/ACCESS.md`](deploy/ACCESS.md) for making API calls later.

## Make targets

| Target | What it does |
|---|---|
| `make setup-infra` | Install Docker, minikube, kubectl, Helm (Ubuntu/Debian). |
| `make start-minikube` | Start minikube (docker driver + Calico CNI). |
| `make docker-build` | Build the image `gpu-telemetry-api:0.1.0`. |
| `make minikube-load` | Build, then load the image into minikube. |
| `make namespace` | Create + label the hardened `gpu-telemetry` namespace. |
| `make helm-lint` / `make helm-template` | Lint / render the API chart. |
| `make deploy` | Full pipeline: TimescaleDB $\rightarrow$ Queue $\rightarrow$ Collector $\rightarrow$ Streamer $\rightarrow$ API. |
| `make deploy-timescaledb` | Deploy TimescaleDB (bitnami/postgresql chart + db-init Job). |
| `make deploy-queue` | Build $\rightarrow$ load $\rightarrow$ install the custom gRPC queue chart. |
| `make docker-build-collector` | Build the collector image `gpu-telemetry-collector:0.1.0`. |
| `make deploy-collector` | Build $\rightarrow$ load $\rightarrow$ install the collector chart. |
| `make undeploy-collector` | Uninstall the collector release. |
| `make docker-build-streamer` | Build the streamer image `gpu-telemetry-streamer:0.1.0`. |
| `make load-streamer-data` | Provision the data PVC and copy the CSV onto it. |
| `make deploy-streamer` | Load data $\rightarrow$ build $\rightarrow$ load $\rightarrow$ install the streamer chart. |
| `make undeploy-streamer` | Uninstall the streamer release. |
| `make deploy-api` | Build $\rightarrow$ load API image $\rightarrow$ install the API Helm chart. |
| `make undeploy-api` | Uninstall the API Helm release. |
| `make status` | Show workloads + security objects in the namespace. |
| `make service-url` / `make expose` | Print URL / open a tunnel for host access. |
| `make undeploy` | Uninstall the API release and delete the namespace. |
| `make openapi` | Regenerate the OpenAPI spec from handler annotations. |
| `make test` | Run all unit tests with the race detector. |
| `make cover` | Run tests and print total statement coverage. |
| `make cover-html` | Generate a browsable `coverage.html` report. |

## Switching between Kafka and the custom gRPC queue

The queue is selected via `QUEUE_TYPE`. Only the active broker's settings are used.

| `QUEUE_TYPE` | Pod needed | Key env var |
|---|---|---|
| `grpc` | `make deploy-queue` | `QUEUE_ADDR=gpu-telemetry-queue:50051` |
| `kafka` | `make deploy-kafka` | `KAFKA_BROKERS=kafka:9092` |

See [Deploy to minikube](#deploy-to-minikube-helm) for the full command sequences.

**Docker Compose always runs Kafka** — no flag needed:
```bash
docker compose -f deploy/docker-compose.yaml up --build
```

## Roadmap

Done:
- ✅ **Custom gRPC Message Queue (Stage 1 MVP)** — Stateless in-memory broker implementing the `queue` interfaces, deployed via Helm.
- ✅ **Containerisation and Kubernetes (minikube) deployment** via Helm, with a
  security-hardened, dedicated namespace.
- ✅ **Telemetry Streamer** — scalable CSV replayer that publishes to the queue,
  stamping each datapoint with its processing time; reads its data from a
  PersistentVolume at runtime; its own image, Helm chart, and HPA.
- ✅ **Telemetry Collector** — scalable competing-consumer that parses and
  persists telemetry, with its own image, Helm chart, and HPA.
- ✅ **Persistence layer** — PostgreSQL/TimescaleDB behind `TelemetryStore`
  (write) and `TelemetryReader` (read) interfaces.
- ✅ **REST API** — reads telemetry back from TimescaleDB (`GET /api/v1/gpus`,
  `GET /api/v1/gpus/{id}/telemetry`), with OpenAPI/Swagger.
- ✅ **End-to-end pipeline** — Streamer $\rightarrow$ Queue $\rightarrow$ Collector $\rightarrow$ TimescaleDB $\rightarrow$ API,
  demonstrable via Docker Compose and on minikube.
- ✅ **Unit tests + coverage** via the Makefile (`make cover`).

Remaining:
- **Stage 2 Custom Queue**: Shared persistence (S3/EFS), smart flush algorithm (size + time + memory-pressure thresholds), Queue-vs-Log mode.
- **Stage 3 Custom Queue**: Consumer group state (`__consumer_offsets` internal topic), per-group offset tracking.
- **Stage 4 Custom Queue**: In-memory peer replication, Metadata Raft cluster, failover for high availability.

## AI assistance

This repository's initial structure was scaffolded with Claude Code. A detailed
account of the prompts used and where AI needed manual intervention is in
[`project_docs/AI_PROMPTS.md`](project_docs/AI_PROMPTS.md).
