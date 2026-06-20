# Elastic GPU Telemetry Pipeline with a Custom Message Queue

An elastic, horizontally-scalable telemetry pipeline for an AI/GPU cluster. The
goal is to stream GPU telemetry (DCGM exporter CSV) through a **custom message
queue** (no Kafka/RabbitMQ/etc.), persist it, and expose it over a REST API with
an auto-generated OpenAPI spec.

> **Status — building out the pipeline.** This repository contains the REST API
> layer (Gin) with Swagger/OpenAPI, a hardened container + Helm deployment onto
> minikube, and now the **Telemetry Collector**: a horizontally-scalable service
> that consumes telemetry from a message queue, parses it, and persists it to
> PostgreSQL/TimescaleDB. The Streamer and the API's data-backed handlers are
> next. This README describes **what exists today** and grows as the project does.

> **A note on the message queue.** The brief's end goal is a *custom* message
> queue (no Kafka/RabbitMQ). The Collector is deliberately written against a
> small `queue.Consumer` interface so the queue technology is a swappable
> implementation detail. **Kafka is the first implementation** (used for now);
> dropping in the custom queue later means adding one more `Consumer`
> implementation, with no change to collector logic. See
> [Telemetry Collector](#telemetry-collector).

## Table of contents
- [What's in the repo today](#whats-in-the-repo-today)
- [Tech stack (today)](#tech-stack-today)
- [Telemetry Collector](#telemetry-collector)
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
internal/api/         REST API layer (Gin)
  handler.go            request handlers + OpenAPI annotations
  router.go             routes, structured logging, Swagger UI route
internal/telemetry/   shared on-the-wire telemetry Record (producer <-> consumer)
internal/queue/       queue.Consumer abstraction (technology-agnostic)
  kafka/                Kafka implementation (segmentio/kafka-go, consumer groups)
internal/store/       store.TelemetryStore abstraction (database-agnostic)
  postgres/             PostgreSQL/TimescaleDB implementation (pgx)
internal/collector/   the collector engine (batch -> persist -> commit)
internal/config/      env-driven configuration (API + Collector)
.dockerignore         build-context exclusions
deploy/
  build/
    Dockerfile.api        API image (multi-stage -> distroless static)
    Dockerfile.collector  Collector image (separate, so it scales independently)
  namespace.yaml        dedicated, security-hardened namespace (restricted PSA)
  docker-compose.yaml   local dev stack: Kafka + TimescaleDB + collector
  helm/gpu-telemetry-api/        API Helm chart
  helm/gpu-telemetry-collector/  Collector Helm chart (Deployment + HPA + NetPol)
Makefile              build / test / coverage / deploy targets (see "Make targets")
DECISION.md           design decisions + rationale (DB, queue, Make, Helm)
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
- **Message queue:** [Kafka](https://kafka.apache.org/) via the pure-Go
  [segmentio/kafka-go](https://github.com/segmentio/kafka-go) client (behind a
  `queue.Consumer` interface — see the message-queue note above)
- **Persistence:** [PostgreSQL](https://www.postgresql.org/) +
  [TimescaleDB](https://www.timescale.com/) via the pure-Go
  [pgx](https://github.com/jackc/pgx) driver (behind a `store.TelemetryStore`
  interface)
- **Container:** multi-stage Docker build on a `distroless/static:nonroot` base
  (`CGO_ENABLED=0`) — which is why both the Kafka and Postgres clients are
  pure-Go (no librdkafka / libpq)
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

- **`internal/queue`** — the `Consumer` interface. The Collector depends only on
  this, never on Kafka directly, so the brief's eventual *custom* message queue
  is a drop-in replacement.
- **`internal/queue/kafka`** — the Kafka implementation (consumer groups, manual
  offset commits) using the pure-Go `segmentio/kafka-go`.
- **`internal/store`** + **`internal/store/postgres`** — the `TelemetryStore`
  interface and its TimescaleDB implementation.
- **`internal/collector`** — the engine: a single consume→batch→persist→commit
  loop per instance, kept simple so at-least-once semantics are easy to reason
  about.

### Dynamic scaling (the headline requirement)

Scaling is **horizontal and configuration-free**: every replica joins the same
Kafka consumer group (`KAFKA_GROUP_ID`), and Kafka distributes the topic's
partitions across the live members, rebalancing automatically as replicas are
added or removed. To scale:

- **Kubernetes:** the [collector Helm chart](deploy/helm/gpu-telemetry-collector/)
  ships a `HorizontalPodAutoscaler` (default 2–10 replicas on CPU). Or scale
  manually: `kubectl -n gpu-telemetry scale deploy/<release>-gpu-telemetry-collector --replicas=N`.
- **Compose:** `docker compose -f deploy/docker-compose.yaml up --scale collector=3`.

> **Provision enough partitions.** Effective parallelism is capped by the topic's
> partition count — replicas beyond the number of partitions sit idle. The dev
> stack and the brief's 10-instance cap assume **≥ 10 partitions** (the compose
> `kafka-init` creates the topic with 10).

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
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list. |
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
and the Collector:

```bash
docker compose -f deploy/docker-compose.yaml up --build
# scale the collectors and watch the group rebalance:
docker compose -f deploy/docker-compose.yaml up --build --scale collector=3
```

There is no Streamer yet, so the Collector idles until something produces to the
topic; it will still connect, create the hypertable, and report `/readyz` healthy.

### Deploy to Kubernetes

```bash
make deploy-collector \
  COLLECTOR_CHART_DIR=deploy/helm/gpu-telemetry-collector
# point it at your Kafka/TimescaleDB via --set or a values file, e.g.:
#   --set kafka.brokers=kafka:9092 --set postgres.dsn=...
minikube addons enable metrics-server   # required for the HPA
```

## REST API

Base path `/api/v1`. The handlers and routes defined today:

| Method & path | Description |
|---|---|
| `GET /api/v1/gpus` | List all GPUs that have reported telemetry. |
| `GET /api/v1/gpus/{id}/telemetry` | All telemetry for a GPU, ordered by time. |
| `GET /api/v1/gpus/{id}/telemetry?start_time=…&end_time=…&limit=…` | Same, filtered to an inclusive RFC3339 time window with an optional row limit. |
| `GET /healthz` | Liveness/readiness probe. |
| `GET /swagger/*any` | Swagger UI and the raw OpenAPI spec. |

## OpenAPI / Swagger

The OpenAPI spec is generated from the annotations on the handlers (the source
of truth), rather than maintained by hand:

```bash
make openapi
```

The Swagger UI is wired into the router and served at `/swagger/index.html`,
with the raw spec at `/swagger/doc.json`.

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
stripped) in a `golang` stage and copies it into a
`gcr.io/distroless/static-debian12:nonroot` runtime stage — no shell, no package
manager, runs as a non-root user (uid `65532`). The result is a small,
low-attack-surface image. The build context is the repo root (where the Go
module lives), selected with `-f`:

```bash
make docker-build            # -f deploy/build/Dockerfile.api       -> gpu-telemetry-api:0.1.0
make docker-build-collector  # -f deploy/build/Dockerfile.collector -> gpu-telemetry-collector:0.1.0
```

The Dockerfile paths are overridable (`API_DOCKERFILE`, `COLLECTOR_DOCKERFILE`),
as are the image name and tag (`IMAGE`, `COLLECTOR_IMAGE`, `TAG`).

## Deploy to minikube (Helm)

Prerequisites: `docker`, `minikube`, `kubectl`, and `helm`. If you don't have
them, `make setup-infra` installs them (Ubuntu/Debian). Then bring up a cluster
**with the Calico CNI** so the chart's NetworkPolicies are actually enforced
(the default CNI ignores them):

```bash
make start-minikube                              # docker driver + Calico
# running minikube as root? add: MINIKUBE_EXTRA_ARGS=--force
```

Deploy everything with one target — it builds the image, loads it into the
cluster, creates the hardened namespace, lints the chart, and installs the
release:

```bash
make deploy
```

This runs, in order:

1. `make minikube-load` — `docker build` then `minikube image load` (the image
   is local-only, so the chart uses `imagePullPolicy: IfNotPresent`).
2. `make namespace` — applies [`deploy/namespace.yaml`](deploy/namespace.yaml),
   a dedicated `gpu-telemetry` namespace labelled for the **restricted** Pod
   Security Standard. This is created up-front because Helm writes its release
   secret into the target namespace before applying manifests.
3. `helm upgrade --install … --wait` — installs the
   [chart](deploy/helm/gpu-telemetry-api/) into that namespace.

Check what's running:

```bash
make status        # deploy/pods/svc/networkpolicy/sa + namespace labels
```

Tear it down with `make undeploy`.

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
- **NetworkPolicies** (enforced by Calico): default-deny all ingress/egress,
  then allow only DNS egress, in-namespace HTTP, and — because the service is
  exposed — external ingress to the API port.

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
from WSL. Swagger's "Try it out" calls the same origin that served the page, so
no extra configuration is needed.

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
| `make helm-lint` / `make helm-template` | Lint / render the chart. |
| `make deploy` | Full API pipeline: build → load → namespace → helm install. |
| `make docker-build-collector` | Build the collector image `gpu-telemetry-collector:0.1.0`. |
| `make deploy-collector` | Build → load → install the collector chart (Deployment + HPA). |
| `make undeploy-collector` | Uninstall the collector release. |
| `make status` | Show workloads + security objects in the namespace. |
| `make service-url` / `make expose` | Print URL / open a tunnel for host access. |
| `make undeploy` | Uninstall the release and delete the namespace. |
| `make openapi` | Regenerate the OpenAPI spec from handler annotations. |
| `make test` | Run all unit tests with the race detector. |
| `make cover` | Run tests and print total statement coverage. |
| `make cover-html` | Generate a browsable `coverage.html` report. |

## Roadmap

Done:

- ✅ **Containerisation and Kubernetes (minikube) deployment** via Helm, with a
  security-hardened, dedicated namespace.
- ✅ **Telemetry Collector** — scalable competing-consumer that parses and
  persists telemetry, with its own image, Helm chart, and HPA.
- ✅ **Persistence layer** — PostgreSQL/TimescaleDB behind a `TelemetryStore`
  interface.
- ✅ **Unit tests + coverage** via the Makefile (`make cover`).

The following are part of the project's goal and will be added over time:

- A **custom message queue** (competing-consumers work queue) replacing Kafka.
  The Collector already targets a `queue.Consumer` interface, so this is a
  drop-in implementation rather than a rewrite.
- A **Streamer** service that replays the CSV onto the queue.
- Implement the API handlers to read from TimescaleDB (they currently return
  `501 Not Implemented`).

## AI assistance

This repository's initial structure was scaffolded with Claude Code. A detailed
account of the prompts used and where AI needed manual intervention is in
[project_docs/AI_PROMPTS.md](project_docs/AI_PROMPTS.md).
