# Project Setup

These are the steps used to bootstrap the project from scratch. They cover
initialising the Go module, adding the web framework and API documentation, and
creating the first routes so the service can be deployed to minikube for an
early end-to-end check — then standing up the full data pipeline (Streamer $\rightarrow$
Queue $\rightarrow$ Collector $\rightarrow$ TimescaleDB $\rightarrow$ API).

> For the full build, test, and deployment workflow, see the [README](README.md).

## 1. Initialise the Go module

1. Confirm the Go version.
2. Initialise the module: `go mod init`.
3. Tidy the dependencies: `go mod tidy`.

## 2. Add the web framework

- Add the Gin framework for the HTTP layer (`go get`).

## 3. Set up API documentation

- Set up the Swagger / OpenAPI documentation.
- Generate the router.

## 4. First deployment check

- Set up the routes — initially just enough to verify the deployment on
  minikube.

## 5. Containerise the API

- Add a multi-stage Dockerfile under [`deploy/build/`](deploy/build/): build a
  static binary in a Go stage, then copy it into a minimal
  `distroless/static:nonroot` runtime stage.
- Add a [`.dockerignore`](.dockerignore) to keep the build context small.
- Build the image: `make docker-build`.

## 6. Provision the cluster

- Install the tooling if needed: `make setup-infra` (Docker, minikube, kubectl,
  Helm).
- Start minikube with the Calico CNI so NetworkPolicies are enforced:
  `make start-minikube` (add `MINIKUBE_EXTRA_ARGS=--force` if running as root).

## 7. Set up security in a dedicated namespace

- Create the hardened `gpu-telemetry` namespace first
  ([`deploy/namespace.yaml`](deploy/namespace.yaml)), labelled for the
  **restricted** Pod Security Standard: `make namespace`.

## 8. Deploy with Helm

- Install the chart in [`deploy/helm/gpu-telemetry-api`](deploy/helm/gpu-telemetry-api)
  into that namespace. The full pipeline (build $\rightarrow$ load image $\rightarrow$ namespace $\rightarrow$
  install) is a single target: `make deploy`.
- Verify: `make status`, then `curl http://$(minikube ip):30080/healthz`.

## 9. Access from the host

- Expose the NodePort service to the host using minikube's native tunnel
  (no `kubectl port-forward`), then reach it from the
  Windows host at `http://localhost:<port>`.

## 10. Build the data pipeline (Queue, Streamer + Collector)

- Define the shared on-the-wire `telemetry.Record`, a technology-agnostic
  `queue` package (`Consumer` for the Collector, `Producer` for the Streamer).
- **Custom gRPC Queue** (`cmd/queue`, `internal/queue/grpc`): hand-written gRPC
  bindings (no protoc), in-memory broker with per-consumer-group offset tracking,
  deployed as a standalone service via Helm (`deploy/helm/gpu-telemetry-queue`).
- **Kafka implementation** (`internal/queue/kafka`): Kafka-backed alternative kept
  for comparison and the Compose stack; selected via `QUEUE_TYPE=kafka`.
- **Collector** ([`internal/collector`](internal/collector/)): consume → batch →
  persist → commit, at-least-once delivery with idempotent inserts. Its own image
  and Helm chart with an HPA.
- **Streamer** ([`internal/streamer`](internal/streamer/)): replay the CSV onto
  the queue, stamping each datapoint with its processing time, looping to simulate
  a continuous stream. Its own image and Helm chart with an HPA. The CSV is mounted
  at runtime from a PersistentVolume.

## 11. Harden the queue (fault tolerance, streaming, security)

A series of improvements made the queue production-ready after the MVP:

- **Server-streaming `StreamConsume`** — replaced the unary `Consume` RPC with a
  server-streaming RPC so the broker pushes batches of up to 500 messages per
  frame. Eliminates the per-message round-trip that capped unary gRPC at ~1k msg/s;
  batch streaming sustains ~500k msg/s. Hand-written stream descriptors added to
  `internal/queue/grpc/api.go`; client updated with an internal batch buffer so the
  `Fetch` interface is unchanged.
- **Bounded buffer with eviction** — `Topic` now enforces a `maxMessages` cap
  (default 10 000, env `QUEUE_MAX_MESSAGES`). When exceeded, the oldest quarter is
  evicted and `baseOffset` advances; lagging consumers are silently reset to the new
  base rather than erroring. Prevents unbounded memory growth.
- **Notify channel (close + replace)** — replaced `sync.Cond` with a
  `notify chan struct{}` closed on every `Produce` and replaced immediately. Lets
  `ConsumeBatch` use a native `select { case <-notifyCh; case <-ctx.Done() }`
  loop, eliminating one goroutine spawned per consumer call that the `sync.Cond`
  workaround required.
- **Consumer reconnection** — `recvBatch()` retries on `codes.Unavailable` /
  `io.EOF` with exponential backoff (100ms → 10s). `waitForReady: true` in the gRPC
  service config queues RPCs during the broker's restart window. The collector never
  sees the reconnection.
- **Producer retry** — `Publish` retries transient errors up to 5 times with
  backoff before surfacing an error.
- **Graceful shutdown** — `grpc.Server.GracefulStop()` on `SIGTERM`;
  `terminationGracePeriodSeconds: 15` in the Helm chart.
- **HTTP health endpoint** — broker serves `/healthz`, `/readyz`, `/stats` on
  `:8083` (`QUEUE_HEALTH_ADDR`); Helm chart wires liveness, readiness, and startup
  probes.
- **TLS** — `GRPC_TLS_CERT_FILE` / `GRPC_TLS_KEY_FILE` on the server,
  `GRPC_TLS_CA_FILE` on clients. `make gen-tls-certs` generates a self-signed CA
  and server cert, stored as Kubernetes Secrets.

## 12. Streamer CSV checkpoint

- Added a `checkpointer` type in `internal/streamer/streamer.go` that writes the
  current record index to a plain file. `Run()` loads the saved index on start,
  saves every 500 records, and saves on clean shutdown (SIGTERM).
- The checkpoint file lives on an `emptyDir` volume (`/checkpoint/progress`).
  `emptyDir` survives container restarts within the same pod, so a crash-loop
  resumes mid-dataset. A full pod eviction resets to 0 — correct, since the new pod
  has no prior state.
- Configured via `STREAMER_CHECKPOINT_DIR` (empty = disabled). The Helm chart
  sets `/checkpoint` and mounts the `emptyDir` volume automatically.

## 14. Wire the API to the store

- Implement the API handlers against `store.TelemetryReader`
  (`GET /api/v1/gpus`, `GET /api/v1/gpus/{id}/telemetry`), add a `/readyz`
  datastore-readiness probe, and connect the API to TimescaleDB via
  `POSTGRES_DSN`. Regenerate the OpenAPI spec: `make openapi`.

## 15. Run the whole pipeline

### Local (Docker Compose — always Kafka)

```bash
docker compose -f deploy/docker-compose.yaml up --build
```

Brings up Kafka + TimescaleDB + Streamer + Collector + API. Query at
`http://localhost:8080/api/v1/gpus`. Scale with:

```bash
docker compose -f deploy/docker-compose.yaml up --build --scale streamer=3 --scale collector=3
```

### minikube — one command

**Custom gRPC queue (recommended):**
```bash
make deploy QUEUE_TYPE=grpc
```

**Kafka:**
```bash
make deploy-kafka && make deploy QUEUE_TYPE=kafka
```

### minikube — step by step

Both modes share the same first two steps:

```bash
make setup-infra       # install Docker, minikube, kubectl, Helm (Ubuntu/Debian; skip if already installed)
make start-minikube    # start cluster with Calico CNI (required for NetworkPolicy enforcement)
```

**Custom gRPC queue:**

| Step | Command | What it does |
|---|---|---|
| 1 | `make deploy-timescaledb` | Installs bitnami/postgresql + db-init Job (creates schema, enables TimescaleDB extension) |
| 2 | `make deploy-queue` | Builds gRPC queue image, loads into minikube, installs Helm chart |
| 3 | `make deploy-collector QUEUE_TYPE=grpc` | Builds collector image, loads into minikube, installs chart + HPA |
| 4 | `make deploy-streamer QUEUE_TYPE=grpc` | Loads CSV onto PVC, builds streamer image, loads into minikube, installs chart + HPA |
| 5 | `make deploy-api` | Builds API image, loads into minikube, installs Helm chart |

**Kafka:**

| Step | Command | What it does |
|---|---|---|
| 1 | `make deploy-timescaledb` | Same as above |
| 2 | `make deploy-kafka` | Deploys Kafka as a KRaft StatefulSet (no Zookeeper) |
| 3 | `make deploy-collector` | Builds collector image, loads into minikube, installs chart + HPA (`QUEUE_TYPE=kafka` is the default) |
| 4 | `make deploy-streamer` | Loads CSV onto PVC, builds streamer image, loads into minikube, installs chart + HPA |
| 5 | `make deploy-api` | Builds API image, loads into minikube, installs Helm chart |

After deploying:

```bash
make status       # show all pods, services, networkpolicies in the namespace
make service-url  # print the URL to reach the API (keep this process running for Windows access)
```

## 16. Switching between Kafka and the custom gRPC queue

The queue implementation is selected at deploy time via `QUEUE_TYPE`. Only the
selected broker's env vars are used; the other's are ignored.

| `QUEUE_TYPE` | Queue pod needed | Kafka pod needed | Key env vars |
|---|---|---|---|
| `grpc` | yes (`make deploy-queue`) | no | `QUEUE_ADDR=gpu-telemetry-queue:50051` |
| `kafka` | no | yes (`make deploy-kafka`) | `KAFKA_BROKERS=kafka:9092` |
