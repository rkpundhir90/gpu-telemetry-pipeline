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
- **Custom gRPC Queue**: Implement the stateless in-memory broker (`cmd/queue`) and
  corresponding gRPC client (`internal/queue/grpc`). Deploy as a standalone
  service via Helm (`deploy/helm/gpu-telemetry-queue`).
- **Kafka Implementation**: Provide a Kafka-backed implementation as a reference
  and interim solution (`internal/queue/kafka`).
- **Collector** ([`internal/collector`](internal/collector/)): consume $\rightarrow$ batch $\rightarrow$
  persist $\rightarrow$ commit, with at-least-once delivery and idempotent inserts. Its own
  image ([`Dockerfile.collector`](deploy/build/Dockerfile.collector)) and Helm
  chart with an HPA so it scales independently.
- **Streamer** ([`internal/streamer`](internal/streamer/)): replay the CSV onto
  the queue, stamping each datapoint with its processing time, looping to
  simulate a continuous stream. Its own image
  ([`Dockerfile.streamer`](deploy/build/Dockerfile.streamer)) and Helm chart with
  an HPA. The CSV is mounted at runtime from a PersistentVolume.

## 11. Wire the API to the store

- Implement the API handlers against `store.TelemetryReader`
  (`GET /api/v1/gpus`, `GET /api/v1/gpus/{id}/telemetry`), add a `/readyz`
  datastore-readiness probe, and connect the API to TimescaleDB via
  `POSTGRES_DSN`. Regenerate the OpenAPI spec: `make openapi`.

## 12. Run the whole pipeline

- **Local (Compose):** `docker compose -f deploy/docker-compose.yaml up --build`
  brings up Kafka + TimescaleDB + Streamer + Collector + API; query it at
  `http://localhost:8080/api/v1/gpus`. Scale with
  `--scale streamer=3 --scale collector=3`.
- **minikube:** a single `make deploy` orchestrates the full sequence:
  `deploy-timescaledb` $\rightarrow$ `deploy-queue` (Custom gRPC) $\rightarrow$ `deploy-collector` $\rightarrow$ `deploy-streamer` $\rightarrow$ `deploy-api`.

## 13. Switching between Kafka and the custom gRPC queue

The queue implementation is selected at deploy time via `QUEUE_TYPE`. Only the
relevant env vars are used; the other broker's settings are ignored.

| `QUEUE_TYPE` | Queue pod needed | Kafka needed | Key env vars |
|---|---|---|---|
| `grpc` (default in `make deploy`) | yes (`deploy-queue`) | no | `QUEUE_ADDR=gpu-telemetry-queue:50051` |
| `kafka` | no | yes (`deploy-kafka`) | `KAFKA_BROKERS=kafka:9092` |

**Run against the custom gRPC queue (minikube):**
```bash
make deploy                          # QUEUE_TYPE=grpc is the Makefile default
# or explicitly:
make deploy QUEUE_TYPE=grpc
```

**Run against Kafka (minikube):**
```bash
make deploy-timescaledb
make deploy-kafka                    # raw StatefulSet, not a Helm chart
make deploy-collector                # QUEUE_TYPE=kafka is the env default
make deploy-streamer
make deploy-api
# or all at once:
make deploy QUEUE_TYPE=kafka
```

**Run against Kafka locally (Compose — always Kafka):**
```bash
docker compose -f deploy/docker-compose.yaml up --build
```
The Compose stack wires Kafka directly; `QUEUE_TYPE` is not needed.
