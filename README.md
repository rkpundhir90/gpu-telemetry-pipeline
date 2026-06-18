# Elastic GPU Telemetry Pipeline with a Custom Message Queue

An elastic, horizontally-scalable telemetry pipeline for an AI/GPU cluster. It
streams GPU telemetry (DCGM exporter CSV) through a **custom message queue**
(no Kafka/RabbitMQ/etc.), persists it to PostgreSQL, and exposes it over a REST
API with an auto-generated OpenAPI spec. Everything is containerised and
deployed to Kubernetes (minikube) with a single Helm chart, and every workflow
is driven by `make`.

> **Status:** initial structure & layout. The codebase compiles, is unit-tested
> (`make test`, ~83% coverage over `internal/...` — `make cover-check` enforces
> ≥80%), lints as a Helm chart, and ships a generated OpenAPI spec. See
> [docs/DESIGN.md](docs/DESIGN.md) for design rationale and known limitations.

## Table of contents
- [System architecture](#system-architecture)
- [Tech stack](#tech-stack)
- [Repository layout](#repository-layout)
- [Prerequisites](#prerequisites)
- [Build & test](#build--test)
- [Run locally (no Kubernetes)](#run-locally-no-kubernetes)
- [Deploy to minikube](#deploy-to-minikube)
- [REST API](#rest-api)
- [Configuration](#configuration)
- [AI assistance](#ai-assistance)

## System architecture

Streamers replay the CSV and **publish** to the broker; collectors **consume**
and persist to Postgres; the API gateway serves the data. Streamers and
collectors scale independently (the brief caps them at 10).

```
Streamer(s) ──publish──▶  Broker (custom MQ)  ──poll/commit──▶  Collector(s) ──▶ Postgres ──▶ API Gateway ──▶ REST/OpenAPI
```

Full diagram and component table: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
Design considerations (queue semantics, scaling, availability, trade-offs):
[docs/DESIGN.md](docs/DESIGN.md).

## Tech stack

- **Language:** Go 1.25
- **HTTP/REST:** [Gin](https://github.com/gin-gonic/gin)
- **ORM / DB:** [GORM](https://gorm.io) + **PostgreSQL**
- **OpenAPI:** [swaggo/swag](https://github.com/swaggo/swag) (spec generated from handler annotations)
- **Message queue:** custom, minimal competing-consumers work queue (this repo)
- **Packaging:** Docker (distroless runtime), Helm, minikube
- **Tooling:** `make` for every workflow

## Repository layout

```
cmd/{broker,streamer,collector,apigateway}   service entrypoints
internal/
  telemetry/      domain model + DCGM CSV parser
  mq/{protocol,broker,client}   custom message queue
  store/          Repository interface + GORM/Postgres + in-memory fake
  streamer/ collector/ api/     service logic (Gin handlers in api/)
  config/ observability/        env config + structured logging
api/openapi/      generated OpenAPI (swagger.json/yaml)
deploy/docker/    one parameterised multi-stage Dockerfile
deploy/helm/      Helm chart for the full stack
docs/             ARCHITECTURE, DESIGN, AI_USAGE
```

## Prerequisites

| Tool      | Why                          | Notes |
|-----------|------------------------------|-------|
| Go 1.25+  | build/test                   | required |
| make      | command runner               | on Windows use Git Bash, WSL, or `choco install make` |
| Docker    | build images                 | required for deploy |
| minikube  | local Kubernetes             | required for deploy |
| helm 3    | install the chart            | required for deploy |
| swag      | regenerate OpenAPI           | `make swag-install` |

## Build & test

```bash
make help            # list all targets
make build           # build all four service binaries into bin/
make test            # run unit tests
make test-coverage   # run tests + print total coverage % (internal/...)
make cover-check     # fail if coverage drops below 80%
make cover-html      # write coverage.html
make check           # fmt + vet + test
make openapi         # regenerate api/openapi from handler annotations
```

## Run locally (no Kubernetes)

You need a Postgres reachable via the `DB_*` env vars (see
[Configuration](#configuration)). In separate terminals:

```bash
# 1. Broker (custom message queue)
make run-broker

# 2. Streamer (point it at the sample CSV)
MQ_BROKER_URL=http://localhost:8080 \
CSV_PATH=dcgm_metrics_20250718_134233.csv \
  make run-streamer

# 3. Collector (writes to Postgres)
MQ_BROKER_URL=http://localhost:8080 \
DB_HOST=localhost DB_USER=telemetry DB_PASSWORD=telemetry DB_NAME=telemetry \
  make run-collector

# 4. API gateway
DB_HOST=localhost DB_USER=telemetry DB_PASSWORD=telemetry DB_NAME=telemetry \
  make run-api

curl http://localhost:8081/api/v1/gpus
```

## Deploy to minikube

```bash
make minikube-start         # start the cluster
make deploy                 # build images INTO minikube + helm install
make minikube-load-data     # copy the CSV into a streamer pod's data volume
make port-forward           # forward the API to localhost:8081
```

Then:

```bash
curl http://localhost:8081/api/v1/gpus
open  http://localhost:8081/swagger/index.html   # Swagger UI
```

Scale each component independently (streamers/collectors capped at 10 per brief):

```bash
kubectl scale deploy/gtp-gpu-telemetry-pipeline-streamer  --replicas=5
kubectl scale deploy/gtp-gpu-telemetry-pipeline-collector --replicas=5

# The broker is a scalable StatefulSet of independent shards. After scaling,
# re-render config so clients learn the new shard endpoints:
helm upgrade gtp deploy/helm/gpu-telemetry-pipeline --reuse-values --set broker.replicas=3
```

Inspect queue depth / consumer lag:

```bash
make port-forward-broker
curl http://localhost:8080/stats
```

Tear down: `make helm-uninstall`.

## REST API

Base path `/api/v1`. Full spec: [api/openapi/swagger.yaml](api/openapi/swagger.yaml),
served live at `/swagger/index.html`.

| Method & path                                   | Description                                  |
|-------------------------------------------------|----------------------------------------------|
| `GET /api/v1/gpus`                              | List all GPUs that have reported telemetry.  |
| `GET /api/v1/gpus/{id}/telemetry`              | All telemetry for a GPU, ordered by time.    |
| `GET /api/v1/gpus/{id}/telemetry?start_time=…&end_time=…` | Same, filtered to an inclusive RFC3339 window. |
| `GET /healthz`                                  | Liveness/readiness probe.                    |

Example:

```bash
curl "http://localhost:8081/api/v1/gpus/GPU-5fd4f087-86f3-7a43-b711-4771313afc50/telemetry?start_time=2025-07-18T20:00:00Z&end_time=2025-07-18T21:00:00Z"
```

## Configuration

All configuration is via environment variables (12-factor; Helm injects them
through a ConfigMap/Secret). Key variables and defaults:

| Variable               | Default                  | Used by        |
|------------------------|--------------------------|----------------|
| `MQ_BROKER_URL`        | `http://localhost:8080`  | streamer, collector (comma-separated list of broker shard endpoints) |
| `MQ_TOPIC`             | `gpu.telemetry`          | streamer, collector |
| `MQ_LISTEN_ADDR`       | `:8080`                  | broker         |
| `MQ_LEASE_TTL`         | `30s`                    | broker (redelivery window for un-acked messages) |
| `MQ_MAX_DEPTH`         | `1000000`                | broker (max retained messages per topic; bounds memory) |
| `CSV_PATH`             | `/data/dcgm_metrics.csv` | streamer       |
| `STREAM_INTERVAL`      | `1s`                     | streamer       |
| `STREAM_BATCH_SIZE`    | `100`                    | streamer       |
| `STREAM_LOOP`          | `true`                   | streamer       |
| `COLLECT_BATCH_SIZE`   | `256`                    | collector      |
| `API_LISTEN_ADDR`      | `:8081`                  | api gateway    |
| `DB_HOST/PORT/USER/PASSWORD/NAME/SSLMODE` | localhost/5432/telemetry/telemetry/telemetry/disable | collector, api |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / `json`      | all            |

## AI assistance

This repository's initial structure was scaffolded with Claude Code. A detailed
account of the prompts used, what AI accelerated, and where it fell short (a real
offset-0 bug caught by the generated tests, the `swag init` correction, and the
CSV-delivery design change) is in [docs/AI_USAGE.md](docs/AI_USAGE.md).
