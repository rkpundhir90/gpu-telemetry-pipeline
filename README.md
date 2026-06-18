# Elastic GPU Telemetry Pipeline with a Custom Message Queue

An elastic, horizontally-scalable telemetry pipeline for an AI/GPU cluster. The
goal is to stream GPU telemetry (DCGM exporter CSV) through a **custom message
queue** (no Kafka/RabbitMQ/etc.), persist it, and expose it over a REST API with
an auto-generated OpenAPI spec.

> **Status — early stage.** This repository currently contains the REST API
> layer (Gin), the Swagger/OpenAPI wiring, and the reference data. The broader
> pipeline (message queue, collectors, database, and Kubernetes packaging) is
> planned and will be added over time. This README describes **what exists
> today** and will grow as the project does.

## Table of contents
- [What's in the repo today](#whats-in-the-repo-today)
- [Tech stack (today)](#tech-stack-today)
- [REST API](#rest-api)
- [OpenAPI / Swagger](#openapi--swagger)
- [Reference data](#reference-data)
- [Build](#build)
- [Roadmap](#roadmap)
- [AI assistance](#ai-assistance)

## What's in the repo today

```
internal/api/         REST API layer (Gin)
  handler.go            request handlers + OpenAPI annotations
  router.go             routes, structured logging, Swagger UI route
Makefile              `make openapi` to (re)generate the OpenAPI spec
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

## Roadmap

The following are part of the project's goal and will be added over time:

- A **custom message queue** (competing-consumers work queue) — no Kafka/RabbitMQ.
- **Streamer** and **collector** services that replay the CSV and persist telemetry.
- A **persistence layer** (database) behind the API.
- **Containerisation and Kubernetes (minikube) deployment** via Helm.
- Unit tests and coverage gating.

## AI assistance

This repository's initial structure was scaffolded with Claude Code. A detailed
account of the prompts used and where AI needed manual intervention is in
[project_docs/AI_PROMPTS.md](project_docs/AI_PROMPTS.md).
