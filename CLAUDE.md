# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
go build ./...

# Test (all packages, race detector)
make test

# Single test
go test -race -run TestName ./internal/collector/

# Coverage
make cover          # prints COVERAGE_TOTAL line (CI-greppable)
make cover-html     # writes coverage.html

# Regenerate OpenAPI spec (docs/ is generated — do not edit by hand)
make openapi

# Full pipeline deploy to minikube (TimescaleDB → Kafka → Collector → Streamer → API)
make deploy

# Individual service deploys
make deploy-timescaledb
make deploy-kafka
make deploy-collector
make deploy-streamer
make deploy-api

# After rebuilding an image with the same TAG, force pods to pick up the new image
kubectl rollout restart deployment/<name> -n gpu-telemetry

# Inspect running state
make status         # deploy/pods/svc/networkpolicy/sa + namespace labels
make service-url    # print minikube tunnel URL (WSL → Windows access)

# Tear down
make undeploy       # API release + namespace
make undeploy-all   # all releases + namespace
```

## Architecture

### Data flow

```
CSV (PVC)
  └─► Streamer (cmd/streamer)
        └─► Kafka topic "gpu-telemetry"   (keyed by GPU UUID → one partition per GPU)
              └─► Collector (cmd/collector)
                    └─► TimescaleDB hypertable "gpu_telemetry"
                          └─► REST API (cmd/api)  GET /api/v1/gpus, /api/v1/gpus/{id}/telemetry
```

### Interface boundaries (the key abstraction layer)

The project is deliberately built around two pairs of interfaces so the underlying Kafka and TimescaleDB implementations are swappable:

- **`internal/queue`** — `Producer` (Streamer writes) and `Consumer` (Collector reads). Kafka lives in `internal/queue/kafka/`. The plan is to replace Kafka with a custom queue by adding a new `Producer`/`Consumer` implementation here — the Streamer and Collector engines do not change.
- **`internal/store`** — `TelemetryStore` (write, used by Collector) and `TelemetryReader` (read, used by API). These are separate interfaces; the Collector never sees read methods. TimescaleDB lives in `internal/store/postgres/`.

### Shared contract

`internal/telemetry.Record` is the single on-the-wire type that flows through the whole pipeline. The Streamer marshals it to JSON, Kafka carries it, the Collector unmarshals and stores it, and the API returns it. **The `Timestamp` field is stamped by the Streamer at publish time** — the original CSV timestamp column is discarded. `Record` still carries BSON tags (a leftover from an earlier MongoDB store) that are harmless but should not be treated as meaningful.

### API layering

```
Handler (internal/api/handler.go)   — HTTP decode/encode, status codes, no business logic
  └─► Service (internal/api/service/)  — validation, defaulting, orchestration
        └─► TelemetryReader            — store interface (read only)
```

Handlers depend on `service.TelemetryService`; they never touch the store directly. The OpenAPI annotations live on the handler methods; `make openapi` regenerates `docs/`.

### Configuration

All three services are configured purely through environment variables via `internal/config/`. Each binary calls the appropriate `config.APIConfig()`, `config.CollectorConfig()`, or `config.StreamerConfig()`. Defaults work for local dev (Kafka on `localhost:9092`, TimescaleDB on `localhost:5432`).

### Kubernetes / deploy specifics

- **Calico CNI is required** — minikube must start with `--cni=calico` (done by `make start-minikube`). Without it, the Kubernetes NetworkPolicies in the Helm charts are silently ignored.
- **Namespace default-deny** — the API chart deploys a `podSelector: {}` default-deny NetworkPolicy that applies to *all* pods in the namespace, including Kafka. Every component (including Kafka itself, via `kafka-allow` in `deploy/helm/kafka/kafka-statefulset.yaml`) needs an explicit allow policy or Calico drops its traffic silently.
- **Kafka is not a Helm chart** — it is a raw StatefulSet applied by `deploy/helm/kafka/install.sh`. The `kafka-allow` NetworkPolicy and `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092` (ClusterIP, not the headless pod FQDN) are both in that YAML.
- **Collector owns schema creation** — `EnsureSchema` runs at Collector startup, creating the `gpu_telemetry` hypertable if absent. The API does not call `EnsureSchema`; a fresh cluster needs the Collector to run first before the API has data.
- **Streamer CSV is on a PVC** — `make load-streamer-data` copies the CSV onto the volume. The Streamer chart has an init container that blocks pod startup until the file exists, so deploying before loading data is safe (pods just wait).
- **Image tag pinning** — all images use `imagePullPolicy: IfNotPresent` with tag `0.1.0`. Rebuilding an image without changing the tag does not cause pods to restart. After `make docker-build-*` + `minikube image load`, run `kubectl rollout restart deployment/<name> -n gpu-telemetry` manually.
- **Idempotent inserts** — the store uses `INSERT … ON CONFLICT (uuid, metric_name, time) DO NOTHING`, so the Collector can re-process a batch after a crash without creating duplicates (at-least-once delivery with idempotent effect).

### Container images

All three images use multi-stage builds (`deploy/build/Dockerfile.*`) producing a `distroless/static:nonroot` runtime — no shell, no package manager. There is no `kubectl exec` shell available for debugging running containers; use logs and the `/stats` health endpoint instead.
