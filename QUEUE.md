# Go gRPC Message Queue

A custom, lightweight, high-throughput message queue built in Go to replace Kafka. All communication uses gRPC with hand-written bindings (no protoc dependency). The broker is purely in-memory with a bounded buffer and server-streaming delivery.

## Table of Contents

- [Context & Motivation](#context--motivation)
- [Architecture Overview](#architecture-overview)
- [gRPC API](#grpc-api)
- [Fault Tolerance & Reliability](#fault-tolerance--reliability)
- [Streamer CSV Checkpoint](#streamer-csv-checkpoint)
- [Phased Implementation Plan](#phased-implementation-plan)
- [Getting Started](#getting-started)

## Context & Motivation

We replaced Kafka with a purpose-built in-memory queue to eliminate the operational overhead of ZooKeeper/KRaft, reduce round-trip latency, and own the full delivery pipeline. The key architectural bet: for this pipeline's workload (single-topic, bounded consumer groups, hot-path GPU telemetry), a well-implemented in-memory broker outperforms Kafka on every metric that matters — latency, throughput, and operational simplicity.

**Why not NATS?** NATS JetStream adds a separate persistence layer and a richer but heavier API surface. Our broker is ~400 lines of Go with no external dependencies and is auditable end-to-end.

## Architecture Overview

```
cmd/queue/main.go          — broker process, :50051 gRPC + :8083 health HTTP
internal/queue/server/     — Broker, Topic, bounded buffer, ConsumeBatch
internal/queue/grpc/       — hand-written gRPC bindings (api.go, client.go)
internal/queue/grpc/codec.go — message codec
```

**Data path:**

```
Streamer (producer)
  └─► grpc.Produce(batch)
        └─► Topic.Produce() → append to ring buffer
              └─► cond.Broadcast() → wake waiting consumers
                    └─► Topic.ConsumeBatch() → server-streaming push
                          └─► Collector (consumer) internal batch buffer
                                └─► queue.Consumer.Fetch() → one message at a time
```

## gRPC API

The service definition lives in `internal/queue/grpc/api.go` — hand-written, no protoc required. The proto schema at `proto/queue.proto` is kept for reference.

### RPCs

| RPC | Type | Description |
|-----|------|-------------|
| `Produce` | Unary | Append a batch of messages to a topic. Returns assigned offsets. |
| `StreamConsume` | Server-streaming | Open a long-lived stream; broker pushes batches as they arrive. Replaces the old unary `Consume` RPC. |
| `Commit` | Unary | Acknowledge the last processed offset for a consumer group. |

### Why server-streaming instead of unary Consume

The original design used a unary `Consume` RPC — one request per message. With gRPC's per-RPC overhead this caps throughput at roughly **1k msg/s**.

`StreamConsume` opens a single long-lived stream. The broker calls `stream.Send()` with batches of up to 500 messages per frame as soon as they arrive. A consumer maintaining an internal batch buffer can drain frames immediately without a per-message round-trip.

Measured improvement: **~500k msg/s** (batch streaming) vs **~1k msg/s** (unary).

### Message types

```go
// Client → broker: open a streaming session
type ConsumeStreamRequest struct {
    Topic        string
    GroupId      string
    MaxBatchSize int32  // broker clips to its own limit if larger
}

// Broker → client: one frame per ConsumeBatch result
type ConsumeStreamResponse struct {
    Messages []*BatchedMessage
}

type BatchedMessage struct {
    Key    []byte
    Value  []byte
    Offset int64  // logical offset within the topic
}
```

## Fault Tolerance & Reliability

### Bounded in-memory buffer

`Topic` holds at most `maxMessages` messages (default: 10 000, configurable via `QUEUE_MAX_MESSAGES`). When the limit is exceeded, the oldest quarter is evicted and `baseOffset` advances. Consumers whose delivered offset fell below `baseOffset` are silently reset to the new base — they miss the evicted messages but stay connected and continue rather than crashing.

This gives a clear backpressure model: producers that outpace consumers by more than `maxMessages` cause data loss by design (preferable to OOM). The `/stats` endpoint exposes `base_offset` and `head_offset` so the gap is observable.

```
baseOffset         HEAD
    │                │
    ▼                ▼
[msg₁₀₀₀ … msg₁₂₄₉]   ← only these 250 messages are buffered
```

### Graceful shutdown

The broker runs `grpc.Server.GracefulStop()` on `SIGTERM`, which lets in-flight RPCs complete before the listener closes. `terminationGracePeriodSeconds: 15` in the Helm chart gives enough time for consumers to drain their batch buffers and producers to finish their current retries.

### Consumer reconnection (broker restart survival)

When the broker pod restarts, all open `StreamConsume` streams break. The client-side `recvBatch()` loop detects this (`codes.Unavailable` or `io.EOF`) and reopens the stream with **exponential backoff** (100ms → 10s). The caller (`Fetch`) never sees the reconnection — it just blocks briefly.

`waitForReady: true` in the gRPC service config causes new RPCs to queue in the channel rather than fail immediately during the broker's restart window.

```go
const grpcServiceConfig = `{
  "methodConfig": [{
    "name": [{"service": "queue.QueueService"}],
    "waitForReady": true
  }]
}`
```

### Producer retry

`Publish` retries transient errors (`codes.Unavailable`, `io.EOF`) up to 5 times with exponential backoff before returning an error. Combined with `waitForReady`, the streamer survives a broker restart without dropping records or exiting.

### Health endpoint

The broker serves HTTP on `:8083` (configurable via `QUEUE_HEALTH_ADDR`):

| Path | Description |
|------|-------------|
| `GET /healthz` | Liveness — returns 200 if the process is up. |
| `GET /readyz`  | Readiness — returns 200 if the broker is ready to accept connections. |
| `GET /stats`   | JSON snapshot: per-topic `buffered`, `base_offset`, `head_offset`, `committed_offsets`, and broker-wide `max_messages`. |

The Helm chart wires liveness, readiness, and startup probes to `/healthz` on this port.

### TLS (minikube)

`GRPC_TLS_CERT_FILE` + `GRPC_TLS_KEY_FILE` on the server and `GRPC_TLS_CA_FILE` on clients enable mTLS. When the env vars are absent the server and clients both default to insecure transport (local dev).

Certs are generated by `make gen-tls-certs` (self-signed CA + server cert) and stored in two Kubernetes Secrets:
- `gpu-telemetry-queue-tls` — server cert + key (mounted into the broker pod)
- `gpu-telemetry-queue-ca` — CA cert (mounted into streamer and collector pods)

## Streamer CSV Checkpoint

The streamer publishes records from a CSV in a loop. Without a checkpoint, a pod restart replays from record 0, duplicating already-published data.

### How it works

`internal/streamer/streamer.go` contains a `checkpointer` type that reads and writes a plain integer (the next record index) to a file. The `Run()` loop:

1. **On start** — loads the saved index; if missing, corrupt, or out-of-bounds for the current dataset, starts from 0.
2. **Every 500 records** — saves `i+1` so restart overhead is bounded to at most 500 records of replay.
3. **On context cancellation (SIGTERM)** — saves the current index before returning, giving near-zero replay on a clean shutdown.
4. **After each complete pass** — saves 0, so the next pass starts from the beginning.

The checkpoint file lives on an `emptyDir` volume (`/checkpoint/progress`). `emptyDir` survives **container restarts** within the same pod, satisfying the requirement. A full pod eviction (OOM kill, node drain) resets to 0 — which is correct, since a new pod on a new node has no meaningful prior state.

### Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `STREAMER_CHECKPOINT_DIR` | `""` (disabled) | Directory for the checkpoint file. Empty = no checkpointing. |

The Helm chart sets `STREAMER_CHECKPOINT_DIR=/checkpoint` via `values.yaml → streamer.checkpointDir`.

## Phased Implementation Plan

### Stage 1: In-Memory Broker ✅ Complete

- Pure in-memory topic storage with `sync.RWMutex` + `sync.Cond`
- `Produce` / `StreamConsume` (server-streaming) / `Commit` RPCs
- Bounded buffer with eviction and `baseOffset` tracking
- Consumer reconnection with exponential backoff
- Producer retry with backoff
- HTTP health endpoint (`/healthz`, `/readyz`, `/stats`)
- Graceful shutdown on SIGTERM
- TLS support (self-signed CA for minikube)
- Streamer CSV checkpoint (emptyDir, survives container restarts)
- **Status:** Running end-to-end in minikube. `publish_errors: 0`, `total_dropped: 0`.

### Stage 2: Shared Persistence & Smart Flushing

- Durability without local state.
- Background worker flushes in-memory buffers to shared storage (S3-compatible / EFS).
- Flush triggered by: size threshold (5 MB), time threshold (200ms), or memory pressure.
- Queue Mode: messages acknowledged before the flush window are evicted and never written to disk.

### Stage 3: Consumer Groups & State

- Multiple microservices reading cooperatively from the same topic.
- Internal `__consumer_offsets` topic for durable offset tracking across pod restarts.

### Stage 4: High Availability & Clustering

- Horizontal scaling across 10+ broker instances.
- In-memory peer replication (memory quorum): leader forwards batch to follower before ACKing producer.
- Lightweight Metadata Raft cluster for partition leadership.

## Getting Started

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `QUEUE_LISTEN_ADDR` | `:50051` | gRPC listen address |
| `QUEUE_HEALTH_ADDR` | `:8083` | HTTP health/stats listen address |
| `QUEUE_MAX_MESSAGES` | `10000` | Max messages buffered per topic before eviction |
| `GRPC_TLS_CERT_FILE` | `""` | Path to server TLS certificate (PEM) |
| `GRPC_TLS_KEY_FILE` | `""` | Path to server TLS private key (PEM) |
| `GRPC_TLS_CA_FILE` | `""` | Path to CA cert used by clients to verify the server (PEM) |

### Local development

```bash
# Run the broker (in-memory, no TLS)
QUEUE_LISTEN_ADDR=:50051 QUEUE_HEALTH_ADDR=:8083 go run ./cmd/queue/

# Streamer (separate terminal) — checkpoint written to /tmp/cp
QUEUE_TYPE=grpc QUEUE_ADDR=localhost:50051 \
  STREAMER_CSV_PATH=path/to/dcgm_metrics.csv \
  STREAMER_CHECKPOINT_DIR=/tmp/cp \
  go run ./cmd/streamer/

# Collector (separate terminal)
QUEUE_TYPE=grpc QUEUE_ADDR=localhost:50051 go run ./cmd/collector/

# Check broker stats
curl -s localhost:8083/stats | jq
```

### Kubernetes deploy

```bash
# Build and load all images into minikube
make docker-build-queue docker-build-collector docker-build-streamer
make minikube-load-queue minikube-load-collector minikube-load-streamer

# Deploy full pipeline (queue mode)
make deploy QUEUE_TYPE=grpc

# Or step by step
make deploy-queue          # generates TLS certs, deploys broker
make deploy-collector QUEUE_TYPE=grpc
make deploy-streamer  QUEUE_TYPE=grpc

# Force pods to pick up a rebuilt image with the same tag
kubectl rollout restart deployment/gpu-telemetry-queue -n gpu-telemetry
```

The Makefile passes `QUEUE_TYPE` and `QUEUE_ADDR` via `--set` to each Helm chart. NetworkPolicies open egress port 50051 for gRPC or 9092 for Kafka based on `queue.type`.

### Verifying end-to-end

```bash
# Tail broker stats during a run
watch -n2 'curl -s $(minikube service gpu-telemetry-queue --url -n gpu-telemetry --port health)/stats | jq'

# Check streamer checkpoint (shows current record index)
kubectl exec -n gpu-telemetry deploy/gpu-telemetry-streamer -- cat /checkpoint/progress

# Confirm zero publish errors
kubectl logs -n gpu-telemetry deploy/gpu-telemetry-streamer | grep publish_errors
```
