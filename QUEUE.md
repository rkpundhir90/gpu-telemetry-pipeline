# Go gRPC Message Queue 🚀

A custom, lightweight, and blazing-fast Message Queue built in Go to serve as a stateless alternative to Kafka. Communication strictly uses gRPC for efficient binary serialization and multiplexing.

## 📖 Table of Contents

- [Context & Motivation](#context--motivation)
- [Phased Implementation Plan](#phased-implementation-plan)
- [Storage Architecture](#storage-architecture-stateless-brokers--shared-persistence)
- [gRPC API Design](#grpc-api-design)
- [Go-Specific Optimizations](#go-specific-optimizations-high-throughput)
- [Getting Started](#getting-started)

## 🎯 Context & Motivation

We are building a custom, lightweight Message Queue in Go to replace Kafka. Communication will strictly use gRPC for efficient binary serialization and multiplexing.

**Architectural Pivot:** Instead of relying on local disk storage (which makes brokers stateful and hard to scale), we use a **Stateless Broker Architecture backed by Shared Persistence** (e.g., S3-compatible Object Storage, EFS, or a shared SAN). Brokers act as highly-optimized memory buffers and network routers.

## 🗺️ Phased Implementation Plan

We embrace an iterative approach, building a simple, working service first, and scaling its complexity progressively.

### Stage 1: The MVP (Pure In-Memory Broker) ✅ Complete

* **Goal:** Establish the gRPC API contract and network layer before introducing I/O complexities.
* **Storage:** Pure in-memory maps and slices protected by Read/Write Mutexes. No disk I/O.
* **Features:** `Produce` (append to topic), `Consume` (blocking read by absolute offset), `Commit` (per-group offset acknowledgement).
* **Status:** Running end-to-end in minikube. Streamer → gRPC Queue → Collector → TimescaleDB → API fully verified (`publish_errors: 0`, `total_dropped: 0`). See `internal/queue/server/`, `internal/queue/grpc/`, `cmd/queue/`.

### Stage 2: Shared Persistence & Smart Flushing

* **Goal:** Durability without local state.
* **Storage:** Asynchronous batch writes to a Shared Storage layer.
* **Mechanics:** Instead of local `mmap`, brokers maintain the in-memory buffer from Stage 1. A background worker periodically flushes these buffers to shared storage.

### Stage 3: Consumer Groups & State

* **Goal:** Allow multiple microservices to read cooperatively.
* **Mechanics:** Add an internal topic `__consumer_offsets`. The server tracks which consumer in a group read which message.

### Stage 4: High Availability & Clustering

* **Goal:** Fault tolerance and horizontal scaling across 10+ instances.
* **Mechanics:** Implement **In-Memory Peer Replication** to prevent data loss during the flush window, coordinated by a lightweight Metadata Raft cluster.

## 💾 Storage Architecture: Stateless Brokers & Shared Persistence

By removing local disks, any node can immediately serve any partition. However, this introduces specific challenges regarding durability and flushing.

### A. The Smart Flush Algorithm

Brokers will not write to shared storage synchronously on every request. They will flush batches based on a tripartite heuristic to optimize IOPS:

1. **Size Threshold:** Flush when the partition buffer hits a specific size (e.g., 5MB).
2. **Time Threshold:** Flush every `X` milliseconds (e.g., 200ms) to ensure low-volume topics don't languish in memory.
3. **Memory Pressure:** Flush aggressively if the Go runtime detects high heap usage.

### B. The Queue vs. Log Paradigm (Handling Read Data)

The system will support two operational modes depending on the topic configuration:

* **Log Mode (Kafka-style):** All messages are flushed to shared storage eventually to allow historical replay, regardless of whether a consumer has read them.
* **Queue Mode (Ephemeral optimization):** If a consumer connects and acknowledges reading the messages *before* the Smart Flush algorithm triggers, those messages are evicted from memory and **never written to disk**. This saves massive amounts of shared storage I/O but sacrifices replayability.

### C. Crash Recovery & Fallbacks (The In-Memory Vulnerability)

**The Problem:** If Node 1 receives 10,000 messages into memory, tells the client "Success," but crashes *before* the Smart Flush writes to shared storage, data is lost.

**The Solution: In-Memory Peer Replication (Memory Quorum)**
To survive broker crashes without touching a local disk, we implement memory-level replication:

1. **Produce Request:** Client sends a batch to the Partition Leader (Node A).
2. **Peer Forward:** Node A immediately streams this payload via gRPC to a Follower (Node B's memory).
3. **Acknowledge:** Once Node B ACKs the memory receipt, Node A returns "Success" to the client.
4. **Asynchronous Flush:** Node A executes the Smart Flush to shared storage in the background.
5. **Failover:** If Node A crashes before the flush, the Metadata cluster promotes Node B to Leader. Node B already has the unflushed data in its memory and assumes responsibility for flushing it to the shared persistence.

## 🔌 gRPC API Design

The system relies on a Pull-based architecture.

* `Produce(Topic, Payload) -> Offset`: Appends data to the topic.
* `Consume(Topic, Offset) -> Payload, NextOffset`: Fetches data exactly at the requested offset.
* `Acknowledge(Topic, Offset)`: (New for Queue Mode) Marks a message as safe to evict before flushing.

## ⚡ Go-Specific Optimizations (High Throughput)

* **Zero-Allocation Data Paths (`sync.Pool`):** gRPC creates a new Goroutine for every request. We will use `sync.Pool` to recycle byte slices (`[]byte`) to prevent massive Garbage Collection (GC) spikes.
* **Channel-Based Batching:** Incoming gRPC streams are fed into Go channels, allowing a single Goroutine to manage the peer-replication and shared-storage flushing without lock contention.
* **Smart Clients:** Clients cache the cluster metadata and route `Produce` requests directly to the exact Go node leading that specific partition, avoiding internal proxy hops.

## 🛠️ Getting Started

### Prerequisites

* Go 1.21+

> **Note:** The current Stage 1 implementation uses hand-written gRPC bindings in
> `internal/queue/grpc/api.go` rather than protoc-generated code. The proto schema
> lives at `proto/queue.proto` for reference and future code generation. No `protoc`
> toolchain is required to build or run the queue.

### Local Development

1. Run the queue server (in-memory, listens on `:50051` by default):
   ```bash
   QUEUE_LISTEN_ADDR=:50051 go run ./cmd/queue/
   ```

2. Point the Streamer and Collector at it via the feature flag:
   ```bash
   # Streamer
   QUEUE_TYPE=grpc QUEUE_ADDR=localhost:50051 STREAMER_CSV_PATH=... go run ./cmd/streamer/

   # Collector (separate terminal)
   QUEUE_TYPE=grpc QUEUE_ADDR=localhost:50051 go run ./cmd/collector/
   ```

3. Verify with `make build` and `make test`:
   ```bash
   go build ./...
   go test -race ./...
   ```

### Kubernetes Deploy (custom queue mode)

```bash
# 1. Build + load all images into minikube
make docker-build-queue docker-build-collector docker-build-streamer
make minikube-load-queue minikube-load-collector minikube-load-streamer

# 2. Deploy the queue, then the rest of the pipeline with QUEUE_TYPE=grpc
make deploy-queue
make deploy-collector QUEUE_TYPE=grpc
make deploy-streamer  QUEUE_TYPE=grpc

# Or deploy the full pipeline at once
make deploy QUEUE_TYPE=grpc
```

The Makefile passes `QUEUE_TYPE` and `QUEUE_ADDR` via `--set` to each Helm chart, and the
Collector/Streamer NetworkPolicies automatically open the correct egress port (9092 for
Kafka, 50051 for gRPC) based on the `queue.type` value.