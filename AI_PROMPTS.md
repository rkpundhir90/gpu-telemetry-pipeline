# How AI Was Used in This Project

## Why this document exists

The project brief asks for an honest account of how AI assistance was used
across the development workflow — including the prompts that were given to the
AI, and a candid description of where AI fell short and needed manual
correction. This document provides that account in plain language for business
and non-technical readers.

## At a glance

| | |
|---|---|
| **AI tool used** | Claude Code (Anthropic) |
| **What it did** | Scaffolded the project, containerised and deployed the API to a security-hardened Kubernetes (minikube) namespace with Helm, then built out the full data pipeline — Telemetry Collector, Telemetry Streamer, and the data-backed REST API |
| **How it was used** | A series of guided prompts, with a human reviewing and correcting the output |
| **Human oversight** | Every output was reviewed; several items needed manual intervention or course-correction (see below) |

The initial structure of this project was scaffolded with **Claude Code**,
Anthropic's AI coding assistant. AI was used to bootstrap the project, wire up
the web framework and the API documentation, and produce the steps needed to
deploy the service — always with a person checking and correcting the result.

## The prompts

The prompts below are grouped by the stage of work they supported.

### 1. Bootstrapping the project

- Confirm the Go version.
- Initialise the Go module (`go mod init`).
- Tidy the project's dependencies (`go mod tidy`).
- Add the Gin web framework for handling HTTP requests.
- Set up the Swagger / OpenAPI documentation.
- Generate the router.

### 2. Routes and a first deployment check

> "Set up routes just for checking the deployment on minikube first."

### 3. API documentation, integration, and deployment

> "Generate swagger doc and integrate with the Go HTTPS APIs and generate
> deployment steps onto the minikube; bring up the minikube cluster if it
> doesn't exist."

### 4. Containerisation, deployment, and exposure

> "Make a Docker image for this REST API and deploy to minikube."

> "Package this API to a Docker image and deploy it to the minikube cluster with
> Helm charts. First set up the security within a dedicated minikube namespace."

> "Run the deploy through a `make` command and update that in the Makefile."

> "Expose this service outside of WSL or minikube as well, without a `kubectl`
> port forwarder command using nodeport services."

> "Update the README, PROJECT_SETUP, and AI usage documents."

### 5. The Telemetry Collector

> "Now it's time to focus on the Telemetry Collector: consumes telemetry from
> the custom message queue, parses and persists it. Support the ability to
> dynamically scale up/down the number of Collectors. For now use Kafka; data
> coming into the Kafka topic follows the CSV file in project_docs."

> "Use the Postgres database with the TimescaleDB extension."

> "This should be a separate Dockerfile, as we want to scale it up and down as
> per the load."

From these, the assistant designed and built the Collector: a shared telemetry
record contract, a technology-agnostic `queue.Consumer` interface with a Kafka
implementation, a `store.TelemetryStore` interface with a TimescaleDB
implementation, the batching collector engine, a separate container image, a
Helm chart with a HorizontalPodAutoscaler, a docker-compose dev stack, and unit
tests with Makefile coverage targets.

### 6. Documentation hygiene

> "Remove the unnecessary comments from code and deployment files."

The assistant found the codebase was deliberately, thoroughly commented, so
rather than strip valuable explanations it asked how aggressive to be and — on
the "noise only" answer — removed just decorative banner/divider lines, keeping
all rationale and doc comments.

### 7. The Telemetry Streamer

> "Now focus on the Telemetry Streamer: reads telemetry from CSV and streams it
> periodically over the custom message queue. Support the ability to dynamically
> scale up/down the number of Streamers. The time at which a telemetry log is
> processed is its timestamp. For now use Kafka."

> "This CSV is supposed to be loaded onto a PVC in minikube and read by the Go
> app at runtime — change the logic in the streamer accordingly."

From these the assistant built the Streamer: a `queue.Producer` interface with a
Kafka implementation, a DCGM-CSV parser, the replay engine (stamp each datapoint
with its processing time, loop to simulate a continuous stream, key by GPU UUID),
a separate image and Helm chart with an HPA, a PVC + data-loader for the CSV, and
unit tests.

### 8. Wiring the API to the store (original session)

> "Update the API as well for these backend services/store changes."

> "Update all the READMEs as per the structure and implementation done."

The assistant added a `store.TelemetryReader` read interface (implemented on
Postgres), wired it into the previously-stubbed API handlers (`/api/v1/gpus`,
`/api/v1/gpus/{id}/telemetry`) with parameter validation and a `/readyz` probe,
connected the API to TimescaleDB, regenerated the OpenAPI spec, and brought all
the documentation in line.

## Where AI fell short

AI accelerated the work, but its output was not accepted blindly. Items that
required manual intervention or human course-correction:

**During early scaffolding**

1. **A real bug** — an "offset-0" defect that was caught by the generated tests.
2. **An OpenAPI correction** — the `swag init` command had to be corrected.
3. **A design change** — how the CSV telemetry data is delivered was redesigned
   by hand.

**During containerisation and deployment**

4. **The code did not compile.** Before an image could be built, the assistant
   had to find and fix several half-finished pieces of code (a missing logger,
   an undefined variable, an inconsistent module path, and missing imports).
5. **The cluster did not actually exist.** The request assumed a running
   minikube cluster, but there was none; one had to be started — and minikube's
   Docker driver refused to run as the administrator user until an explicit
   override was supplied.
6. **Network rules were not being enforced.** The default cluster networking
   silently ignores the security network rules, so the cluster had to be
   recreated with a networking add-on (Calico) that actually enforces them.
7. **A packaging ordering problem.** The deployment tool needs the namespace to
   exist before it can install into it, so creating the namespace had to be
   split out into a separate first step (which also matched the "security
   first" requirement).
8. **The security rules initially blocked legitimate traffic.** The default
   "deny everything" policy also blocked the externally-exposed port, so an
   explicit "allow external access to the API port" rule had to be added.
9. **A course-correction on how to expose the service.** The assistant's first
   approach used a custom forwarding script; on feedback, this was replaced with
   minikube's own built-in commands for accessing cluster services.

**During the Collector build**

10. **Conflicting requirements, reconciled by design.** The brief forbids
    off-the-shelf queues, but the instruction was to "use Kafka for now". Rather
    than pick one, the assistant put the queue behind a small interface so Kafka
    is a swappable implementation and the eventual custom queue is a drop-in.
11. **A persistence change mid-build.** The driver already vendored pointed at
    MongoDB; the human redirected to PostgreSQL/TimescaleDB partway through. Only
    the implementation behind the storage interface changed — the interface and
    collector were untouched, validating the abstraction.
12. **A packaging course-correction.** The assistant's first move folded the
    collector into the existing image via a build argument; on feedback this was
    split into a dedicated `Dockerfile.collector` so the service scales as its
    own independently-versioned image.
13. **A toolchain/environment workaround.** The Go toolchain could not lock files
    over the `\\wsl.localhost` share from Windows, so builds and tests were run
    inside a `golang` container bind-mounted to the native Linux path.

**During the Streamer and API build**

14. **A data-delivery course-correction.** The assistant's first Streamer baked
    the CSV into the binary (`go:embed`). The human redirected it to load the CSV
    from a PersistentVolume at runtime, so the dataset is versioned and
    provisioned independently of the image. The assistant reworked the loader,
    added the PVC + data-loader manifests, and switched Compose to a bind mount.
15. **Over-eager comment removal, avoided by asking.** Asked to "remove
    unnecessary comments," the assistant recognised the comments were valuable and
    asked for the intended aggressiveness rather than stripping documentation
    wholesale.
16. **A second OpenAPI correction.** `swag` could not resolve `telemetry.Record`
    in a response annotation until the type was imported into the handler file;
    the fix doubled as a real improvement (empty results now serialise as `[]`,
    not `null`).
17. **Line-ending consistency.** Rewriting existing files flipped them from the
    repo's CRLF to LF; the assistant converted them back so each package stayed
    internally consistent.

These corrections are the reason every AI-generated output in this project was
reviewed before being kept.

---

### 9. The Custom gRPC Message Queue (Stage 1)

> "Implement a custom message queue to replace Kafka. Use gRPC. Start with a
> pure in-memory broker behind the existing `queue.Consumer` / `queue.Producer`
> interfaces. Add a feature flag `QUEUE_TYPE=grpc` so Kafka and the new queue
> can run side by side."

> "Deploy the queue as its own Kubernetes service with a Helm chart, add
> NetworkPolicies so the collector and streamer can reach it, and update the
> Makefile with build/deploy/undeploy targets."

> "Fix the errors in the running pods — the streamer is failing with
> `proto: failed to marshal, message is *grpc.ProduceRequest, want proto.Message`."

> "Update all the markup files for documentation."

From these the assistant built: the in-memory broker (`internal/queue/server/`),
hand-written gRPC service bindings (`internal/queue/grpc/api.go`) without protoc,
a JSON codec override that registers as `"proto"` so gRPC uses `encoding/json`
instead of protobuf (`internal/queue/grpc/codec.go`), producer/consumer clients
(`internal/queue/grpc/client.go`), the queue binary (`cmd/queue/`), a Helm chart
(`deploy/helm/gpu-telemetry-queue/`), a Dockerfile, and Makefile targets for the
full build/deploy lifecycle.

## Where AI fell short (custom queue session)

**During the gRPC queue build**

18. **gRPC marshal error in production.** The streamer pods failed with
    `proto: failed to marshal, message is *grpc.ProduceRequest, want proto.Message`.
    The fix (`codec.go`) was correct, but the streamer image in minikube was stale —
    built before `codec.go` existed. The root cause took multiple rounds to diagnose:
    `minikube image load` silently no-ops when a running container holds the same
    tag, so the new image was never actually loaded. Fix: scale to 0 replicas,
    `minikube image rm`, reload, scale back to 1.

19. **Docker exec cache mounts obscured stale builds.** `docker build --no-cache`
    clears the layer cache but not BuildKit exec cache mounts
    (`--mount=type=cache,target=/root/.cache/go-build`). The Go build cache
    persisted independently, making it appear a rebuild had occurred when it had
    not. Fix: `docker builder prune --filter type=exec.cachemount -f` before
    rebuilding.

20. **Deprecated gRPC dial API.** The initial client code used `grpc.Dial` and
    `grpc.WithInsecure()`, both deprecated in grpc-go v1.27+. Updated to
    `grpc.NewClient` + `insecure.NewCredentials()`.

21. **Queue Helm chart Chart.yaml had `type: app` (invalid).** Helm requires
    `type: application` or `type: library`; the invalid value caused chart
    validation to fail.

22. **NetworkPolicy gaps.** The collector and streamer charts initially opened
    only Kafka egress (port 9092). When `QUEUE_TYPE=grpc`, they need egress to
    port 50051 instead. The queue chart had no NetworkPolicy at all. Both were
    fixed: Collector/Streamer NetworkPolicies are now conditional on `queue.type`,
    and the queue chart has its own allow rule for gRPC ingress.
