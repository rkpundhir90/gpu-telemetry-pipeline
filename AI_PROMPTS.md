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
| **What it did** | Scaffolded the project's initial structure, then containerised the API and deployed it to a security-hardened Kubernetes (minikube) namespace with Helm |
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

These corrections are the reason every AI-generated output in this project was
reviewed before being kept.
