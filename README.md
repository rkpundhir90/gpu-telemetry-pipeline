# Elastic GPU Telemetry Pipeline with a Custom Message Queue

An elastic, horizontally-scalable telemetry pipeline for an AI/GPU cluster. The
goal is to stream GPU telemetry (DCGM exporter CSV) through a **custom message
queue** (no Kafka/RabbitMQ/etc.), persist it, and expose it over a REST API with
an auto-generated OpenAPI spec.

> **Status — early stage.** This repository currently contains the REST API
> layer (Gin), the Swagger/OpenAPI wiring, the reference data, and a hardened
> container + Helm deployment onto minikube. The broader pipeline (message
> queue, collectors, and database) is planned and will be added over time. This
> README describes **what exists today** and will grow as the project does.

## Table of contents
- [What's in the repo today](#whats-in-the-repo-today)
- [Tech stack (today)](#tech-stack-today)
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
cmd/api/main.go       service entry point (graceful shutdown, slog logging)
internal/api/         REST API layer (Gin)
  handler.go            request handlers + OpenAPI annotations
  router.go             routes, structured logging, Swagger UI route
internal/config/      env-driven configuration (API_LISTEN_ADDR, …)
Dockerfile            multi-stage build -> minimal distroless image
.dockerignore         build-context exclusions
deploy/
  namespace.yaml        dedicated, security-hardened namespace (restricted PSA)
  helm/gpu-telemetry-api/   Helm chart (Deployment, Service, SA, NetworkPolicies)
Makefile              build / deploy / expose targets (see "Make targets")
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
- **Container:** multi-stage Docker build on a `distroless/static:nonroot` base
- **Orchestration:** Kubernetes via [minikube](https://minikube.sigs.k8s.io/),
  packaged with [Helm](https://helm.sh/), [Calico](https://www.tigera.io/project-calico/)
  CNI for NetworkPolicy enforcement

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

## Container image

A multi-stage [`Dockerfile`](Dockerfile) builds a statically-linked binary
(`CGO_ENABLED=0`, stripped) in a `golang` stage and copies it into a
`gcr.io/distroless/static-debian12:nonroot` runtime stage — no shell, no package
manager, runs as a non-root user (uid `65532`). The result is a small,
low-attack-surface image.

```bash
make docker-build            # DOCKER_BUILDKIT=1 docker build -t gpu-telemetry-api:0.1.0 .
```

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
| `make deploy` | Full pipeline: build → load → namespace → helm install. |
| `make status` | Show workloads + security objects in the namespace. |
| `make service-url` / `make expose` | Print URL / open a tunnel for host access. |
| `make undeploy` | Uninstall the release and delete the namespace. |
| `make openapi` | Regenerate the OpenAPI spec from handler annotations. |

## Roadmap

Done:

- ✅ **Containerisation and Kubernetes (minikube) deployment** via Helm, with a
  security-hardened, dedicated namespace.

The following are part of the project's goal and will be added over time:

- A **custom message queue** (competing-consumers work queue) — no Kafka/RabbitMQ.
- **Streamer** and **collector** services that replay the CSV and persist telemetry.
- A **persistence layer** (database) behind the API.
- Implement the API handlers (they currently return `501 Not Implemented`).
- Unit tests and coverage gating.

## AI assistance

This repository's initial structure was scaffolded with Claude Code. A detailed
account of the prompts used and where AI needed manual intervention is in
[project_docs/AI_PROMPTS.md](project_docs/AI_PROMPTS.md).
