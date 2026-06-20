# Project Setup

These are the steps used to bootstrap the project from scratch. They cover
initialising the Go module, adding the web framework and API documentation, and
creating the first routes so the service can be deployed to minikube for an
early end-to-end check.

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
  into that namespace. The full pipeline (build → load image → namespace →
  install) is a single target: `make deploy`.
- Verify: `make status`, then `curl http://$(minikube ip):30080/healthz`.

## 9. Access from the host

- Expose the NodePort service to the host using minikube's native tunnel
  (no `kubectl port-forward`): `make service-url`, then reach it from the
  Windows host at `http://localhost:<port>`.

> For the full command reference and the security model, see the
> [README](README.md).
