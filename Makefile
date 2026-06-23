.PHONY: setup-infra install-docker install-minikube install-kubectl install-helm verify start-minikube stop-minikube

# Use docker driver by default
MINIKUBE_DRIVER ?= docker
MINIKUBE_FORCE ?= 0
MINIKUBE_START_ARGS ?=

# Sets up the required infrastructure (Docker, Minikube, kubectl, and Helm)
setup-infra: install-docker install-minikube install-kubectl install-helm verify start-minikube

# Ubuntu/Debian targets
install-docker:
ifndef DOCKER_BIN
	@echo "Updating apt package index..."
	sudo apt-get update
	@echo "Installing prerequisites..."
	sudo apt-get install -y ca-certificates curl gnupg lsb-release
	@echo "Adding Docker's official GPG key..."
	sudo mkdir -m 0755 -p /etc/apt/keyrings
	curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg --yes
	sudo chmod a+r /etc/apt/keyrings/docker.gpg
	@echo "Setting up the Docker repository..."
	echo "deb [arch=$$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $$(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
	@echo "Installing Docker Engine..."
	sudo apt-get update
	sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
	@echo "Adding current user to the docker group..."
	sudo usermod -aG docker $$USER
	@echo "✅ Docker installation complete!"
	@echo "⚠️ IMPORTANT: You must log out and log back in (or run 'newgrp docker') to use Docker without sudo."
else
	@echo "✅ Docker is already installed. Skipping..."
endif

install-minikube:
ifndef MINIKUBE_BIN
	@echo "Downloading Minikube for Linux (amd64)..."
	curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
	@echo "Installing Minikube executable..."
	sudo install minikube-linux-amd64 /usr/local/bin/minikube
	@echo "Cleaning up..."
	rm minikube-linux-amd64
	@echo "✅ Minikube installation complete!"
else
	@echo "✅ Minikube is already installed. Skipping..."
endif

install-kubectl:
ifndef KUBECTL_BIN
	@echo "Downloading kubectl for Linux..."
	curl -LO "https://dl.k8s.io/release/$$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
	@echo "Installing kubectl executable..."
	sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
	@echo "Cleaning up..."
	rm kubectl
	@echo "✅ kubectl installation complete!"
else
	@echo "✅ kubectl is already installed. Skipping..."
endif

install-helm:
ifndef HELM_BIN
	@echo "Downloading Helm installation script..."
	curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
	@echo "Installing Helm..."
	chmod 700 get_helm.sh
	./get_helm.sh
	@echo "Cleaning up..."
	rm get_helm.sh
	@echo "✅ Helm installation complete!"
else
	@echo "✅ Helm is already installed. Skipping..."
endif

verify:
	@echo "\n--- Verification ---"
	@docker --version || echo "Docker not found"
	@minikube version || echo "Minikube not found"
	@kubectl version --client || echo "kubectl not found"
	@helm version || echo "Helm not found"
	@echo "--------------------\n"

start-minikube:
	@echo "Starting Minikube with driver '$(MINIKUBE_DRIVER)' (CNI: calico)..."
	@echo "(Calico is required for the NetworkPolicies in the chart to actually be enforced.)"
	minikube start --driver=$(MINIKUBE_DRIVER) --cni=calico --force $(MINIKUBE_EXTRA_ARGS) $(MINIKUBE_START_ARGS)

stop-minikube:
	@echo "Stopping and deleting Minikube cluster..."
	-minikube delete --all --force
	@echo "Cleaning up minikube data directory..."
	-rm -rf ~/.minikube
	@echo "✅ Minikube cluster removed!"

.PHONY: openapi

# Regenerate the OpenAPI/Swagger spec from the handler annotations into docs/.
openapi:
	swag init \
		--generalInfo internal/api/router.go \
		--output docs \
		--parseInternal \
		--parseDependency

# Build & deploy (Docker image + Helm -> minikube)
IMAGE     ?= gpu-telemetry-api
TAG       ?= 0.1.0
NAMESPACE ?= gpu-telemetry
RELEASE   ?= gpu-telemetry
CHART_DIR ?= deploy/helm/gpu-telemetry-api

# Collector service: its own image and chart so it scales independently.
COLLECTOR_IMAGE     ?= gpu-telemetry-collector
COLLECTOR_RELEASE   ?= gpu-telemetry-collector
COLLECTOR_CHART_DIR ?= deploy/helm/gpu-telemetry-collector

# Streamer service: its own image and chart so it scales independently.
STREAMER_IMAGE     ?= gpu-telemetry-streamer
STREAMER_RELEASE   ?= gpu-telemetry-streamer
STREAMER_CHART_DIR ?= deploy/helm/gpu-telemetry-streamer

# Queue service: the custom gRPC broker.
QUEUE_IMAGE        ?= gpu-telemetry-queue
QUEUE_RELEASE      ?= gpu-telemetry-queue
QUEUE_CHART_DIR    ?= deploy/helm/gpu-telemetry-queue

# Global Queue Configuration (used by Collector and Streamer)
QUEUE_TYPE        ?= kafka
QUEUE_ADDR        ?= gpu-telemetry-queue:50051

# Telemetry data loaded onto the streamer's PVC at runtime (not baked into the image).
STREAMER_CSV       ?= project_docs/dcgm_metrics_20250718_134233.csv
STREAMER_CSV_NAME  ?= dcgm_metrics.csv

.PHONY: docker-build minikube-load namespace helm-lint helm-template deploy undeploy status expose service-url
.PHONY: docker-build-collector minikube-load-collector deploy-collector undeploy-collector
.PHONY: docker-build-streamer minikube-load-streamer deploy-streamer undeploy-streamer load-streamer-data
.PHONY: docker-build-queue minikube-load-queue deploy-queue undeploy-queue
.PHONY: deploy-api undeploy-api deploy-all undeploy-all

# Dockerfiles live under deploy/build; the build context is the repo root (where
# the Go module is), passed as the final ".".
API_DOCKERFILE       ?= deploy/build/Dockerfile.api
COLLECTOR_DOCKERFILE ?= deploy/build/Dockerfile.collector
STREAMER_DOCKERFILE  ?= deploy/build/Dockerfile.streamer
QUEUE_DOCKERFILE     ?= deploy/build/Dockerfile.queue

# Build the minimal, multi-stage (distroless, static) image.
docker-build:
	DOCKER_BUILDKIT=1 docker build -f $(API_DOCKERFILE) -t $(IMAGE):$(TAG) .

# Load the locally built image into the running minikube cluster.
minikube-load: docker-build
	minikube image load $(IMAGE):$(TAG)

docker-build-collector:
	DOCKER_BUILDKIT=1 docker build -f $(COLLECTOR_DOCKERFILE) -t $(COLLECTOR_IMAGE):$(TAG) .

minikube-load-collector: docker-build-collector
	minikube image load $(COLLECTOR_IMAGE):$(TAG)

docker-build-queue:
	DOCKER_BUILDKIT=1 docker build -f $(QUEUE_DOCKERFILE) -t $(QUEUE_IMAGE):$(TAG) .

minikube-load-queue: docker-build-queue
	minikube image load $(QUEUE_IMAGE):$(TAG)

docker-build-streamer:
	DOCKER_BUILDKIT=1 docker build -f $(STREAMER_DOCKERFILE) -t $(STREAMER_IMAGE):$(TAG) .

minikube-load-streamer: docker-build-streamer
	minikube image load $(STREAMER_IMAGE):$(TAG)

# Step 1 of security: create + harden the dedicated namespace
# (restricted Pod Security Admission).
namespace:
	kubectl apply -f deploy/namespace.yaml
	kubectl get ns $(NAMESPACE) --show-labels

helm-lint:
	helm lint $(CHART_DIR)

# Render templates without installing (sanity check).
helm-template:
	helm template $(RELEASE) $(CHART_DIR) --namespace $(NAMESPACE)


# Deploy the API chart (expects the namespace + backing services to already
# exist). Builds + loads the image, then installs / upgrades the API release.
# For the full pipeline in one command use `make deploy`.
deploy-api: minikube-load namespace
	helm lint $(CHART_DIR)
	helm upgrade --install $(RELEASE) $(CHART_DIR) \
		--namespace $(NAMESPACE) \
		--create-namespace=false \
		--wait --timeout 180s
	kubectl -n $(NAMESPACE) get deploy,pod,svc -o wide
	@echo "✅ API deployed. Run 'make service-url' or visit /swagger/index.html."

undeploy-api:
	-helm uninstall $(RELEASE) --namespace $(NAMESPACE)

# Full deploy: TimescaleDB → Queue → Collector → Streamer → API.
deploy: deploy-timescaledb deploy-queue deploy-collector deploy-streamer deploy-api
	@echo "✅ Full pipeline deployed to namespace $(NAMESPACE). Run 'make service-url' to connect."

# Show what is running in the namespace, including the security posture.
status:
	kubectl -n $(NAMESPACE) get deploy,pod,svc,networkpolicy,serviceaccount -o wide
	kubectl get ns $(NAMESPACE) --show-labels

# Print the externally-reachable URL via minikube's native tunnel. With the
# docker driver this binds 127.0.0.1 on the WSL host, which the Windows host
# reaches through WSL2 localhost forwarding. Keep this process running.
service-url:
	minikube service $(RELEASE)-$(IMAGE) --namespace $(NAMESPACE) --url

# Same as above but opens the tunnel interactively (Ctrl-C to stop).
expose:
	minikube service $(RELEASE)-$(IMAGE) --namespace $(NAMESPACE)

# Deploy the collector chart (expects the namespace to already exist; create it
# with `make namespace`). Pass Kafka/Postgres endpoints via --set or values.
deploy-collector: minikube-load-collector namespace
	helm lint $(COLLECTOR_CHART_DIR)
	helm upgrade --install $(COLLECTOR_RELEASE) $(COLLECTOR_CHART_DIR) \
		--namespace $(NAMESPACE) \
		--create-namespace=false \
		--set queue.type=$(QUEUE_TYPE) \
		--set queue.addr=$(QUEUE_ADDR) \
		--wait --timeout 180s
	kubectl -n $(NAMESPACE) get deploy,pod,hpa -l app.kubernetes.io/name=gpu-telemetry-collector -o wide

undeploy-collector:
	-helm uninstall $(COLLECTOR_RELEASE) --namespace $(NAMESPACE)

deploy-queue: minikube-load-queue namespace
	helm lint $(QUEUE_CHART_DIR)
	helm upgrade --install $(QUEUE_RELEASE) $(QUEUE_CHART_DIR) \
		--namespace $(NAMESPACE) \
		--create-namespace=false \
		--wait --timeout 180s
	kubectl -n $(NAMESPACE) get deploy,pod,svc -l app.kubernetes.io/name=gpu-telemetry-queue -o wide

undeploy-queue:
	-helm uninstall $(QUEUE_RELEASE) --namespace $(NAMESPACE)

# Provision the streamer's data PVC and copy the CSV onto it, so the Streamer
# reads it from a mounted volume at runtime. Uses a short-lived helper pod
# because the local file must be copied into the volume (`kubectl cp`).
load-streamer-data: namespace
	kubectl apply -f deploy/streamer-data-pvc.yaml
	kubectl -n $(NAMESPACE) apply -f deploy/streamer-data-loader.yaml
	kubectl -n $(NAMESPACE) wait --for=condition=ready pod/streamer-data-loader --timeout=120s
	kubectl -n $(NAMESPACE) cp $(STREAMER_CSV) streamer-data-loader:/data/$(STREAMER_CSV_NAME)
	kubectl -n $(NAMESPACE) delete pod streamer-data-loader --wait=false
	@echo "✅ loaded $(STREAMER_CSV) -> PVC gpu-telemetry-streamer-data:/data/$(STREAMER_CSV_NAME)"

# Deploy the streamer chart (expects the namespace to already exist; create it
# with `make namespace`). Loads the CSV onto the PVC first. Point it at Kafka via
# --set or values.
deploy-streamer: minikube-load-streamer namespace load-streamer-data
	helm lint $(STREAMER_CHART_DIR)
	helm upgrade --install $(STREAMER_RELEASE) $(STREAMER_CHART_DIR) \
		--namespace $(NAMESPACE) \
		--create-namespace=false \
		--set queue.type=$(QUEUE_TYPE) \
		--set queue.addr=$(QUEUE_ADDR) \
		--wait --timeout 180s
	kubectl -n $(NAMESPACE) get deploy,pod,hpa -l app.kubernetes.io/name=gpu-telemetry-streamer -o wide

undeploy-streamer:
	-helm uninstall $(STREAMER_RELEASE) --namespace $(NAMESPACE)

# Alias for `make deploy` kept for backwards compatibility.
deploy-all: deploy
	@echo "✅ Full pipeline deployed to namespace $(NAMESPACE)."

# Remove the API release and the dedicated namespace.
undeploy:
	-helm uninstall $(RELEASE) --namespace $(NAMESPACE)
	-kubectl delete -f deploy/namespace.yaml

# Tear down the whole pipeline: uninstall all releases, then delete the namespace.
undeploy-all: undeploy-streamer undeploy-collector undeploy
	@echo "✅ removed API + Collector + Streamer from namespace $(NAMESPACE)"

# Test & coverage
.PHONY: test cover cover-html

# Run all unit tests with the race detector.
test:
	go test -race ./...

# Run tests and report total statement coverage (gates can grep COVERAGE_TOTAL).
cover:
	go test -race -covermode=atomic -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out | tail -n 1 | awk '{print "COVERAGE_TOTAL " $$3}'

# Generate a browsable HTML coverage report.
cover-html: cover
	go tool cover -html=coverage.out -o coverage.html
	@echo "wrote coverage.html"

# TimescaleDB (Helm deploy script)
TIMESCALE_IMAGE_REPO ?= timescale/timescaledb
TIMESCALE_IMAGE_TAG  ?= latest-pg15
TIMESCALE_RELEASE    ?= timescaledb
TIMESCALE_VALUES     ?= deploy/helm/timescaledb/values.yaml

.PHONY: deploy-timescaledb

deploy-timescaledb:
	./deploy/helm/timescaledb/install.sh

.PHONY: deploy-kafka

deploy-kafka:
	./deploy/helm/kafka/install.sh
