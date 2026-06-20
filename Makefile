.PHONY: setup-infra install-docker install-minikube install-kubectl install-helm verify start-minikube

# Check for existing installations
DOCKER_BIN := $(shell command -v docker 2> /dev/null)
MINIKUBE_BIN := $(shell command -v minikube 2> /dev/null)
KUBECTL_BIN := $(shell command -v kubectl 2> /dev/null)
HELM_BIN := $(shell command -v helm 2> /dev/null)

# Sets up the required infrastructure (Docker, Minikube, kubectl, and Helm)
setup-infra: install-docker install-minikube install-kubectl install-helm verify

# ==========================================
# UBUNTU/DEBIAN TARGETS
# ==========================================

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
	@echo "Starting Minikube with Docker driver + Calico CNI (NetworkPolicy enforcement)..."
	@echo "(Calico is required for the NetworkPolicies in the chart to actually be enforced.)"
	minikube start --driver=docker --cni=calico $(MINIKUBE_EXTRA_ARGS)

.PHONY: openapi

# Regenerate the OpenAPI/Swagger spec from the handler annotations into docs/.
openapi:
	swag init \
		--generalInfo internal/api/router.go \
		--output docs \
		--parseInternal \
		--parseDependency

# ==========================================
# BUILD & DEPLOY (Docker image + Helm -> minikube)
# ==========================================

IMAGE     ?= gpu-telemetry-api
TAG       ?= 0.1.0
NAMESPACE ?= gpu-telemetry
RELEASE   ?= gpu-telemetry
CHART_DIR ?= deploy/helm/gpu-telemetry-api

.PHONY: docker-build minikube-load namespace helm-lint helm-template deploy undeploy status expose service-url

# Build the minimal, multi-stage (distroless, static) image.
docker-build:
	DOCKER_BUILDKIT=1 docker build -t $(IMAGE):$(TAG) .

# Load the locally built image into the running minikube cluster.
minikube-load: docker-build
	minikube image load $(IMAGE):$(TAG)

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

# Full deploy: build image -> load into minikube -> harden namespace -> helm release.
deploy: minikube-load namespace helm-lint
	helm upgrade --install $(RELEASE) $(CHART_DIR) \
		--namespace $(NAMESPACE) \
		--create-namespace=false \
		--wait --timeout 180s
	kubectl -n $(NAMESPACE) get deploy,pod,svc -o wide

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

# Remove the release and the dedicated namespace.
undeploy:
	-helm uninstall $(RELEASE) --namespace $(NAMESPACE)
	-kubectl delete -f deploy/namespace.yaml
