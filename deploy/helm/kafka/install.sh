#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

NAMESPACE="${NAMESPACE:-gpu-telemetry}"
KAFKA_RELEASE="${KAFKA_RELEASE:-kafka}"
KAFKA_CHART="${KAFKA_CHART:-bitnami/kafka}"
KAFKA_VALUES="${KAFKA_VALUES:-$REPO_ROOT/deploy/helm/kafka/values.yaml}"
ALLOW_INSECURE_IMAGES="${ALLOW_INSECURE_IMAGES:-false}"

command -v helm >/dev/null 2>&1 || { echo "helm not found; install helm first"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "kubectl not found; install kubectl first"; exit 1; }

# Ensure namespace exists
if ! kubectl get ns "$NAMESPACE" >/dev/null 2>&1; then
  kubectl apply -f "$REPO_ROOT/deploy/namespace.yaml"
fi

echo "Adding/updating Helm repo bitnami..."
helm repo add bitnami https://charts.bitnami.com/bitnami || true
helm repo update

set_args=(--set replicaCount=1 --set zookeeper.enabled=true --set auth.enabled=false --set persistence.enabled=false)
if [ "$ALLOW_INSECURE_IMAGES" = "true" ]; then
  set_args+=(--set global.security.allowInsecureImages=true)
fi

attempts=3
for i in $(seq 1 $attempts); do
  echo "Helm attempt $i/$attempts..."
  if helm upgrade --install "$KAFKA_RELEASE" "$KAFKA_CHART" \
       --namespace "$NAMESPACE" \
       -f "$KAFKA_VALUES" \
       "${set_args[@]}" \
       --wait --timeout 180s --atomic; then
    echo "Kafka deploy succeeded"
    kubectl -n "$NAMESPACE" get statefulset,svc,pod -o wide
    exit 0
  else
    echo "Kafka deploy failed (attempt $i)."
    if [ "$i" -lt "$attempts" ]; then
      echo "Waiting 10s before retrying..."
      sleep 10
    else
      echo "All attempts failed. Gathering debug output..."
      set +e
      helm --debug upgrade --install "$KAFKA_RELEASE" "$KAFKA_CHART" \
        --namespace "$NAMESPACE" \
        -f "$KAFKA_VALUES" \
        "${set_args[@]}" \
        --timeout 60s || true
      echo "=== kubectl events ==="
      kubectl -n "$NAMESPACE" get events --sort-by='.metadata.creationTimestamp' || true
      echo "=== describe pods ==="
      kubectl -n "$NAMESPACE" describe pods || true
      exit 1
    fi
  fi
done
