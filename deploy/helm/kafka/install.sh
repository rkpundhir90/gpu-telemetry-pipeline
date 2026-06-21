#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

NAMESPACE="${NAMESPACE:-gpu-telemetry}"

command -v kubectl >/dev/null 2>&1 || { echo "kubectl not found; install kubectl first"; exit 1; }

# Ensure namespace exists
if ! kubectl get ns "$NAMESPACE" >/dev/null 2>&1; then
  kubectl apply -f "$REPO_ROOT/deploy/namespace.yaml"
fi

echo "Deploying Kafka StatefulSet (single-node, minimal setup)..."

# Apply the StatefulSet manifest
if kubectl apply -f "$SCRIPT_DIR/kafka-statefulset.yaml"; then
  echo "Kafka StatefulSet deployed successfully"
  
  # Wait for StatefulSet to be ready
  echo "Waiting for Kafka pod to be ready..."
  if kubectl rollout status statefulset/kafka -n "$NAMESPACE" --timeout=300s; then
    echo "✓ Kafka is ready"
    kubectl -n "$NAMESPACE" get statefulset,svc,pod -o wide
    exit 0
  else
    echo "Timeout waiting for Kafka to be ready"
    echo "=== pod status ==="
    kubectl -n "$NAMESPACE" describe pod kafka-0 || true
    exit 1
  fi
else
  echo "Failed to deploy Kafka StatefulSet"
  exit 1
fi
