#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

NAMESPACE="${NAMESPACE:-gpu-telemetry}"
TIMESCALE_RELEASE="${TIMESCALE_RELEASE:-timescaledb}"
TIMESCALE_CHART="${TIMESCALE_CHART:-bitnami/postgresql}"
TIMESCALE_VALUES="${TIMESCALE_VALUES:-$REPO_ROOT/helm/timescaledb/values.yaml}"
IMAGE_REPO="${TIMESCALE_IMAGE_REPO:-timescale/timescaledb}"
IMAGE_TAG="${TIMESCALE_IMAGE_TAG:-latest-pg15}"
ALLOW_INSECURE_IMAGES="${ALLOW_INSECURE_IMAGES:-true}"
AUTH_USER="${TIMESCALE_AUTH_USER:-telemetry}"
AUTH_PASSWORD="${TIMESCALE_AUTH_PASSWORD:-telemetry}"
AUTH_DATABASE="${TIMESCALE_AUTH_DATABASE:-telemetry}"

command -v helm >/dev/null 2>&1 || { echo "helm not found; install helm first"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "kubectl not found; install kubectl first"; exit 1; }

# Ensure namespace exists
if ! kubectl get ns "$NAMESPACE" >/dev/null 2>&1; then
  echo "Creating namespace $NAMESPACE"
  kubectl apply -f "$REPO_ROOT/namespace.yaml"
fi

echo "Adding/updating Helm repo bitnami..."
helm repo add bitnami https://charts.bitnami.com/bitnami || true
helm repo update

# Build --set args
set_args=(--set image.repository="$IMAGE_REPO" --set image.tag="$IMAGE_TAG" --set auth.username="$AUTH_USER" --set auth.password="$AUTH_PASSWORD" --set auth.database="$AUTH_DATABASE" --set postgresqlSharedPreloadLibraries='timescaledb')
if [ "$ALLOW_INSECURE_IMAGES" = "true" ]; then
  set_args+=("--set" "global.security.allowInsecureImages=true")
fi

attempts=3
for i in $(seq 1 $attempts); do
  echo "Helm attempt $i/$attempts..."
  if helm upgrade --install "$TIMESCALE_RELEASE" "$TIMESCALE_CHART" \
       --namespace "$NAMESPACE" \
       -f "$TIMESCALE_VALUES" \
       "${set_args[@]}" \
       --wait --timeout 180s --atomic; then
    echo "Helm deploy succeeded"

    # The timescale/timescaledb image does not run the Bitnami startup scripts
    # that normally add "include_dir" to postgresql.conf. Without it the
    # override.conf (which sets ssl = on) is never loaded. Append the directive
    # once; the line is idempotent — grep guards against duplicates.
    PG_POD=$(kubectl get pod -n "$NAMESPACE" -l app.kubernetes.io/name=postgresql -o jsonpath='{.items[0].metadata.name}')
    echo "Ensuring include_dir is set in postgresql.conf (pod: $PG_POD)..."
    kubectl exec -n "$NAMESPACE" "$PG_POD" -- bash -c "
      grep -qF \"include_dir = '/bitnami/postgresql/conf/conf.d'\" /bitnami/postgresql/data/postgresql.conf \
        || echo \"include_dir = '/bitnami/postgresql/conf/conf.d'\" >> /bitnami/postgresql/data/postgresql.conf
      pg_ctl reload -D /bitnami/postgresql/data -s
    "

    echo "Running db-init Job to ensure database and extension exist..."
    # Delete any previous run so kubectl wait has a clean job to watch.
    kubectl delete job timescaledb-db-init -n "$NAMESPACE" --ignore-not-found
    kubectl apply -f "$SCRIPT_DIR/db-init-job.yaml"
    if kubectl wait --for=condition=complete job/timescaledb-db-init \
         -n "$NAMESPACE" --timeout=120s; then
      echo "✓ Database init complete"
    else
      echo "db-init Job did not complete — check: kubectl logs -n $NAMESPACE -l app=timescaledb-db-init"
      exit 1
    fi

    kubectl -n "$NAMESPACE" get deploy,statefulset,pod,svc -o wide
    exit 0
  else
    echo "Helm deploy failed (attempt $i)."
    if [ "$i" -lt "$attempts" ]; then
      echo "Waiting 10s before retrying..."
      sleep 10
    else
      echo "All attempts failed. Gathering debug output..."
      set +e
      helm --debug upgrade --install "$TIMESCALE_RELEASE" "$TIMESCALE_CHART" \
        --namespace "$NAMESPACE" \
        -f "$TIMESCALE_VALUES" \
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
