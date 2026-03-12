#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_NAME="${IMAGE_NAME:-replay-api:latest}"
API_PORT="${API_PORT:-18080}"
WITH_KAFKA="${WITH_KAFKA:-false}"
SKIP_BUILD="${SKIP_BUILD:-false}"

usage() {
  cat <<'EOF'
Usage: scripts/deploy.sh [--with-kafka] [--skip-build]

End-to-end deploy helper for local Kubernetes workflows.

What it does:
  1. Builds the application JAR with Maven
  2. Builds the container image (default: replay-api:latest)
  3. If minikube is running, builds directly into minikube's image store
     (or loads the image when built locally)
  4. Applies Kubernetes manifests in deploy/
  5. Waits for Postgres and replay-api rollouts
  6. Runs a temporary port-forward and health check

Environment variables:
  IMAGE_NAME   Docker image tag to build/load/apply (default: replay-api:latest)
  API_PORT     Local port for temporary health-check port-forward (default: 18080)
  WITH_KAFKA   true|false, deploy optional Kafka (default: false)
  SKIP_BUILD   true|false, skip Maven and Docker build steps (default: false)
EOF
}

log() {
  printf '\n[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 1
  }
}

docker_available() {
  docker info >/dev/null 2>&1
}

wait_for_port() {
  local port="$1"
  local attempts=50
  local i
  for ((i = 1; i <= attempts; i++)); do
    if python3 - "$port" <<'PY'
import socket, sys
port = int(sys.argv[1])
s = socket.socket()
s.settimeout(0.25)
try:
    s.connect(("127.0.0.1", port))
    sys.exit(0)
except Exception:
    sys.exit(1)
finally:
    s.close()
PY
    then
      return 0
    fi
    sleep 1
  done
  return 1
}

port_forward_pid=""
cleanup() {
  if [[ -n "${port_forward_pid}" ]] && kill -0 "${port_forward_pid}" >/dev/null 2>&1; then
    kill "${port_forward_pid}" >/dev/null 2>&1 || true
    wait "${port_forward_pid}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-kafka)
      WITH_KAFKA=true
      shift
      ;;
    --skip-build)
      SKIP_BUILD=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_cmd kubectl
require_cmd docker
require_cmd mvn
require_cmd curl
require_cmd python3

cd "${ROOT_DIR}"

MINIKUBE_RUNNING=false
if command -v minikube >/dev/null 2>&1; then
  if minikube status >/dev/null 2>&1; then
    MINIKUBE_RUNNING=true
  fi
fi

if [[ "${SKIP_BUILD}" != "true" ]]; then
  log "Building application JAR"
  mvn -q package -DskipTests

  if [[ "${MINIKUBE_RUNNING}" == "true" ]]; then
    log "Building image directly in minikube: ${IMAGE_NAME}"
    minikube image build -t "${IMAGE_NAME}" .
  else
    if ! docker_available; then
      cat >&2 <<'EOF'
Docker is not reachable from this shell.

If you previously ran 'eval $(minikube docker-env)', reset your shell first:
  eval "$(minikube docker-env -u)"

Then rerun:
  scripts/deploy.sh
EOF
      exit 1
    fi

    log "Building Docker image ${IMAGE_NAME}"
    docker build -t "${IMAGE_NAME}" .
  fi
else
  log "Skipping Maven and Docker build"
fi

if [[ "${MINIKUBE_RUNNING}" == "true" ]]; then
  if [[ "${SKIP_BUILD}" == "true" ]]; then
    log "Loading existing image into minikube: ${IMAGE_NAME}"
    minikube image load "${IMAGE_NAME}"
  fi
else
  if command -v minikube >/dev/null 2>&1; then
    log "Minikube is installed but not running; skipping minikube image integration"
  else
    log "Minikube not found; if your cluster cannot see ${IMAGE_NAME}, load/push it manually"
  fi
fi

log "Applying Kubernetes secrets and storage"
kubectl apply -f deploy/secret.yaml
kubectl apply -f deploy/postgres-pvc.yaml

log "Deploying PostgreSQL"
kubectl apply -f deploy/postgres-deployment.yaml
kubectl apply -f deploy/postgres-service.yaml

log "Deploying replay API"
kubectl apply -f deploy/replay-api-deployment.yaml

if [[ "${WITH_KAFKA}" == "true" ]]; then
  log "Deploying optional Kafka"
  kubectl apply -f deploy/kafka-deployment.yaml
else
  log "Skipping Kafka deployment (use --with-kafka to enable)"
fi

log "Waiting for Postgres rollout"
kubectl rollout status deployment/postgres --timeout=180s

log "Waiting for replay-api rollout"
kubectl rollout status deployment/replay-api --timeout=180s

if [[ "${WITH_KAFKA}" == "true" ]]; then
  log "Waiting for Kafka rollout"
  kubectl rollout status deployment/kafka --timeout=180s
fi

log "Starting temporary port-forward on localhost:${API_PORT}"
kubectl port-forward svc/replay-api "${API_PORT}:80" >/tmp/replay-api-port-forward.log 2>&1 &
port_forward_pid=$!

if ! wait_for_port "${API_PORT}"; then
  echo "Port-forward did not become ready; see /tmp/replay-api-port-forward.log" >&2
  exit 1
fi

log "Running health check"
health_response="$(curl -fsS "http://127.0.0.1:${API_PORT}/health")"
printf 'Health response: %s\n' "${health_response}"

log "Deployment complete"
cat <<EOF
Next steps:
  - API:      kubectl port-forward svc/replay-api 8080:80
  - Pods:     kubectl get pods
  - Jobs API: curl -s http://127.0.0.1:${API_PORT}/api/v1/replay/jobs
  - Demo:     docs/DEMO-WALKTHROUGH.md

Notes:
  - This script uses IMAGE_NAME=${IMAGE_NAME}
  - Kafka deployment is ${WITH_KAFKA}
EOF
