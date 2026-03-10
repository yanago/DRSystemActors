# Containerization and Kubernetes (minikube / kind / k3s)

This document describes how to build Docker images for the Replay API server, run the stack on **minikube**, **kind**, **k3s**, or similar Kubernetes clusters, and test end-to-end locally.

## Requirements compliance

- **Must run on Kubernetes (minikube, kind, k3s, or similar)** — Yes. All runtime components are deployed via standard Kubernetes manifests in `deploy/`. The same manifests work on minikube, kind, k3s, and other Kubernetes clusters.
- **All components must be containerized** — Yes.
  - **Replay API**: Custom image built from `Dockerfile` (tag `replay-api:latest`), run as a Deployment.
  - **PostgreSQL**: Containerized via official image `postgres:16-alpine` (Deployment + PVC).
  - **Kafka** (optional): Containerized via `bitnami/kafka:3.7` (Deployment). Omit if using an external Kafka.

## Prerequisites

- Docker (for building images and for minikube/kind/k3s)
- kubectl
- **minikube**, **kind**, or **k3s** (or another Kubernetes cluster)
- (Optional) kustomize

## 1. Build the API server image

From the project root:

```bash
mvn -q package -DskipTests
docker build -t replay-api:latest .
```

The Dockerfile uses a multi-stage build: Maven produces a fat JAR, then a minimal JRE image runs it. The server listens on port **8080** and uses `REPLAY_JDBC_*` env vars for PostgreSQL (see below).

## 2. Run on minikube

### Start minikube and use its Docker daemon

```bash
minikube start
eval $(minikube docker-env)
```

So that `docker build` goes into minikube’s image store.

### Build image inside minikube

```bash
docker build -t replay-api:latest .
```

### Apply Kubernetes manifests

Apply in order (Postgres depends on the secret; API depends on Postgres):

```bash
kubectl apply -f deploy/secret.yaml
kubectl apply -f deploy/postgres-pvc.yaml
kubectl apply -f deploy/postgres-deployment.yaml
kubectl apply -f deploy/postgres-service.yaml
kubectl apply -f deploy/replay-api-deployment.yaml
# Optional: in-cluster Kafka
kubectl apply -f deploy/kafka-deployment.yaml
```

Or apply the whole directory (order may matter; if something fails, apply secret first then re-run):

```bash
kubectl apply -f deploy/
```

### Wait for pods

```bash
kubectl get pods -w
```

Wait until `postgres-*` and `replay-api-*` are `Running` and ready (and optionally `kafka-*` if you applied it).

### Expose and test the API

Port-forward the API service:

```bash
kubectl port-forward svc/replay-api 8080:80
```

In another terminal:

```bash
curl -s http://localhost:8080/health
# Expected: OK

curl -s -X POST http://localhost:8080/api/v1/replay/jobs \
  -H "Content-Type: application/json" \
  -d '{"name":"e2e","source":"simulated","parameters":{"total_count":100,"batch_size":10}}'
# Expected: JSON with job_id

curl -s http://localhost:8080/api/v1/replay/jobs
# Expected: JSON list including the created job
```

### End-to-end test (create job, start, check status)

```bash
# Create job
JOB_ID=$(curl -s -X POST http://localhost:8080/api/v1/replay/jobs \
  -H "Content-Type: application/json" \
  -d '{"name":"e2e","source":"simulated","parameters":{"total_count":50,"batch_size":25}}' | jq -r '.job_id')

# Start job
curl -s -X POST "http://localhost:8080/api/v1/replay/jobs/${JOB_ID}/start" -H "Content-Type: application/json" -d '{}'

# Wait a moment, then check status and metrics
sleep 3
curl -s "http://localhost:8080/api/v1/replay/jobs/${JOB_ID}/status"
curl -s "http://localhost:8080/api/v1/replay/jobs/${JOB_ID}/metrics"
```

## 3. Run on kind

### Create cluster and load image

```bash
kind create cluster --name replay
docker build -t replay-api:latest .
kind load docker-image replay-api:latest --name replay
```

### Apply manifests and test

Same as minikube:

```bash
kubectl apply -f deploy/secret.yaml
kubectl apply -f deploy/postgres-pvc.yaml
kubectl apply -f deploy/postgres-deployment.yaml
kubectl apply -f deploy/postgres-service.yaml
kubectl apply -f deploy/replay-api-deployment.yaml
kubectl apply -f deploy/kafka-deployment.yaml   # optional

kubectl get pods -w
# When ready:
kubectl port-forward svc/replay-api 8080:80
# Then run the same curl commands as above.
```

## 4. Run on k3s (or similar)

k3s and other standard Kubernetes clusters use the same manifests. Build the API image, load or push it so the cluster can pull it, then apply:

```bash
# If using k3d (k3s in Docker): create cluster and load image
k3d cluster create replay
docker build -t replay-api:latest .
k3d image import replay-api:latest -c replay

# Or with plain k3s: build and push to a registry the cluster can pull from,
# then set image in deploy/replay-api-deployment.yaml to your registry URL.

kubectl apply -f deploy/
kubectl get pods -w
kubectl port-forward svc/replay-api 8080:80
# Then run the same curl commands as in section 2.
```

## 5. Configuration

### API server environment

| Variable | Description |
|----------|-------------|
| `REPLAY_JDBC_URL` | JDBC URL (e.g. `jdbc:postgresql://postgres:5432/replay`) |
| `REPLAY_JDBC_USER` | DB user |
| `REPLAY_JDBC_PASSWORD` | DB password |

These are provided via the `replay-api-secret` Secret in `deploy/secret.yaml`. The API uses them for Flyway migrations and HikariCP; if `REPLAY_JDBC_URL` is not set, the server falls back to in-memory job storage.

### Postgres

- **Database name:** `replay` (set in `postgres-deployment.yaml` via `POSTGRES_DB`).
- **User/password:** from Secret `replay-db-secret` (same values as in `replay-api-secret` for local dev).
- **Storage:** PVC `postgres-pvc` (2Gi).

### Kafka (optional)

- Service DNS name: `kafka:9092`. In job parameters use `kafka_bootstrap_servers: "kafka:9092"` and `destination: "kafka"` when using the in-cluster Kafka.
- Omit `deploy/kafka-deployment.yaml` if you use an external Kafka; set `kafka_bootstrap_servers` in job params to your external broker list.

## 6. Resource limits (summary)

- **replay-api:** requests 256Mi/100m CPU, limits 512Mi/1000m; liveness/readiness on `/health`.
- **postgres:** requests 128Mi/100m, limits 512Mi/500m.
- **kafka:** requests 256Mi/100m, limits 512Mi/500m.

## 7. Teardown

**minikube:**

```bash
kubectl delete -f deploy/
minikube stop
```

**kind:**

```bash
kubectl delete -f deploy/
kind delete cluster --name replay
```

## 8. Troubleshooting

- **ImagePullBackOff for replay-api:** Build the image inside the cluster’s Docker (e.g. `eval $(minikube docker-env)` then `docker build -t replay-api:latest .`) or push to a registry and set `imagePullPolicy` / image name in `replay-api-deployment.yaml`.
- **Postgres not ready:** Ensure `replay-db-secret` exists and Postgres pod has the PVC bound (`kubectl get pvc`).
- **API 503 / not ready:** Check that `REPLAY_JDBC_URL` points to the Postgres service (`postgres:5432`) and that Postgres is running; check API pod logs: `kubectl logs -l app=replay-api`.
- **Kafka connection from API:** Use job parameters `kafka_bootstrap_servers: "kafka:9092"` and `destination: "kafka"`; ensure the Kafka deployment is running if you use in-cluster Kafka.
