# Kubernetes manifests

Apply order (if applying individually):

1. `secret.yaml` – DB credentials and API env (required by Postgres and API)
2. `postgres-pvc.yaml`
3. `postgres-deployment.yaml`
4. `postgres-service.yaml`
5. `replay-api-deployment.yaml` – expects image `replay-api:latest` (build and load into cluster)
6. `kafka-deployment.yaml` – optional, for in-cluster Kafka
7. `configmap.yaml` – optional

Or apply all at once (secret must exist for Postgres/API to start):

```bash
kubectl apply -f deploy/
```

See [docs/CONTAINERIZATION.md](../docs/CONTAINERIZATION.md) for minikube/kind and end-to-end testing.
