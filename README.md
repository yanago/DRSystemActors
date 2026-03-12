# DRSystemActors

Replay/DR API and job orchestration (Apache Pekko, Java 17).

Supported replay inputs today include simulated data, partitioned Parquet files, and Hadoop-table Apache Iceberg tables.

## Documentation

- **[OpenAPI spec](openapi.yaml)** — REST API: health, jobs CRUD, lifecycle (start/pause/resume/cancel), status, metrics.
- **[System design](docs/DESIGN.md)** — Architecture overview, API design, data strategy, key trade-offs, and future work.
- **[Architecture & partitioning](docs/ARCHITECTURE.md)** — High-level design, actor hierarchy, partitioning strategy, job parameters.
- **[20-minute demo](docs/DEMO-WALKTHROUGH.md)** — Step-by-step walkthrough: run server, create/start jobs, status/metrics, pause/resume/cancel, partitioning.
- **[Containerization](docs/CONTAINERIZATION.md)** — Docker and Kubernetes (minikube/kind), end-to-end testing.

## Quick start

```bash
mvn -q compile exec:java -Dexec.mainClass=com.example.replay.rest.MiniHttpServer
curl -s http://localhost:8080/health
curl -s -X POST http://localhost:8080/api/v1/replay/jobs -H "Content-Type: application/json" \
  -d '{"name":"demo","parameters":{"source":"simulated","total_count":100,"batch_size":20}}'
```
