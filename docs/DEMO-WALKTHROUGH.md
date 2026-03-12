# 20-Minute Demo Walkthrough

This guide walks through the Replay/DR API and features in about **20 minutes**, suitable for a live demo or self-run.

**Prerequisites:** Java 17+, Maven. Optional: PostgreSQL (or use in-memory storage), Kafka (or use REST/simulated destination).

---

## Minute 0–2: Intro and start the server

1. **Show the project**
   - OpenAPI: `openapi.yaml` (all REST endpoints).
   - Architecture: `docs/ARCHITECTURE.md` (actors, partitioning, data flow).

2. **Start the API server** (in-memory storage, no DB required)
   ```bash
   mvn -q compile exec:java -Dexec.mainClass=com.example.replay.rest.MiniHttpServer
   ```
   Or with explicit port:
   ```bash
   mvn -q compile exec:java -Dexec.mainClass=com.example.replay.rest.MiniHttpServer -Dexec.args="8080"
   ```
   Expected: “Health check server listening on http://localhost:8080/health” and “Job storage: in-memory”.

3. **Health check**
   ```bash
   curl -s http://localhost:8080/health
   ```
   Expected: `OK`

---

## Minute 2–6: Create and inspect jobs

4. **Create a first job** (simulated source, small count)
   ```bash
   curl -s -X POST http://localhost:8080/api/v1/replay/jobs \
     -H "Content-Type: application/json" \
     -d '{"name":"demo-1","parameters":{"source":"simulated","total_count":100,"batch_size":20}}'
   ```
   Expected: `{"job_id":"<uuid>","status":"PENDING"}`. Copy `job_id` for next steps.

5. **List all jobs**
   ```bash
   curl -s http://localhost:8080/api/v1/replay/jobs | jq .
   ```
   Expected: JSON array with one job (job_id, status, parameters, created_at, etc.).

6. **Get one job by ID**
   ```bash
   export JOB_ID=<paste job_id from step 4>
   curl -s "http://localhost:8080/api/v1/replay/jobs/$JOB_ID" | jq .
   ```

7. **Create a second job** (with destination and more events)
   ```bash
   curl -s -X POST http://localhost:8080/api/v1/replay/jobs \
     -H "Content-Type: application/json" \
     -d '{"name":"demo-2","parameters":{"source":"simulated","total_count":500,"batch_size":50,"destination":"rest"}}'
   ```
   Explain: `destination: rest` with no URL uses the in-memory simulated REST endpoint.

---

## Minute 6–10: Start job and watch progress/metrics

8. **Start the first job**
   ```bash
   curl -s -X POST "http://localhost:8080/api/v1/replay/jobs/$JOB_ID/start" -H "Content-Type: application/json" -d '{}'
   ```
   Expected: `{"job_id":"...","command":"start","status":"accepted"}`.

9. **Poll status** (run a few times)
   ```bash
   curl -s "http://localhost:8080/api/v1/replay/jobs/$JOB_ID/status" | jq .
   ```
   Show: `status` (RUNNING → COMPLETED), `progress.events_processed`, `started_at`, `last_activity_at`.

10. **Get metrics**
    ```bash
    curl -s "http://localhost:8080/api/v1/replay/jobs/$JOB_ID/metrics" | jq .
    ```
    Show: `events_per_second`, `latency_ms_avg`, `error_count`.

11. **Create and start a larger job** (so it stays RUNNING for pause demo)
    ```bash
    export JOB2=$(curl -s -X POST http://localhost:8080/api/v1/replay/jobs \
      -H "Content-Type: application/json" \
      -d '{"name":"demo-pause","parameters":{"source":"simulated","total_count":5000,"batch_size":200}}' | jq -r '.job_id')
    curl -s -X POST "http://localhost:8080/api/v1/replay/jobs/$JOB2/start" -H "Content-Type: application/json" -d '{}'
    ```

---

## Minute 10–14: Pause, resume, cancel

12. **Pause the running job**
    ```bash
    curl -s -X POST "http://localhost:8080/api/v1/replay/jobs/$JOB2/pause" -H "Content-Type: application/json" -d '{}'
    curl -s "http://localhost:8080/api/v1/replay/jobs/$JOB2/status" | jq .
    ```
    Show: `status: PAUSED`, `progress.events_processed` frozen.

13. **Resume**
    ```bash
    curl -s -X POST "http://localhost:8080/api/v1/replay/jobs/$JOB2/resume" -H "Content-Type: application/json" -d '{}'
    sleep 2
    curl -s "http://localhost:8080/api/v1/replay/jobs/$JOB2/status" | jq .
    ```
    Show: `status: RUNNING`, progress increasing again.

14. **Cancel a job** (use the same job or create a new one)
    ```bash
    curl -s -X POST "http://localhost:8080/api/v1/replay/jobs/$JOB2/cancel" -H "Content-Type: application/json" -d '{}'
    curl -s "http://localhost:8080/api/v1/replay/jobs/$JOB2/status" | jq .
    ```
    Show: `status: CANCELLED` (or COMPLETED if it finished before cancel).

---

## Minute 14–18: Partitioning and destinations (optional depth)

15. **Partition-aware job** (multiple workers, simulated day partitions)
    ```bash
    curl -s -X POST http://localhost:8080/api/v1/replay/jobs \
      -H "Content-Type: application/json" \
      -d '{"name":"partition-demo","parameters":{"source":"simulated","total_count":2000,"batch_size":100,"partition_aware":true,"worker_count":3}}'
    ```
    Start it and show status/metrics. Mention: WorkDistributor creates work packets (e.g. by day or skew), assigns to N workers, balances by partition size.

16. **Kafka vs REST**
    - **REST**: `destination: rest`, optional `rest_url` (omit or `http://simulate` for in-memory).
    - **Kafka**: `destination: kafka`, `kafka_topic`, `kafka_bootstrap_servers`. Events are partitioned by `cid` so per-customer order is preserved.

17. **Optional Iceberg-backed replay**
    ```bash
    curl -s -X POST http://localhost:8080/api/v1/replay/jobs \
      -H "Content-Type: application/json" \
      -d '{"name":"iceberg-demo","parameters":{"source":"iceberg","source_type":"iceberg","iceberg_table_path":"/tmp/iceberg-table","iceberg_partition_field":"day","partition_day":"2025-03-01","batch_size":500,"destination":"rest"}}'
    ```
    Explain: the replay path can now plan files from a Hadoop-table Apache Iceberg table, optionally prune by the configured partition field, and emit records through the same reader/distributor/emitter pipeline.

18. **List jobs again**
    ```bash
    curl -s http://localhost:8080/api/v1/replay/jobs | jq 'length'
    ```
    Show all created jobs and their statuses.

---

## Minute 18–20: Wrap-up

19. **Recap**
    - **REST**: Health, create/list/get jobs, lifecycle (start/pause/resume/cancel), status, metrics.
    - **Architecture**: JobManager → ReplayJobActor → DataReader or WorkDistributor + DataEmitter; optional Postgres and Kafka.
    - **Partitioning**: Partition metadata (Iceberg/Parquet days or simulated day/skew) → work packets (sorted by size) → workers; Kafka output by cid.

20. **Point to more docs**
    - **OpenAPI**: `openapi.yaml` for full API spec.
    - **Architecture**: `docs/ARCHITECTURE.md` for partitioning strategy and diagrams.
    - **Containerization**: `docs/CONTAINERIZATION.md` for Docker and Kubernetes (minikube/kind).

21. **Optional**: Run tests or show Parquet event generator
    ```bash
    mvn -q test -Dtest=MiniHttpServerTest
    # Or generate Parquet data (see README / generate-events profile)
    ```

---

## Quick reference: all endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Liveness/readiness |
| POST | `/api/v1/replay/jobs` | Create job |
| GET | `/api/v1/replay/jobs` | List jobs |
| GET | `/api/v1/replay/jobs/{id}` | Get job |
| GET | `/api/v1/replay/jobs/{id}/status` | Progress + status |
| GET | `/api/v1/replay/jobs/{id}/metrics` | Throughput, latency, errors |
| POST | `/api/v1/replay/jobs/{id}/start` | Start |
| POST | `/api/v1/replay/jobs/{id}/pause` | Pause |
| POST | `/api/v1/replay/jobs/{id}/resume` | Resume |
| POST | `/api/v1/replay/jobs/{id}/cancel` | Cancel |

Use `openapi.yaml` for request/response schemas and examples.
