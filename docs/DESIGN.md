# System Design Document

## 1. Purpose and Scope

This document describes the design of the replay / disaster-recovery system implemented in this repository. The system accepts replay jobs through a REST API, reads events from a simulated or Parquet-backed source, partitions and distributes work when needed, and emits events to Kafka or REST targets while tracking lifecycle state, progress, and runtime metrics.

The design goals are:

- Keep the control plane simple and observable.
- Support both single-threaded and partition-aware distributed replay.
- Handle skewed input data without forcing every job into the same execution model.
- Allow local development and demonstration with minimal dependencies.
- Provide a migration path toward production-hardening without changing the core control model.

---

## 2. Architecture Overview

### 2.1 High-level architecture

```text
                        +--------------------------------------+
                        |          MiniHttpServer              |
                        |  /health, /jobs, /status, /metrics   |
                        +-------------------+------------------+
                                            |
                                            v
                        +--------------------------------------+
                        |            JobManager                |
                        | creates / routes one actor per job   |
                        +-------------------+------------------+
                                            |
                                            v
                  +------------------------------------------------------+
                  |                 ReplayJobActor                       |
                  | status, parameters, progress, metrics, lifecycle     |
                  +-------------+-------------------+---------------------+
                                |                   |
                                |                   |
                    single-path |                   | partition-aware path
                                |                   |
                                v                   v
                      +----------------+   +----------------------+
                      | DataReaderActor|   | WorkDistributorActor |
                      +--------+-------+   +----------+-----------+
                               |                      |
                               |                      v
                               |             +----------------------+
                               |             | WorkPacketWorkerActor|
                               |             |        (N)           |
                               |             +----------+-----------+
                               |                        |
                               +-----------+------------+
                                           |
                                           v
                                  +-------------------+
                                  | DataEmitterActor  |
                                  | Kafka / REST sink |
                                  +---------+---------+
                                            |
                                            v
                           +--------------------------------------+
                           | KafkaEventDestination / Rest...      |
                           +--------------------------------------+

                          +---------------------------------------+
                          | ReplayJobRepository                   |
                          | PostgreSQL + Flyway or in-memory      |
                          +---------------------------------------+
```

### 2.2 Component descriptions and rationale

`MiniHttpServer`

- Chosen as a lightweight raw-socket HTTP server to keep the service footprint small and the code path explicit.
- It exposes only the control-plane endpoints needed for replay management and monitoring.
- This was preferred over a larger web framework because the service is actor-driven and the API surface is intentionally small.

`JobManager`

- Root actor responsible for job creation and lifecycle routing.
- One `ReplayJobActor` is created per job, which isolates job state and makes concurrency easier to reason about.
- This design avoids a single mutable job registry object with shared locking.

`ReplayJobActor`

- Central coordinator for one replay job.
- Stores job parameters, lifecycle state, progress, and aggregated metrics.
- Chooses the execution model:
  - single reader path for simple jobs
  - partition-aware distributed path for larger or skew-aware jobs
- This separation keeps the external lifecycle model stable while allowing different internal execution strategies.

`DataReaderActor`

- Used for the simplest case: one source, one reader, sequential batches.
- Lower coordination overhead than the distributed path.
- Appropriate for small jobs, local demos, and cases where partitioning is unnecessary.

`WorkDistributorActor` and `WorkPacketWorkerActor`

- Used when partition-aware execution is enabled.
- `WorkDistributorActor` creates work packets from partition metadata and assigns them largest-first to a worker pool.
- `WorkPacketWorkerActor` processes a packet independently.
- This design addresses skew and uneven partition sizes without requiring a full distributed compute framework.

`DataEmitterActor`

- Separates reading from downstream delivery.
- Encapsulates destination-specific latency and failure reporting.
- Supports a single metrics aggregation point through emitted acknowledgements.

`ReplayJobRepository`

- Stores durable job metadata when PostgreSQL is configured.
- Falls back to in-memory storage when DB configuration is unavailable or unsupported.
- This makes the system usable in both production-like and demo environments.

### 2.3 Data flow through the system

1. A client creates a job with `POST /api/v1/replay/jobs`.
2. The job definition is validated and persisted.
3. `JobManager` creates a `ReplayJobActor` and sends it a run/start command.
4. `ReplayJobActor` configures the destination and selects the execution path:
   - `DataReaderActor` for single-stream replay
   - `WorkDistributorActor` plus worker actors for partition-aware replay
5. Source events are read in batches.
6. `DataEmitterActor` sends batches to Kafka or REST.
7. Emission results are fed back into `ReplayJobActor` to update:
   - `events_processed`
   - average latency
   - error count
   - timestamps
8. Clients retrieve state via `/status`, `/metrics`, `/jobs`, and lifecycle endpoints.

### 2.4 Technology stack justification

- **Java 17**: stable LTS runtime with strong library compatibility.
- **Apache Pekko classic actors**: a good fit for independent, message-driven job coordination and lifecycle handling.
- **Jackson**: straightforward JSON serialization and Java time support.
- **PostgreSQL**: simple, durable job metadata persistence with broad operational support.
- **Flyway**: lightweight schema migration tool.
- **Kafka client**: direct producer integration for target emission.
- **Parquet / Hadoop libraries**: enough to support partitioned local-file replay and generated demo data.
- **Docker + Kubernetes**: required deployment targets and good alignment with isolated components.

---

## 3. API Design

### 3.1 API surface

The control plane is intentionally small and centered around job lifecycle and observability.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Liveness / readiness check |
| `POST` | `/api/v1/replay/jobs` | Create replay job |
| `GET` | `/api/v1/replay/jobs` | List jobs |
| `GET` | `/api/v1/replay/jobs/{jobId}` | Get job definition and current stored state |
| `GET` | `/api/v1/replay/jobs/{jobId}/status` | Get current lifecycle state and progress |
| `GET` | `/api/v1/replay/jobs/{jobId}/metrics` | Get throughput, latency, and error metrics |
| `POST` | `/api/v1/replay/jobs/{jobId}/start` | Start job |
| `POST` | `/api/v1/replay/jobs/{jobId}/pause` | Pause job |
| `POST` | `/api/v1/replay/jobs/{jobId}/resume` | Resume paused job |
| `POST` | `/api/v1/replay/jobs/{jobId}/cancel` | Cancel job |

### 3.2 Core request and response schemas

Create job request:

```json
{
  "name": "replay-demo",
  "parameters": {
    "source": "simulated",
    "total_count": 1000,
    "batch_size": 100,
    "destination": "rest"
  }
}
```

Create job response:

```json
{
  "job_id": "8a1f3d65-77c6-4de2-a0f0-3fc913f1fd11",
  "status": "PENDING"
}
```

Job resource:

```json
{
  "job_id": "8a1f3d65-77c6-4de2-a0f0-3fc913f1fd11",
  "status": "RUNNING",
  "parameters": {
    "name": "replay-demo",
    "source": "simulated",
    "total_count": 1000,
    "batch_size": 100
  },
  "created_at": "2026-03-10T12:00:00Z",
  "updated_at": "2026-03-10T12:01:00Z",
  "message": null
}
```

Status response:

```json
{
  "job_id": "8a1f3d65-77c6-4de2-a0f0-3fc913f1fd11",
  "status": "RUNNING",
  "progress": {
    "events_processed": 700,
    "started_at": "2026-03-10T12:00:10Z",
    "last_activity_at": "2026-03-10T12:00:45Z"
  }
}
```

Metrics response:

```json
{
  "job_id": "8a1f3d65-77c6-4de2-a0f0-3fc913f1fd11",
  "status": "RUNNING",
  "metrics": {
    "events_per_second": 450.0,
    "latency_ms_avg": 12.5,
    "error_count": 0
  }
}
```

Lifecycle response:

```json
{
  "job_id": "8a1f3d65-77c6-4de2-a0f0-3fc913f1fd11",
  "command": "pause",
  "status": "accepted"
}
```

Validation / error response:

```json
{
  "errors": [
    "parameters must contain mandatory field: source"
  ]
}
```

### 3.3 Job lifecycle management

The lifecycle model is:

- `PENDING`
- `RUNNING`
- `PAUSED`
- `COMPLETED`
- `FAILED`
- `CANCELLED`

Lifecycle transitions are handled by `ReplayJobActor`, which forwards pause/resume/cancel to the active execution path:

- single reader path -> `DataReaderActor`
- partition-aware path -> `WorkDistributorActor`

This keeps the external API simple and hides the execution strategy from clients.

### 3.4 Job state storage and tracking

State is stored in two layers:

- **Repository state**: durable job record in PostgreSQL, or in-memory if DB is unavailable.
- **Live actor state**: runtime progress, timestamps, throughput, latency, and errors inside `ReplayJobActor`.

This split was chosen because persistence and runtime telemetry have different update frequencies and consistency needs. The repository gives durable control-plane state; the actor keeps fast-changing execution state without turning the database into a metrics store.

### 3.5 Error handling approach

- Validation failures return `400`.
- Unknown job IDs return `404`.
- Lifecycle requests return `200` with `accepted` when routing succeeds.
- Downstream emission failures increment error counters and keep metrics accurate.
- PostgreSQL / Flyway initialization failures now fall back to in-memory storage rather than crashing startup.

This approach favors operational continuity over strict failure propagation during bootstrap.

### 3.6 API design choices and rationale

- A job-oriented API was chosen over per-record replay APIs because replay is naturally a long-running asynchronous operation.
- Separate `/status` and `/metrics` endpoints avoid overloading a single job response with frequently changing telemetry.
- Lifecycle endpoints use simple command-style POSTs because the actor model already treats them as explicit messages.
- The API surface is intentionally narrow to keep the control plane easy to test and document.

---

## 4. Data Strategy

### 4.1 Source partitioning strategy

The source side supports two broad modes:

- **single-path reading** for small or simple jobs
- **partition-aware distribution** for larger, skewed, or parallel jobs

Partitioning inputs are represented as `PartitionInfo` with:

- partition identifier
- estimated event count

### 4.2 How the data lake is partitioned

For Parquet-backed inputs, the current design assumes partitions such as:

- `day=yyyy-MM-dd`

This is a practical default because:

- replay workloads are often time-window based
- time partitioning aligns with operational slicing and file organization
- it provides a good unit for parallelism without requiring global indexing

For simulated test data, two virtual partitioning strategies are supported:

- by day
- by customer skew (`heavy`, `medium`, `light`)

### 4.3 Why this approach was chosen

Time-based partitions are simple to reason about and easy to map onto replay windows. Virtual skew partitions were added because real replay workloads are rarely uniform, and the design needed a way to exercise hot-tenant behavior without a full external dataset.

### 4.4 Trade-offs

Advantages:

- simple partition discovery
- predictable operational slicing
- compatible with largest-first packet scheduling

Trade-offs:

- time partitions alone do not eliminate skew inside a partition
- file size is only an estimate of event count
- a single hot day can still dominate one worker unless split further

### 4.5 Target partitioning strategy

#### Kafka

- Partition key: `cid`
- Reasoning:
  - preserves per-customer ordering
  - keeps replay semantics stable for downstream consumers
  - aligns naturally with skew testing and hot-customer behavior

Partition count is deployment-specific. The current code assumes the topic exists and uses the broker's partitioning model. In production, topic partition count should be chosen based on:

- expected concurrent hot customers
- downstream consumer parallelism
- acceptable partition hot-spot risk

#### REST

- Delivery is batch-oriented.
- Batches reduce per-request overhead and let the emitter report coarse throughput and latency.
- Routing is controlled by the configured `rest_url`; when omitted or set to `http://simulate`, an in-memory simulated destination is used for tests.

Trade-offs:

- larger batches improve throughput but increase per-batch latency and retry cost
- smaller batches improve fairness and responsiveness but increase HTTP overhead

### 4.6 Test data characteristics

The system includes a generator for 50k+ security events written to Parquet. The generated test data has:

- time distribution across multiple days
- strong customer skew
- realistic core fields:
  - `cid`
  - `event_timestamp`
  - `event_time`
  - `event_type`
  - `event_id`

Skew model:

- first 5 customers -> about 65%
- next 10 customers -> about 25%
- remainder -> about 10%

This was chosen to reproduce common operational pain points:

- hot partitions
- uneven worker utilization
- Kafka key concentration

### 4.7 Additional fields and why

The current core event model stays intentionally minimal to keep replay behavior easy to verify. The design supports adding richer fields later such as:

- source system id
- tenant region
- replay metadata
- correlation ids

These are omitted from the current baseline to reduce noise in the control-plane implementation.

---

## 5. Key Design Decisions

### 5.1 Handling data skew

Data skew is handled through a combination of:

- partition metadata with estimated sizes
- optional skew-aware virtual partitions
- largest-first packet ordering
- configurable worker count

This does not completely eliminate skew, but it makes it explicit and testable.

### 5.2 Efficient work distribution

Work is distributed as `WorkPacket` units rather than one actor per raw event. This reduces coordination overhead and allows the system to:

- reuse partition-level metadata
- split large simulated partitions into smaller ranges
- keep assignment policy simple

The chosen policy is largest-first round-robin. It is not globally optimal, but it is simple, deterministic, and performs well enough for this class of workload.

### 5.3 Failures and job recovery

Failure handling today is pragmatic rather than fully durable:

- emission failures are counted and exposed via metrics
- lifecycle commands are accepted asynchronously
- bootstrap DB failures fall back to in-memory mode
- job state is persisted when PostgreSQL is available

Recovery is currently focused on operational continuity, not exact resume-from-offset semantics.

### 5.4 Managing concurrent jobs

Concurrency is managed by the actor model:

- one actor per job
- per-job internal actors for reading/distribution/emission
- no shared mutable execution state across jobs

This reduces lock contention and keeps job interactions mostly independent. The main trade-off is that status may be briefly asynchronous relative to in-flight work.

### 5.5 Trade-offs made

- Chose a lightweight raw-socket HTTP server over a larger framework to reduce surface area.
- Chose actor-local metrics over a DB-centric progress model for lower write overhead.
- Chose simple partition heuristics over expensive global planning.
- Chose fallback-to-memory on DB init failure to favor availability in demos and local environments.

---

## 6. Limitations and Future Work

### 6.1 What is missing for production

- stronger authentication / authorization for control-plane APIs
- durable resume checkpoints at source offsets or partition positions
- explicit idempotency and replay deduplication strategy
- richer audit logs and tracing
- better Kafka deployment guidance for Kubernetes
- stronger database compatibility management and startup validation
- multi-replica API coordination strategy

### 6.2 Improvements with more time

- move from raw sockets to a production-grade HTTP framework
- add persistent checkpointing for true pause/resume/cancel recovery
- add backpressure-aware destination handling
- add better autoscaling and per-job resource governance
- support richer datalake partition discovery and catalog integration
- formalize load testing and chaos testing harnesses

### 6.3 Known limitations

- current PostgreSQL/Flyway integration is not yet hardened across all Postgres versions
- Kafka-in-cluster setup is intentionally basic and currently optional
- progress and metrics are eventually consistent with in-flight actor messages
- partition estimates are heuristic, especially for Parquet
- the current system is optimized for clarity and demonstration, not maximum production throughput

---

## 7. Summary

The system is designed around a simple control plane and an actor-based execution core. The design deliberately separates:

- job lifecycle control
- data reading and partitioning
- downstream emission
- durable metadata persistence

This keeps the replay workflow understandable, supports both simple and skew-aware execution, and provides a strong base for future hardening without discarding the current architecture.
