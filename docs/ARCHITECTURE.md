# High-Level Architecture and Partitioning Strategy

## Overview

The Replay/DR system is a Java application built with **Apache Pekko** (actors and streams). It reads security events from a datalake (Parquet or simulated catalog), optionally partitions work by size for load balancing, and emits events to Kafka or a REST API. Job state is stored in PostgreSQL or in-memory.

---

## High-Level Architecture

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                     REST API (MiniHttpServer)            │
                    │  /health  /api/v1/replay/jobs  /status  /metrics  etc.  │
                    └─────────────────────────────┬───────────────────────────┘
                                                   │
                    ┌──────────────────────────────▼───────────────────────────┐
                    │                      JobManager (actor)                    │
                    │  Creates ReplayJobActor per job, routes lifecycle cmds    │
                    └──────────────────────────────┬───────────────────────────┘
                                                   │
         ┌─────────────────────────────────────────┼─────────────────────────────────────────┐
         │                         ReplayJobActor (per job)                                  │
         │  Status, params, metrics aggregation; chooses reader vs distributor path           │
         └──┬──────────────────────┬──────────────────────┬──────────────────────┬──────────┘
            │                      │                      │                      │
            ▼                      ▼                      ▼                      ▼
   ┌───────────────┐    ┌──────────────────┐   ┌─────────────────┐   ┌─────────────────────┐
   │ DataReader    │    │ WorkDistributor  │   │ DataEmitter     │   │ ReplayJobRepository  │
   │ Actor         │    │ Actor            │   │ Actor           │   │ (Postgres / memory)  │
   │ (single path) │    │ (partition path) │   │ Kafka / REST    │   │                     │
   └───────┬───────┘    └────────┬─────────┘   └────────┬────────┘   └─────────────────────┘
           │                    │                      │
           │                    │  WorkPacketWorker    │
           │                    │  (N workers)        │
           ▼                    ▼                      ▼
   ┌───────────────┐    ┌──────────────────┐   ┌─────────────────┐
   │ ReplayEvent   │    │ ReplayEventSource│   │ EventDestination│
   │ Source        │    │ (per packet)     │   │ (Kafka / REST)  │
   │ (full scan)   │    │ Parquet/Simulated│   │                 │
   └───────────────┘    └──────────────────┘   └─────────────────┘
```

### Components

| Component | Role |
|-----------|------|
| **MiniHttpServer** | Raw-socket HTTP server. Serves health, job CRUD, lifecycle (start/pause/resume/cancel), status, and metrics. |
| **JobManager** | Root Pekko actor. One child `ReplayJobActor` per job; routes `CreateJob`, `GetJobStatus`, `JobLifecycleCommand`. |
| **ReplayJobActor** | Per-job actor. Holds status, parameters, and aggregated metrics. Chooses **single reader** or **partition-aware distributor** based on `partition_aware` or `worker_count > 1`. Creates and supervises DataReader, WorkDistributor, and DataEmitter. |
| **DataReaderActor** | Single-path reader: one source, reads batches, sends to parent which forwards to DataEmitter. Used when not using partition-aware distribution. |
| **WorkDistributorActor** | Partition path: builds work packets from partition metadata, spawns N `WorkPacketWorkerActor`s, assigns packets (largest first, round-robin). Forwards batches to DataEmitter and aggregates BatchEmitted/BatchEmitFailed. |
| **WorkPacketWorkerActor** | Processes one work packet (partition or range). Uses `ReplayEventSourceFactory.createForWorkPacket()` to read only that partition/range; sends batches to distributor → emitter. |
| **DataEmitterActor** | Single emitter per job. Configured with Kafka or REST destination; receives batches, sends via `EventDestination.sendBatch()`, reports BatchEmitted (with latency) or BatchEmitFailed. |
| **ReplayJobRepository** | Persists job state (PostgreSQL with Flyway, or in-memory). |

### Data flow

1. **Create job** (POST `/api/v1/replay/jobs`) → persisted → JobManager creates ReplayJobActor and sends Run.
2. **Run** (or **Start**): ReplayJobActor configures emitter (destination), then either starts DataReader (single path) or WorkDistributor (partition path).
3. **Reader/Distributor** reads events in batches from Parquet or simulated catalog and sends batches to DataEmitter.
4. **DataEmitter** sends each batch to Kafka or REST, then replies BatchEmitted/BatchEmitFailed to the job actor.
5. **ReplayJobActor** aggregates events processed, latency, and errors for status/metrics APIs.

---

## Partitioning Strategy

Partitioning is used to **split work by input partitions** and **balance load across workers** so that no single worker is stuck with one huge partition.

### When partition-aware distribution is used

- Job parameters set **`partition_aware: true`**, or **`worker_count > 1`**.
- Then ReplayJobActor starts **WorkDistributorActor** instead of the single DataReaderActor.

### Partition metadata

- **PartitionMetadataProvider** returns a list of **PartitionInfo** (partition id + estimated event count).
- **Parquet**: `ParquetPartitionMetadata` lists `day=yyyy-MM-dd` (or top-level Parquet files), estimates size from file bytes.
- **Simulated**:
  - **By day**: `SimulatedPartitionMetadata` splits total count into `num_days` partitions (`day-0`, `day-1`, …).
  - **By customer skew**: with `partition_by_skew: true`, three virtual partitions — **heavy** (~65%), **medium** (~25%), **light** (~10%) — so work packets reflect skewed load.

### Work packets

- **WorkPacket** = unit of work: either a full partition (e.g. one Parquet partition) or a **range** (start/end offset) for simulated/streaming.
- **WorkPacketFactory.createPackets(config)**:
  - Gets partitions from the appropriate metadata provider (Parquet or Simulated).
  - Optionally splits large partitions into smaller range-based packets (simulated only, when partition size > `max_packet_size`).
  - **Sorts packets by estimated event count descending** so the largest chunks are assigned first in round-robin.

### Distribution and load balancing

- **WorkDistributorActor** creates a fixed pool of **WorkPacketWorkerActor**s (`worker_count`).
- It assigns one packet at a time to each worker; when a worker finishes (PacketComplete), it gets the next packet from the queue.
- **Largest-first + round-robin** keeps total work per worker roughly balanced even when partition sizes vary (e.g. heavy vs light customer buckets).

### Kafka partitioning (output)

- Events are sent to Kafka with **customer id (cid)** as the partition key (`CustomerPartitionKey.keyFor(record)`), so all events for the same customer go to the same Kafka partition and order is preserved per customer.

---

## Job parameters (summary)

| Parameter | Description |
|-----------|-------------|
| `source` | Required. e.g. `simulated`, `parquet`. |
| `total_count` | Number of events (simulated). |
| `batch_size` | Events per batch. |
| `destination` | `rest` or `kafka`. |
| `partition_aware` | If true, use WorkDistributor path. |
| `worker_count` | Number of workers in partition path (default 4). |
| `partition_by_skew` | Simulated: use heavy/medium/light partitions. |
| `num_days` | Simulated: number of day partitions. |
| `kafka_topic`, `kafka_bootstrap_servers` | For Kafka destination. |
| `rest_url` | For REST destination; blank or `http://simulate` = in-memory. |

---

## Storage and configuration

- **PostgreSQL**: When `REPLAY_JDBC_URL` is set, Flyway runs migrations and job state is persisted. Otherwise in-memory repository is used.
- **Secrets/config**: JDBC URL, user, password via env or system properties; see containerization docs for Kubernetes.
