package com.example.replay.actors.messages;

import com.example.replay.datalake.WorkPacket;

import java.util.List;
import java.util.Map;

/**
 * Messages for work distribution: coordinator and workers.
 */
public final class WorkDistributorMessages {

    private WorkDistributorMessages() {
    }

    /** Start distribution: build packets from config and assign to workers. */
    public record StartDistribution(Map<String, Object> config, int workerCount) {
    }

    /** Assign one work packet to a worker (jobId + config for creating source). */
    public record AssignPacket(String jobId, WorkPacket packet, Map<String, Object> config) {
    }

    /** Worker finished a packet (and sent all BatchReads). Request next or idle. */
    public record PacketComplete(String workerId) {
    }

    /** Worker reports batch read (forwarded to emitter). */
    public record WorkerBatchRead(String jobId, List<Object> records, boolean lastBatch) {
    }

    /** All work done; notify job actor. */
    public record AllWorkComplete(String jobId) {
    }

    /** Pause / resume / cancel from job actor. */
    public record PauseDistribution() {
    }

    public record ResumeDistribution() {
    }

    public record CancelDistribution() {
    }
}
