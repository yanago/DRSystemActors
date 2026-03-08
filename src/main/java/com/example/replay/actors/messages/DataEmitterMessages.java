package com.example.replay.actors.messages;

import java.util.List;
import java.util.Map;

/**
 * Messages for {@link com.example.replay.actors.DataEmitterActor}.
 */
public final class DataEmitterMessages {

    private DataEmitterMessages() {
    }

    /** Configure destination (Kafka or REST) from job parameters. Sent before first EmitBatch. */
    public record ConfigureDestination(Map<String, Object> config) {
    }

    /** Emit a batch of records. */
    public record EmitBatch(String jobId, List<Object> records) {
    }

    /** Stop emitting (shutdown). */
    public record StopEmitting() {
    }

    /** Acknowledgement that batch was emitted (count, latencyMs for metrics). */
    public record BatchEmitted(String jobId, int count, long latencyMs) {
        public BatchEmitted(String jobId, int count) {
            this(jobId, count, 0L);
        }
    }

    /** Batch send failed; count events that failed for error metrics. */
    public record BatchEmitFailed(String jobId, int failedCount) {
    }
}
