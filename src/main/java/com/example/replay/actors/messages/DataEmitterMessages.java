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

    /** Acknowledgement that batch was emitted (optional back-pressure). */
    public record BatchEmitted(String jobId, int count) {
    }
}
