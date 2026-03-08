package com.example.replay.actors.messages;

import java.util.List;

/**
 * Messages for {@link com.example.replay.actors.DataEmitterActor}.
 */
public final class DataEmitterMessages {

    private DataEmitterMessages() {
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
