package com.example.replay.actors.messages;

import java.util.List;
import java.util.Map;

/**
 * Messages for {@link com.example.replay.actors.DataReaderActor}.
 */
public final class DataReaderMessages {

    private DataReaderMessages() {
    }

    /** Start reading with given config (e.g. source, topic). */
    public record StartReading(Map<String, Object> config) {
    }

    /** Pause reading. */
    public record PauseReading() {
    }

    /** Resume reading. */
    public record ResumeReading() {
    }

    /** Stop reading. */
    public record StopReading() {
    }

    /** Internal: trigger reading the next batch (sent by reader to self for streaming). */
    public record ReadNextBatch() {
    }

    /** Result: batch of records read (payload is opaque for the pipeline). */
    public record BatchRead(String jobId, List<Object> records, boolean lastBatch) {
    }
}
