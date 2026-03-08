package com.example.replay.api;

import java.util.List;

/**
 * Sends replayed events to a destination (Kafka, REST, etc.).
 */
public interface EventDestination {

    /**
     * Sends a batch of events. Records are opaque (Map or SecurityEvent); implementation
     * must serialize and use customer id (cid) for partitioning where relevant.
     */
    void sendBatch(List<Object> records) throws Exception;

    /**
     * Closes the destination (flush Kafka producer, close HTTP client).
     */
    void close() throws Exception;
}
