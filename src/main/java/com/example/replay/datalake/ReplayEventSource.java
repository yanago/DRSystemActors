package com.example.replay.datalake;

import java.io.Closeable;
import java.util.Map;

/**
 * Source of event batches for replay (Iceberg, Delta Lake, Parquet, or simulated).
 * Supports batch pagination and optional partition filtering (e.g. by day).
 */
public interface ReplayEventSource extends Closeable {

    /**
     * Returns the next batch of events. When no more data, returns a batch with lastBatch=true (possibly empty).
     */
    EventBatch nextBatch();

    /**
     * Whether more batches may be available (optimization; nextBatch() may still return empty with lastBatch=true).
     */
    boolean hasMore();

    @Override
    void close();
}
