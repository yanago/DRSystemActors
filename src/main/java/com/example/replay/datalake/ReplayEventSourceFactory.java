package com.example.replay.datalake;

import java.util.Map;

/**
 * Creates a ReplayEventSource from job config (source_type, path, batch_size, partition_day, etc.).
 */
public final class ReplayEventSourceFactory {

    private ReplayEventSourceFactory() {
    }

    /**
     * Creates a source from config. source_type: "simulated" (default) or "parquet".
     * Simulated: batch_size, total_count, cid_prefix.
     * Parquet: parquet_path, batch_size, partition_day (optional).
     */
    public static ReplayEventSource create(Map<String, Object> config) {
        Map<String, Object> cfg = config != null ? config : Map.of();
        String sourceType = cfg.containsKey(EventBatch.SOURCE_TYPE_KEY)
                ? String.valueOf(cfg.get(EventBatch.SOURCE_TYPE_KEY)).toLowerCase()
                : EventBatch.SOURCE_TYPE_SIMULATED;
        if (EventBatch.SOURCE_TYPE_PARQUET.equals(sourceType)) {
            try {
                return ParquetEventReader.fromConfig(cfg);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create Parquet source: " + e.getMessage(), e);
            }
        }
        return SimulatedEventCatalog.fromConfig(cfg);
    }
}
