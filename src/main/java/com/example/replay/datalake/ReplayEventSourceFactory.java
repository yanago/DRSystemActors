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
        return createForWorkPacket(config != null ? config : Map.of(), null);
    }

    /**
     * Creates a source scoped to a work packet (one partition or one range).
     * When packet is null, creates a full-scan source.
     */
    public static ReplayEventSource createForWorkPacket(Map<String, Object> config, WorkPacket packet) {
        Map<String, Object> cfg = config != null ? config : Map.of();
        String sourceType = cfg.containsKey(EventBatch.SOURCE_TYPE_KEY)
                ? String.valueOf(cfg.get(EventBatch.SOURCE_TYPE_KEY)).toLowerCase()
                : EventBatch.SOURCE_TYPE_SIMULATED;
        if (EventBatch.SOURCE_TYPE_PARQUET.equals(sourceType)) {
            try {
                if (packet != null && !packet.getPartitionId().isEmpty() && !"default".equals(packet.getPartitionId())) {
                    Map<String, Object> scoped = new java.util.HashMap<>(cfg);
                    scoped.put(EventBatch.PARTITION_DAY_KEY, packet.getPartitionId());
                    return ParquetEventReader.fromConfig(scoped);
                }
                return ParquetEventReader.fromConfig(cfg);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create Parquet source: " + e.getMessage(), e);
            }
        }
        if (packet != null && packet.isRange() && packet.getStartOffset() != null && packet.getEndOffset() != null) {
            return SimulatedEventCatalog.fromConfig(cfg, packet.getStartOffset(), packet.getEndOffset());
        }
        return SimulatedEventCatalog.fromConfig(cfg);
    }
}
