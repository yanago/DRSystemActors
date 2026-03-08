package com.example.replay.datalake;

/**
 * Metadata for one partition (e.g. day=2025-03-01) used to size work packets.
 */
public record PartitionInfo(String partitionId, long estimatedEventCount) {

    public PartitionInfo {
        partitionId = partitionId != null ? partitionId : "";
        estimatedEventCount = Math.max(0, estimatedEventCount);
    }
}
