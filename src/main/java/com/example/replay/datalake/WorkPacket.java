package com.example.replay.datalake;

import java.util.Objects;

/**
 * Unit of work for a single worker: one partition (and optional sub-range for simulated sources).
 * Sized by estimated event count for balanced distribution.
 */
public final class WorkPacket {

    private final String partitionId;
    private final Long startOffset;
    private final Long endOffset;
    private final long estimatedEventCount;

    public WorkPacket(String partitionId, Long startOffset, Long endOffset, long estimatedEventCount) {
        this.partitionId = partitionId != null ? partitionId : "";
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.estimatedEventCount = Math.max(0, estimatedEventCount);
    }

    /** Single partition (e.g. Parquet day partition). */
    public static WorkPacket partition(String partitionId, long estimatedEventCount) {
        return new WorkPacket(partitionId, null, null, estimatedEventCount);
    }

    /** Sub-range for simulated/streaming source (inclusive start, exclusive end). */
    public static WorkPacket range(String partitionId, long startOffset, long endOffset) {
        return new WorkPacket(partitionId, startOffset, endOffset, Math.max(0, endOffset - startOffset));
    }

    public String getPartitionId() {
        return partitionId;
    }

    public Long getStartOffset() {
        return startOffset;
    }

    public Long getEndOffset() {
        return endOffset;
    }

    public long getEstimatedEventCount() {
        return estimatedEventCount;
    }

    public boolean isRange() {
        return startOffset != null && endOffset != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkPacket that = (WorkPacket) o;
        return estimatedEventCount == that.estimatedEventCount
                && Objects.equals(partitionId, that.partitionId)
                && Objects.equals(startOffset, that.startOffset)
                && Objects.equals(endOffset, that.endOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId, startOffset, endOffset, estimatedEventCount);
    }
}
