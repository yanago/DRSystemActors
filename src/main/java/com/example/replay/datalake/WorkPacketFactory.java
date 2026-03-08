package com.example.replay.datalake;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Builds work packets from partition metadata, considering partition sizes
 * and optional max packet size for splitting. Sorts by estimated size (largest first)
 * so distributors can assign in round-robin for balance.
 */
public final class WorkPacketFactory {

    /** Max events per packet; partitions larger than this may be split (simulated only). */
    private static final long DEFAULT_MAX_PACKET_SIZE = 50_000L;

    /**
     * Produces work packets from config: uses partition metadata for the source type,
     * optionally splits large partitions, and returns packets sorted by estimated size descending.
     */
    public static List<WorkPacket> createPackets(Map<String, Object> config) {
        if (config == null) config = Map.of();
        String sourceType = config.containsKey(EventBatch.SOURCE_TYPE_KEY)
                ? String.valueOf(config.get(EventBatch.SOURCE_TYPE_KEY)).toLowerCase()
                : EventBatch.SOURCE_TYPE_SIMULATED;
        List<PartitionInfo> partitions = getPartitionsForSource(sourceType, config);
        long maxPacket = EventBatch.numberFromConfig(config.get("max_packet_size"), (int) DEFAULT_MAX_PACKET_SIZE);
        return buildPackets(partitions, sourceType, config, maxPacket);
    }

    private static List<PartitionInfo> getPartitionsForSource(String sourceType, Map<String, Object> config) {
        if (EventBatch.SOURCE_TYPE_PARQUET.equals(sourceType)) {
            try {
                return ParquetPartitionMetadata.getPartitionsFromConfig(config);
            } catch (Exception e) {
                throw new RuntimeException("Parquet partition metadata: " + e.getMessage(), e);
            }
        }
        return new SimulatedPartitionMetadata().getPartitions(config);
    }

    private static List<WorkPacket> buildPackets(List<PartitionInfo> partitions, String sourceType,
                                                  Map<String, Object> config, long maxPacketSize) {
        int totalCountConfig = EventBatch.numberFromConfig(config.get(EventBatch.TOTAL_COUNT_KEY), 50_000);
        if (partitions.isEmpty()) {
            return List.of(WorkPacket.range("full", 0L, (long) totalCountConfig));
        }
        List<WorkPacket> packets = new ArrayList<>();
        boolean isSimulated = EventBatch.SOURCE_TYPE_SIMULATED.equals(sourceType);
        long runningStart = 0;
        for (PartitionInfo p : partitions) {
            long est = p.estimatedEventCount();
            if (isSimulated && est > maxPacketSize && p.partitionId().startsWith("day-")) {
                long totalCount = partitions.stream().mapToLong(PartitionInfo::estimatedEventCount).sum();
                int partIndex = Integer.parseInt(p.partitionId().substring("day-".length()));
                long start = partIndex * (totalCount / partitions.size());
                long remaining = est;
                while (remaining > 0) {
                    long chunk = Math.min(remaining, maxPacketSize);
                    packets.add(WorkPacket.range(p.partitionId(), start, start + chunk));
                    start += chunk;
                    remaining -= chunk;
                }
            } else if (isSimulated && p.partitionId().startsWith("day-")) {
                long totalCount = partitions.stream().mapToLong(PartitionInfo::estimatedEventCount).sum();
                int partIndex = Integer.parseInt(p.partitionId().substring("day-".length()));
                long start = partIndex * (totalCount / partitions.size());
                packets.add(WorkPacket.range(p.partitionId(), start, start + est));
            } else if (isSimulated && (p.partitionId().equals("heavy") || p.partitionId().equals("medium") || p.partitionId().equals("light"))) {
                long start = startOffsetForSkewBucket(p.partitionId(), partitions);
                packets.add(WorkPacket.range(p.partitionId(), start, start + est));
            } else {
                packets.add(WorkPacket.partition(p.partitionId(), est));
            }
        }
        packets.sort(Comparator.comparingLong(WorkPacket::getEstimatedEventCount).reversed());
        return packets;
    }

    private static long startOffsetForSkewBucket(String bucket, List<PartitionInfo> partitions) {
        long start = 0;
        for (PartitionInfo p : partitions) {
            if (p.partitionId().equals(bucket)) break;
            start += p.estimatedEventCount();
        }
        return start;
    }
}
