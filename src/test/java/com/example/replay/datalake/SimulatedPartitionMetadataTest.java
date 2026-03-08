package com.example.replay.datalake;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for skewed data distribution: heavy/medium/light partition fractions.
 */
class SimulatedPartitionMetadataTest {

    private static final double TOLERANCE = 0.01;

    @Test
    void partitionsByDayDistributeEvenly() {
        SimulatedPartitionMetadata provider = new SimulatedPartitionMetadata();
        List<PartitionInfo> partitions = provider.getPartitions(Map.of(
                EventBatch.TOTAL_COUNT_KEY, 70_000,
                "num_days", 7
        ));
        assertEquals(7, partitions.size());
        long total = partitions.stream().mapToLong(PartitionInfo::estimatedEventCount).sum();
        assertEquals(70_000, total);
        long perDay = 70_000 / 7;
        for (PartitionInfo p : partitions) {
            assertTrue(p.estimatedEventCount() >= perDay - 1 && p.estimatedEventCount() <= perDay + 1);
        }
    }

    @Test
    void partitionsBySkewHeavyDominates() {
        SimulatedPartitionMetadata provider = new SimulatedPartitionMetadata();
        int total = 100_000;
        List<PartitionInfo> partitions = provider.getPartitions(Map.of(
                EventBatch.TOTAL_COUNT_KEY, total,
                "partition_by_skew", true
        ));
        assertEquals(3, partitions.size());
        long heavy = partitions.stream().filter(p -> "heavy".equals(p.partitionId())).mapToLong(PartitionInfo::estimatedEventCount).sum();
        long medium = partitions.stream().filter(p -> "medium".equals(p.partitionId())).mapToLong(PartitionInfo::estimatedEventCount).sum();
        long light = partitions.stream().filter(p -> "light".equals(p.partitionId())).mapToLong(PartitionInfo::estimatedEventCount).sum();
        assertEquals(total, heavy + medium + light);
        assertTrue(heavy >= total * 0.60 && heavy <= total * 0.70, "heavy ~65%: " + heavy);
        assertTrue(medium >= total * 0.20 && medium <= total * 0.30, "medium ~25%: " + medium);
        assertTrue(light >= total * 0.05 && light <= total * 0.15, "light ~10%: " + light);
    }

    @Test
    void skewedWorkPacketsSortedBySizeDescending() {
        List<WorkPacket> packets = WorkPacketFactory.createPackets(Map.of(
                EventBatch.SOURCE_TYPE_KEY, EventBatch.SOURCE_TYPE_SIMULATED,
                EventBatch.TOTAL_COUNT_KEY, 10_000,
                "partition_by_skew", Boolean.TRUE
        ));
        assertEquals(3, packets.size());
        for (int i = 0; i < packets.size() - 1; i++) {
            assertTrue(packets.get(i).getEstimatedEventCount() >= packets.get(i + 1).getEstimatedEventCount(),
                    "Packets should be sorted by size descending for load balance");
        }
    }
}
