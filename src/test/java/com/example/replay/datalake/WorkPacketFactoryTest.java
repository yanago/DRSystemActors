package com.example.replay.datalake;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkPacketFactoryTest {

    @Test
    void createPacketsSimulatedByDay() {
        Map<String, Object> config = Map.of(
                EventBatch.SOURCE_TYPE_KEY, EventBatch.SOURCE_TYPE_SIMULATED,
                EventBatch.TOTAL_COUNT_KEY, 70_000,
                "num_days", 7
        );
        List<WorkPacket> packets = WorkPacketFactory.createPackets(config);
        assertNotNull(packets);
        assertEquals(7, packets.size());
        long total = packets.stream().mapToLong(WorkPacket::getEstimatedEventCount).sum();
        assertEquals(70_000, total);
        assertTrue(packets.get(0).getEstimatedEventCount() >= packets.get(packets.size() - 1).getEstimatedEventCount(),
                "Packets sorted by size descending for balance");
    }

    @Test
    void createPacketsSimulatedWithSkew() {
        Map<String, Object> config = Map.of(
                EventBatch.SOURCE_TYPE_KEY, EventBatch.SOURCE_TYPE_SIMULATED,
                EventBatch.TOTAL_COUNT_KEY, 10_000,
                "partition_by_skew", true
        );
        List<WorkPacket> packets = WorkPacketFactory.createPackets(config);
        assertNotNull(packets);
        assertEquals(3, packets.size());
        assertTrue(packets.stream().anyMatch(p -> "heavy".equals(p.getPartitionId())));
        assertTrue(packets.stream().anyMatch(p -> "medium".equals(p.getPartitionId())));
        assertTrue(packets.stream().anyMatch(p -> "light".equals(p.getPartitionId())));
        assertTrue(packets.get(0).getEstimatedEventCount() >= packets.get(2).getEstimatedEventCount());
    }

    @Test
    void createPacketsDefaultSimulatedProducesDayPartitions() {
        Map<String, Object> config = Map.of(
                EventBatch.SOURCE_TYPE_KEY, EventBatch.SOURCE_TYPE_SIMULATED,
                EventBatch.TOTAL_COUNT_KEY, 50_000
        );
        List<WorkPacket> packets = WorkPacketFactory.createPackets(config);
        assertNotNull(packets);
        assertFalse(packets.isEmpty());
        assertTrue(packets.stream().allMatch(WorkPacket::isRange));
    }
}
