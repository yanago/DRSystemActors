package com.example.replay.rest;

import com.example.replay.model.SecurityEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for REST emission to simulated destination (no data loss, correct count).
 */
class SimulatedRestDestinationTest {

    @Test
    void sendBatchIncreasesReceivedCount() {
        SimulatedRestDestination dest = new SimulatedRestDestination();
        assertEquals(0, dest.getReceivedCount());

        dest.sendBatch(List.of(
                Map.of("cid", "c1", "event_type", "LOGIN", "event_id", "e1"),
                Map.of("cid", "c2", "event_type", "ACCESS", "event_id", "e2")
        ));
        assertEquals(2, dest.getReceivedCount());

        dest.sendBatch(List.of(Map.of("cid", "c3", "event_id", "e3")));
        assertEquals(3, dest.getReceivedCount());
    }

    @Test
    void sendBatchWithSecurityEvents() throws Exception {
        SimulatedRestDestination dest = new SimulatedRestDestination();
        Instant now = Instant.now();
        List<Object> events = List.of(
                new SecurityEvent("cid-01", now, now, "LOGIN", "evt-1"),
                new SecurityEvent("cid-02", now, now, "ACCESS", "evt-2")
        );
        dest.sendBatch(events);
        assertEquals(2, dest.getReceivedCount());
        assertEquals(events, dest.getReceived());
    }

    @Test
    void sendBatchNullOrEmptyDoesNotThrow() throws Exception {
        SimulatedRestDestination dest = new SimulatedRestDestination();
        dest.sendBatch(null);
        assertEquals(0, dest.getReceivedCount());
        dest.sendBatch(List.of());
        assertEquals(0, dest.getReceivedCount());
    }

    @Test
    void isSimulationUrlRecognizesSimulateAndBlank() {
        assertTrue(SimulatedRestDestination.isSimulationUrl(null));
        assertTrue(SimulatedRestDestination.isSimulationUrl(""));
        assertTrue(SimulatedRestDestination.isSimulationUrl("http://simulate"));
        assertTrue(SimulatedRestDestination.isSimulationUrl("http://localhost:0/events"));
    }
}
