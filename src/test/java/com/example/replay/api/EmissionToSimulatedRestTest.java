package com.example.replay.api;

import com.example.replay.rest.SimulatedRestDestination;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration-style test: emission to simulated REST destination (mock).
 * Validates that events sent via EventDestination reach the simulated backend.
 */
class EmissionToSimulatedRestTest {

    @Test
    void factoryCreatesSimulatedRestWhenNoUrl() throws Exception {
        EventDestination dest = EventDestinationFactory.create(Map.of(EventDestinationFactory.DESTINATION_KEY, "rest"));
        assertInstanceOf(SimulatedRestDestination.class, dest);

        List<Object> batch = List.of(
                Map.of("cid", "customer-1", "event_type", "LOGIN", "event_id", "evt-1"),
                Map.of("cid", "customer-1", "event_type", "ACCESS", "event_id", "evt-2")
        );
        dest.sendBatch(batch);
        assertEquals(2, ((SimulatedRestDestination) dest).getReceivedCount());
        dest.close();
    }

    @Test
    void multipleBatchesAccumulateInSimulatedRest() throws Exception {
        EventDestination dest = EventDestinationFactory.create(Map.of(
                EventDestinationFactory.DESTINATION_KEY, "rest",
                "rest_url", "http://simulate"
        ));
        assertInstanceOf(SimulatedRestDestination.class, dest);

        for (int i = 0; i < 5; i++) {
            dest.sendBatch(List.of(Map.of("cid", "c" + i, "event_id", "e" + i)));
        }
        assertEquals(5, ((SimulatedRestDestination) dest).getReceivedCount());
        dest.close();
    }
}
