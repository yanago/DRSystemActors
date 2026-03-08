package com.example.replay.api;

import com.example.replay.kafka.KafkaEventDestination;
import com.example.replay.rest.SimulatedRestDestination;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventDestinationFactoryTest {

    @Test
    void createRestDefault() {
        EventDestination dest = EventDestinationFactory.create(Map.of());
        assertNotNull(dest);
        assertTrue(dest instanceof SimulatedRestDestination, "no rest_url => simulated");
    }

    @Test
    void createRestSimulateUrl() {
        EventDestination dest = EventDestinationFactory.create(Map.of(
                EventDestinationFactory.DESTINATION_KEY, "rest",
                "rest_url", "http://simulate"
        ));
        assertNotNull(dest);
        assertTrue(dest instanceof SimulatedRestDestination);
    }

    @Test
    void createKafka() {
        EventDestination dest = EventDestinationFactory.create(Map.of(
                EventDestinationFactory.DESTINATION_KEY, "kafka",
                "kafka_topic", "replay",
                "kafka_bootstrap_servers", "localhost:9092"
        ));
        assertNotNull(dest);
        assertEquals(KafkaEventDestination.class, dest.getClass());
    }
}
