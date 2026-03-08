package com.example.replay.kafka;

import com.example.replay.api.EventDestination;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for Kafka emission (partition key = cid).
 * Does not require embedded Kafka; validates config and empty batch.
 */
class KafkaEventDestinationTest {

    @Test
    void fromConfigCreatesDestination() {
        KafkaEventDestination dest = KafkaEventDestination.fromConfig(Map.of(
                KafkaEventDestination.TOPIC_KEY, "test-topic",
                KafkaEventDestination.BOOTSTRAP_SERVERS_KEY, "localhost:9092"
        ));
        assertNotNull(dest);
        dest.close();
    }

    @Test
    void sendBatchEmptyDoesNotThrow() {
        EventDestination dest = KafkaEventDestination.fromConfig(Map.of(
                "destination", "kafka",
                KafkaEventDestination.TOPIC_KEY, "test-events"
        ));
        assertDoesNotThrow(() -> dest.sendBatch(List.of()));
        assertDoesNotThrow(dest::close);
    }
}
