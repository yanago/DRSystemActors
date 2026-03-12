package com.example.replay.kafka;

import com.example.replay.model.SecurityEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CustomerPartitionKeyTest {

    @Test
    void keyForSecurityEvent() {
        Instant now = Instant.now();
        SecurityEvent evt = new SecurityEvent("cid-heavy-01", now, now.toEpochMilli(), "LOGIN", "evt-1");
        assertEquals("cid-heavy-01", CustomerPartitionKey.keyFor(evt));
    }

    @Test
    void keyForMap() {
        assertEquals("cid-00", CustomerPartitionKey.keyFor(Map.of("cid", "cid-00", "event_type", "ACCESS")));
        assertEquals("", CustomerPartitionKey.keyFor(Map.of("event_type", "ACCESS")));
    }

    @Test
    void keyForNull() {
        assertEquals("", CustomerPartitionKey.keyFor(null));
    }
}
