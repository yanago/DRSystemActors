package com.example.replay.model;

import com.example.replay.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SecurityEventTest {

    @Test
    void roundTripSerialization() {
        Instant ts = Instant.parse("2024-10-15T14:23:45.123Z");
        long epochMillis = 1729002225123L;
        SecurityEvent event = new SecurityEvent(
                "customer-a1b2c3",
                ts,
                epochMillis,
                "ProcessStart",
                "550e8400-e29b-41d4-a716-446655440000");
        String json = JsonUtil.toJson(event);
        assertNotNull(json);
        assertTrue(json.contains("\"event_timestamp\":\"2024-10-15T14:23:45.123Z\""));
        assertTrue(json.contains("\"event_time\":1729002225123"));
        SecurityEvent decoded = JsonUtil.fromJson(json, SecurityEvent.class);
        assertEquals(event.getCid(), decoded.getCid());
        assertEquals(event.getEventId(), decoded.getEventId());
        assertEquals(event.getEventType(), decoded.getEventType());
        assertEquals(event.getEventTimestamp(), decoded.getEventTimestamp());
        assertEquals(event.getEventTime(), decoded.getEventTime());
    }
}
