package com.example.replay.model;

import com.example.replay.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SecurityEventTest {

    @Test
    void roundTripSerialization() {
        Instant ts = Instant.parse("2025-03-08T12:00:00Z");
        SecurityEvent event = new SecurityEvent("cid-1", ts, ts, "LOGIN", "evt-001");
        String json = JsonUtil.toJson(event);
        assertNotNull(json);
        SecurityEvent decoded = JsonUtil.fromJson(json, SecurityEvent.class);
        assertEquals(event.getCid(), decoded.getCid());
        assertEquals(event.getEventId(), decoded.getEventId());
        assertEquals(event.getEventType(), decoded.getEventType());
        assertEquals(event.getEventTimestamp(), decoded.getEventTimestamp());
        assertEquals(event.getEventTime(), decoded.getEventTime());
    }
}
