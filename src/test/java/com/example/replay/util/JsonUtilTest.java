package com.example.replay.util;

import com.example.replay.model.SecurityEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JsonUtilTest {

    @Test
    void fromJsonReturnsNullForBlankString() {
        assertEquals(null, JsonUtil.fromJson("", SecurityEvent.class));
        assertEquals(null, JsonUtil.fromJson("   ", SecurityEvent.class));
        assertEquals(null, JsonUtil.fromJson((String) null, SecurityEvent.class));
    }

    @Test
    void toJsonHandlesNull() {
        assertEquals("null", JsonUtil.toJson(null));
    }

    @Test
    void mapperReturnsSameInstance() {
        assertNotNull(JsonUtil.mapper());
        assertEquals(JsonUtil.mapper(), JsonUtil.mapper());
    }
}
