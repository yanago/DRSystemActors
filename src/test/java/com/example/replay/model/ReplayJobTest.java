package com.example.replay.model;

import com.example.replay.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ReplayJobTest {

    @Test
    void roundTripSerialization() {
        Instant now = Instant.now();
        ReplayJob job = new ReplayJob(
                "job-1",
                ReplayJob.ReplayJobStatus.RUNNING,
                Map.of("source", "kafka", "topic", "events"),
                now,
                now,
                null
        );
        String json = JsonUtil.toJson(job);
        assertNotNull(json);
        ReplayJob decoded = JsonUtil.fromJson(json, ReplayJob.class);
        assertEquals(job.getJobId(), decoded.getJobId());
        assertEquals(job.getStatus(), decoded.getStatus());
        assertEquals(job.getParameters(), decoded.getParameters());
    }
}
