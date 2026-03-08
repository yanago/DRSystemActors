package com.example.replay.api;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CreateReplayJobValidatorTest {

    @Test
    void validRequestPasses() {
        CreateReplayJobRequest req = new CreateReplayJobRequest("job-1", Map.of("source", "kafka-topic"));
        assertTrue(CreateReplayJobValidator.validate(req).isValid());
    }

    @Test
    void nullRequestFails() {
        ValidationResult r = CreateReplayJobValidator.validate(null);
        assertFalse(r.isValid());
        assertTrue(r.getErrors().stream().anyMatch(e -> e.contains("Request body")));
    }

    @Test
    void blankNameFails() {
        CreateReplayJobRequest req = new CreateReplayJobRequest("", Map.of("source", "x"));
        ValidationResult r = CreateReplayJobValidator.validate(req);
        assertFalse(r.isValid());
        assertTrue(r.getErrors().stream().anyMatch(e -> e.contains("name")));
    }

    @Test
    void nullParametersFails() {
        CreateReplayJobRequest req = new CreateReplayJobRequest("job", null);
        ValidationResult r = CreateReplayJobValidator.validate(req);
        assertFalse(r.isValid());
        assertTrue(r.getErrors().stream().anyMatch(e -> e.contains("parameters")));
    }

    @Test
    void emptyParametersFails() {
        CreateReplayJobRequest req = new CreateReplayJobRequest("job", Map.of());
        ValidationResult r = CreateReplayJobValidator.validate(req);
        assertFalse(r.isValid());
        assertTrue(r.getErrors().stream().anyMatch(e -> e.contains("empty")));
    }

    @Test
    void parametersWithoutSourceFails() {
        CreateReplayJobRequest req = new CreateReplayJobRequest("job", Map.of("target", "s3://x"));
        ValidationResult r = CreateReplayJobValidator.validate(req);
        assertFalse(r.isValid());
        assertTrue(r.getErrors().stream().anyMatch(e -> e.contains("source")));
    }
}
