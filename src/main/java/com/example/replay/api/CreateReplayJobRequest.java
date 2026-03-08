package com.example.replay.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/**
 * Request body for creating a new replay job.
 */
public final class CreateReplayJobRequest {

    private final String name;
    private final Map<String, Object> parameters;

    @JsonCreator
    public CreateReplayJobRequest(
            @JsonProperty(value = "name", required = true) String name,
            @JsonProperty(value = "parameters", required = true) Map<String, Object> parameters) {
        this.name = name;
        this.parameters = parameters == null ? null : Map.copyOf(parameters);
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("parameters")
    public Map<String, Object> getParameters() {
        return parameters == null ? null : Map.copyOf(parameters);
    }
}
