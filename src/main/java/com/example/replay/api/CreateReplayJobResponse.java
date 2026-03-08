package com.example.replay.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Response body for POST /api/v1/replay/jobs (201 Created).
 */
public final class CreateReplayJobResponse {

    private final String jobId;
    private final String status;

    @JsonCreator
    public CreateReplayJobResponse(
            @JsonProperty(value = "job_id", required = true) String jobId,
            @JsonProperty(value = "status", required = true) String status) {
        this.jobId = Objects.requireNonNull(jobId, "job_id");
        this.status = Objects.requireNonNull(status, "status");
    }

    @JsonProperty("job_id")
    public String getJobId() {
        return jobId;
    }

    @JsonProperty("status")
    public String getStatus() {
        return status;
    }
}
