package com.example.replay.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Replay job model: parameters and status for a replay/DR job.
 */
public final class ReplayJob {

    private final String jobId;
    private final ReplayJobStatus status;
    private final Map<String, Object> parameters;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final String message;

    @JsonCreator
    public ReplayJob(
            @JsonProperty(value = "job_id", required = true) String jobId,
            @JsonProperty(value = "status", required = true) ReplayJobStatus status,
            @JsonProperty("parameters") Map<String, Object> parameters,
            @JsonProperty("created_at") Instant createdAt,
            @JsonProperty("updated_at") Instant updatedAt,
            @JsonProperty("message") String message) {
        this.jobId = Objects.requireNonNull(jobId, "job_id");
        this.status = Objects.requireNonNull(status, "status");
        this.parameters = parameters == null ? Map.of() : Map.copyOf(parameters);
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.message = message;
    }

    @JsonProperty("job_id")
    public String getJobId() {
        return jobId;
    }

    @JsonProperty("status")
    public ReplayJobStatus getStatus() {
        return status;
    }

    @JsonProperty("parameters")
    public Map<String, Object> getParameters() {
        return parameters;
    }

    @JsonProperty("created_at")
    public Instant getCreatedAt() {
        return createdAt;
    }

    @JsonProperty("updated_at")
    public Instant getUpdatedAt() {
        return updatedAt;
    }

    @JsonProperty("message")
    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplayJob that = (ReplayJob) o;
        return Objects.equals(jobId, that.jobId)
                && status == that.status
                && Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, status, parameters);
    }

    @Override
    public String toString() {
        return "ReplayJob{jobId='" + jobId + "', status=" + status + "}";
    }

    /**
     * Job lifecycle status.
     */
    public enum ReplayJobStatus {
        PENDING,
        RUNNING,
        PAUSED,
        COMPLETED,
        FAILED,
        CANCELLED
    }
}
