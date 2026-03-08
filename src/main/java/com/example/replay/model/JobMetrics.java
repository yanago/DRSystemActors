package com.example.replay.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Aggregated job metrics: throughput, latency, errors for metrics API.
 */
public final class JobMetrics {

    private final double eventsPerSecond;
    private final double latencyMsAvg;
    private final long errorCount;

    @JsonCreator
    public JobMetrics(
            @JsonProperty("events_per_second") double eventsPerSecond,
            @JsonProperty("latency_ms_avg") double latencyMsAvg,
            @JsonProperty("error_count") long errorCount) {
        this.eventsPerSecond = Math.max(0, eventsPerSecond);
        this.latencyMsAvg = Math.max(0, latencyMsAvg);
        this.errorCount = Math.max(0, errorCount);
    }

    @JsonProperty("events_per_second")
    public double getEventsPerSecond() {
        return eventsPerSecond;
    }

    @JsonProperty("latency_ms_avg")
    public double getLatencyMsAvg() {
        return latencyMsAvg;
    }

    @JsonProperty("error_count")
    public long getErrorCount() {
        return errorCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobMetrics that = (JobMetrics) o;
        return Double.compare(that.eventsPerSecond, eventsPerSecond) == 0
                && Double.compare(that.latencyMsAvg, latencyMsAvg) == 0
                && errorCount == that.errorCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventsPerSecond, latencyMsAvg, errorCount);
    }
}
