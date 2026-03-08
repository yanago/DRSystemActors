package com.example.replay.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * Current job progress: events processed and timestamps for status API.
 */
public final class JobProgress {

    private final long eventsProcessed;
    private final Instant startedAt;
    private final Instant lastActivityAt;

    @JsonCreator
    public JobProgress(
            @JsonProperty("events_processed") long eventsProcessed,
            @JsonProperty("started_at") Instant startedAt,
            @JsonProperty("last_activity_at") Instant lastActivityAt) {
        this.eventsProcessed = Math.max(0, eventsProcessed);
        this.startedAt = startedAt;
        this.lastActivityAt = lastActivityAt;
    }

    @JsonProperty("events_processed")
    public long getEventsProcessed() {
        return eventsProcessed;
    }

    @JsonProperty("started_at")
    public Instant getStartedAt() {
        return startedAt;
    }

    @JsonProperty("last_activity_at")
    public Instant getLastActivityAt() {
        return lastActivityAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobProgress that = (JobProgress) o;
        return eventsProcessed == that.eventsProcessed
                && Objects.equals(startedAt, that.startedAt)
                && Objects.equals(lastActivityAt, that.lastActivityAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventsProcessed, startedAt, lastActivityAt);
    }
}
