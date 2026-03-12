package com.example.replay.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * Core event model for security/replay events.
 * All fields are required.
 */
public final class SecurityEvent {

    private final String cid;
    private final Instant eventTimestamp;
    private final long eventTime;
    private final String eventType;
    private final String eventId;

    @JsonCreator
    public SecurityEvent(
            @JsonProperty(value = "cid", required = true) String cid,
            @JsonProperty(value = "event_timestamp", required = true) Instant eventTimestamp,
            @JsonProperty(value = "event_time", required = true) long eventTime,
            @JsonProperty(value = "event_type", required = true) String eventType,
            @JsonProperty(value = "event_id", required = true) String eventId) {
        this.cid = Objects.requireNonNull(cid, "cid");
        this.eventTimestamp = Objects.requireNonNull(eventTimestamp, "event_timestamp");
        this.eventTime = eventTime;
        this.eventType = Objects.requireNonNull(eventType, "event_type");
        this.eventId = Objects.requireNonNull(eventId, "event_id");
    }

    @JsonProperty("cid")
    public String getCid() {
        return cid;
    }

    @JsonProperty("event_timestamp")
    public Instant getEventTimestamp() {
        return eventTimestamp;
    }

    @JsonProperty("event_time")
    public long getEventTime() {
        return eventTime;
    }

    @JsonProperty("event_type")
    public String getEventType() {
        return eventType;
    }

    @JsonProperty("event_id")
    public String getEventId() {
        return eventId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SecurityEvent that = (SecurityEvent) o;
        return eventTime == that.eventTime
                && Objects.equals(cid, that.cid)
                && Objects.equals(eventTimestamp, that.eventTimestamp)
                && Objects.equals(eventType, that.eventType)
                && Objects.equals(eventId, that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cid, eventTimestamp, eventTime, eventType, eventId);
    }

    @Override
    public String toString() {
        return "SecurityEvent{cid='" + cid + "', eventType='" + eventType + "', eventId='" + eventId + "'}";
    }
}
