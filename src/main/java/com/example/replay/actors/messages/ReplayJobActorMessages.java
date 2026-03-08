package com.example.replay.actors.messages;

import com.example.replay.model.ReplayJob;

import java.util.Map;

/**
 * Messages for {@link com.example.replay.actors.ReplayJobActor}.
 */
public final class ReplayJobActorMessages {

    private ReplayJobActorMessages() {
    }

    /** Start the job (begin reading and emitting). */
    public record Start() {
    }

    /** Pause the job. */
    public record Pause() {
    }

    /** Resume a paused job. */
    public record Resume() {
    }

    /** Cancel the job. */
    public record Cancel() {
    }

    /** Request current status. Reply with StatusResponse. */
    public record GetStatus() {
    }

    /** Response with current status. */
    public record StatusResponse(String jobId, ReplayJob.ReplayJobStatus status, String message) {
    }

    /** Internal: tell the job actor to run with these parameters (from JobManager). */
    public record Run(String jobId, Map<String, Object> parameters) {
    }
}
