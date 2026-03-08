package com.example.replay.actors.messages;

import com.example.replay.model.ReplayJob;

import java.util.List;
import java.util.Map;

/**
 * Messages for {@link com.example.replay.actors.JobManager}.
 */
public final class JobManagerMessages {

    private JobManagerMessages() {
    }

    /** Request to create a new replay job (jobId and parameters). */
    public record CreateJob(String jobId, Map<String, Object> parameters) {
    }

    /** Request job status by id. Reply with JobStatusResponse. */
    public record GetJobStatus(String jobId) {
    }

    /** Request list of all jobs. Reply with JobListResponse. */
    public record ListJobs() {
    }

    /** Lifecycle command for a job. Forwarded to ReplayJobActor. */
    public record JobLifecycleCommand(String jobId, LifecycleCommand command) {
        public enum LifecycleCommand {
            START,
            PAUSE,
            RESUME,
            CANCEL
        }
    }

    /** Response with current job status. */
    public record JobStatusResponse(String jobId, ReplayJob.ReplayJobStatus status, String message) {
    }

    /** Response with list of jobs. */
    public record JobListResponse(List<ReplayJob> jobs) {
    }
}
