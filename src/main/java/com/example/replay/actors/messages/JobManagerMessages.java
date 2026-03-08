package com.example.replay.actors.messages;

import com.example.replay.model.JobMetrics;
import com.example.replay.model.JobProgress;
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

    /** Request job status by id. Reply with JobStatusResponse (includes progress and metrics when from actor). */
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

    /** Response with current job status, optional progress and metrics. */
    public record JobStatusResponse(
            String jobId,
            ReplayJob.ReplayJobStatus status,
            String message,
            JobProgress progress,
            JobMetrics metrics) {

        public JobStatusResponse(String jobId, ReplayJob.ReplayJobStatus status, String message) {
            this(jobId, status, message, null, null);
        }
    }

    /** Response with list of jobs. */
    public record JobListResponse(List<ReplayJob> jobs) {
    }

    /** Reply to JobLifecycleCommand: command was accepted and forwarded to the job actor. */
    public record CommandAccepted(String jobId) {
    }

    /** Reply to JobLifecycleCommand: no actor found for this job. */
    public record JobNotFound(String jobId) {
    }
}
