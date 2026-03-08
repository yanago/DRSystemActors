package com.example.replay.actors;

import com.example.replay.actors.messages.JobManagerMessages;
import com.example.replay.actors.messages.ReplayJobActorMessages;
import com.example.replay.model.ReplayJob;
import com.example.replay.storage.ReplayJobRepository;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.event.Logging;
import org.apache.pekko.event.LoggingAdapter;

import java.time.Instant;
import java.util.Map;

/**
 * Root actor for replay jobs: creates and supervises ReplayJobActors, routes lifecycle commands.
 */
public final class JobManager extends AbstractActor {

    private final ReplayJobRepository repository;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private JobManager(ReplayJobRepository repository) {
        this.repository = repository;
    }

    public static Props props(ReplayJobRepository repository) {
        return Props.create(JobManager.class, repository);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JobManagerMessages.CreateJob.class, this::onCreateJob)
                .match(JobManagerMessages.GetJobStatus.class, this::onGetJobStatus)
                .match(JobManagerMessages.ListJobs.class, this::onListJobs)
                .match(JobManagerMessages.JobLifecycleCommand.class, this::onJobLifecycleCommand)
                .matchAny(msg -> log.warning("Unhandled message: {}", msg))
                .build();
    }

    private void onCreateJob(JobManagerMessages.CreateJob msg) {
        String jobId = msg.jobId();
        if (jobId == null || jobId.isBlank()) {
            log.warning("CreateJob with empty jobId ignored");
            return;
        }
        if (getContext().findChild(jobId).isPresent()) {
            log.warning("Job [{}] already exists", jobId);
            return;
        }
        Map<String, Object> params = msg.parameters() != null ? msg.parameters() : Map.of();
        Instant now = Instant.now();
        ReplayJob job = new ReplayJob(jobId, ReplayJob.ReplayJobStatus.PENDING, params, now, now, null);
        repository.save(job);

        ActorRef jobActor = getContext().actorOf(ReplayJobActor.props(jobId, repository), jobId);
        jobActor.tell(new ReplayJobActorMessages.Run(jobId, params), getSelf());
        log.info("Job [{}] created and started", jobId);
    }

    private void onGetJobStatus(JobManagerMessages.GetJobStatus msg) {
        String jobId = msg.jobId();
        getContext().findChild(jobId).ifPresentOrElse(
                ref -> ref.forward(new ReplayJobActorMessages.GetStatus(), getContext()),
                () -> replyFromRepository(jobId)
        );
    }

    private void replyFromRepository(String jobId) {
        repository.findById(jobId)
                .map(job -> new JobManagerMessages.JobStatusResponse(jobId, job.getStatus(), job.getMessage()))
                .ifPresentOrElse(
                        response -> getSender().tell(response, getSelf()),
                        () -> getSender().tell(new JobManagerMessages.JobStatusResponse(jobId, null, "Job not found"), getSelf())
                );
    }

    private void onListJobs(JobManagerMessages.ListJobs msg) {
        getSender().tell(new JobManagerMessages.JobListResponse(repository.findAll()), getSelf());
    }

    private void onJobLifecycleCommand(JobManagerMessages.JobLifecycleCommand msg) {
        String jobId = msg.jobId();
        JobManagerMessages.JobLifecycleCommand.LifecycleCommand cmd = msg.command();
        getContext().findChild(jobId).ifPresentOrElse(
                ref -> {
                    switch (cmd) {
                        case START -> ref.tell(new ReplayJobActorMessages.Start(), getSelf());
                        case PAUSE -> ref.tell(new ReplayJobActorMessages.Pause(), getSelf());
                        case RESUME -> ref.tell(new ReplayJobActorMessages.Resume(), getSelf());
                        case CANCEL -> ref.tell(new ReplayJobActorMessages.Cancel(), getSelf());
                    }
                },
                () -> log.warning("Job [{}] not found for command {}", jobId, cmd)
        );
    }
}
