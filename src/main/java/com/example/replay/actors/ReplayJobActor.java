package com.example.replay.actors;

import com.example.replay.actors.messages.DataEmitterMessages;
import com.example.replay.actors.messages.DataReaderMessages;
import com.example.replay.actors.messages.JobManagerMessages;
import com.example.replay.actors.messages.ReplayJobActorMessages;
import com.example.replay.actors.messages.WorkDistributorMessages;
import com.example.replay.model.ReplayJob;
import com.example.replay.storage.ReplayJobRepository;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import org.apache.pekko.event.Logging;
import org.apache.pekko.event.LoggingAdapter;

import java.time.Instant;
import java.util.Map;

/**
 * Manages one replay job: lifecycle (start, pause, resume, cancel) and coordinates DataReader + DataEmitter.
 */
public final class ReplayJobActor extends AbstractActor {

    private final String jobId;
    private final ReplayJobRepository repository;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private ReplayJob.ReplayJobStatus status = ReplayJob.ReplayJobStatus.PENDING;
    private Map<String, Object> parameters = Map.of();
    private ActorRef readerRef;
    private ActorRef distributorRef;
    private ActorRef emitterRef;

    private ReplayJobActor(String jobId, ReplayJobRepository repository) {
        this.jobId = jobId;
        this.repository = repository;
    }

    public static Props props(String jobId, ReplayJobRepository repository) {
        return Props.create(ReplayJobActor.class, jobId, repository);
    }

    @Override
    public void preStart() {
        emitterRef = getContext().actorOf(DataEmitterActor.props(jobId), "emitter");
        readerRef = getContext().actorOf(DataReaderActor.props(jobId), "reader");
        distributorRef = getContext().actorOf(WorkDistributorActor.props(jobId, emitterRef), "distributor");
    }

    private static boolean usePartitionAwareDistribution(Map<String, Object> params) {
        if (params == null) return false;
        if (Boolean.TRUE.equals(params.get("partition_aware"))) return true;
        Object w = params.get("worker_count");
        if (w instanceof Number n && n.intValue() > 1) return true;
        return false;
    }

    private static int workerCount(Map<String, Object> params) {
        if (params == null) return 4;
        Object w = params.get("worker_count");
        if (w instanceof Number n) return Math.max(1, n.intValue());
        return 4;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReplayJobActorMessages.Run.class, this::onRun)
                .match(ReplayJobActorMessages.Start.class, this::onStart)
                .match(ReplayJobActorMessages.Pause.class, this::onPause)
                .match(ReplayJobActorMessages.Resume.class, this::onResume)
                .match(ReplayJobActorMessages.Cancel.class, this::onCancel)
                .match(ReplayJobActorMessages.GetStatus.class, this::onGetStatus)
                .match(DataReaderMessages.BatchRead.class, this::onBatchRead)
                .match(WorkDistributorMessages.AllWorkComplete.class, this::onAllWorkComplete)
                .match(DataEmitterMessages.BatchEmitted.class, msg -> { /* ack from emitter; optional back-pressure */ })
                .matchAny(msg -> log.warning("Unhandled message: {}", msg))
                .build();
    }

    private void onRun(ReplayJobActorMessages.Run msg) {
        if (status != ReplayJob.ReplayJobStatus.PENDING) {
            log.warning("Job [{}] already running/paused/cancelled, ignoring Run", jobId);
            return;
        }
        this.parameters = msg.parameters() != null ? msg.parameters() : Map.of();
        status = ReplayJob.ReplayJobStatus.RUNNING;
        saveJob();
        emitterRef.tell(new DataEmitterMessages.ConfigureDestination(parameters), getSelf());
        if (usePartitionAwareDistribution(parameters)) {
            distributorRef.tell(new WorkDistributorMessages.StartDistribution(parameters, workerCount(parameters)), getSelf());
        } else {
            readerRef.tell(new DataReaderMessages.StartReading(parameters), getSelf());
        }
        log.info("Job [{}] started", jobId);
    }

    private void onStart(ReplayJobActorMessages.Start msg) {
        if (status == ReplayJob.ReplayJobStatus.PENDING) {
            emitterRef.tell(new DataEmitterMessages.ConfigureDestination(parameters), getSelf());
            if (usePartitionAwareDistribution(parameters)) {
                distributorRef.tell(new WorkDistributorMessages.StartDistribution(parameters, workerCount(parameters)), getSelf());
            } else {
                readerRef.tell(new DataReaderMessages.StartReading(parameters), getSelf());
            }
            status = ReplayJob.ReplayJobStatus.RUNNING;
            saveJob();
            log.info("Job [{}] started", jobId);
        }
    }

    private void onPause(ReplayJobActorMessages.Pause msg) {
        if (status == ReplayJob.ReplayJobStatus.RUNNING) {
            status = ReplayJob.ReplayJobStatus.PAUSED;
            saveJob();
            if (usePartitionAwareDistribution(parameters)) {
                distributorRef.tell(new WorkDistributorMessages.PauseDistribution(), getSelf());
            } else {
                readerRef.tell(new DataReaderMessages.PauseReading(), getSelf());
            }
            log.info("Job [{}] paused", jobId);
        }
    }

    private void onResume(ReplayJobActorMessages.Resume msg) {
        if (status == ReplayJob.ReplayJobStatus.PAUSED) {
            status = ReplayJob.ReplayJobStatus.RUNNING;
            saveJob();
            if (usePartitionAwareDistribution(parameters)) {
                distributorRef.tell(new WorkDistributorMessages.ResumeDistribution(), getSelf());
            } else {
                readerRef.tell(new DataReaderMessages.ResumeReading(), getSelf());
            }
            log.info("Job [{}] resumed", jobId);
        }
    }

    private void onCancel(ReplayJobActorMessages.Cancel msg) {
        if (status == ReplayJob.ReplayJobStatus.CANCELLED || status == ReplayJob.ReplayJobStatus.COMPLETED) {
            return;
        }
        status = ReplayJob.ReplayJobStatus.CANCELLED;
        saveJob();
        if (usePartitionAwareDistribution(parameters)) {
            distributorRef.tell(new WorkDistributorMessages.CancelDistribution(), getSelf());
        } else {
            readerRef.tell(new DataReaderMessages.StopReading(), getSelf());
        }
        emitterRef.tell(new DataEmitterMessages.StopEmitting(), getSelf());
        log.info("Job [{}] cancelled", jobId);
    }

    private void onAllWorkComplete(WorkDistributorMessages.AllWorkComplete msg) {
        if (status != ReplayJob.ReplayJobStatus.CANCELLED && status != ReplayJob.ReplayJobStatus.FAILED) {
            status = ReplayJob.ReplayJobStatus.COMPLETED;
            log.info("Job [{}] completed (distributed)", jobId);
        }
        saveJob();
        emitterRef.tell(new DataEmitterMessages.StopEmitting(), getSelf());
    }

    private void onGetStatus(ReplayJobActorMessages.GetStatus msg) {
        getSender().tell(new JobManagerMessages.JobStatusResponse(jobId, status, null), getSelf());
    }

    private void onBatchRead(DataReaderMessages.BatchRead msg) {
        if (msg.lastBatch()) {
            if (status != ReplayJob.ReplayJobStatus.CANCELLED && status != ReplayJob.ReplayJobStatus.FAILED) {
                status = ReplayJob.ReplayJobStatus.COMPLETED;
                log.info("Job [{}] completed", jobId);
            }
            saveJob();
            emitterRef.tell(new DataEmitterMessages.StopEmitting(), getSelf());
            return;
        }
        if (status == ReplayJob.ReplayJobStatus.RUNNING && msg.records() != null && !msg.records().isEmpty()) {
            emitterRef.tell(new DataEmitterMessages.EmitBatch(jobId, msg.records()), getSelf());
        }
    }

    private void saveJob() {
        Instant now = Instant.now();
        Instant created = repository.findById(jobId).map(ReplayJob::getCreatedAt).orElse(now);
        ReplayJob job = new ReplayJob(jobId, status, parameters, created, now, null);
        repository.save(job);
    }
}
