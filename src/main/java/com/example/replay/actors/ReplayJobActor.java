package com.example.replay.actors;

import com.example.replay.actors.messages.DataEmitterMessages;
import com.example.replay.actors.messages.DataReaderMessages;
import com.example.replay.actors.messages.JobManagerMessages;
import com.example.replay.actors.messages.ReplayJobActorMessages;
import com.example.replay.actors.messages.WorkDistributorMessages;
import com.example.replay.model.JobMetrics;
import com.example.replay.model.JobProgress;
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

    private long eventsProcessed;
    private Instant startedAt;
    private Instant lastActivityAt;
    private long errorCount;
    private long latencySumMs;
    private long latencyCount;

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
                .match(DataEmitterMessages.BatchEmitted.class, this::onBatchEmitted)
                .match(DataEmitterMessages.BatchEmitFailed.class, this::onBatchEmitFailed)
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
        this.startedAt = Instant.now();
        this.lastActivityAt = this.startedAt;
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
            this.startedAt = Instant.now();
            this.lastActivityAt = this.startedAt;
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
        JobProgress progress = buildProgress();
        JobMetrics metrics = buildMetrics();
        getSender().tell(new JobManagerMessages.JobStatusResponse(jobId, status, null, progress, metrics), getSelf());
    }

    private void onBatchEmitted(DataEmitterMessages.BatchEmitted msg) {
        eventsProcessed += msg.count();
        lastActivityAt = Instant.now();
        if (msg.latencyMs() > 0) {
            latencySumMs += msg.latencyMs();
            latencyCount++;
        }
    }

    private void onBatchEmitFailed(DataEmitterMessages.BatchEmitFailed msg) {
        errorCount += msg.failedCount();
        lastActivityAt = Instant.now();
    }

    private JobProgress buildProgress() {
        if (startedAt == null) return new JobProgress(0, null, null);
        return new JobProgress(eventsProcessed, startedAt, lastActivityAt != null ? lastActivityAt : startedAt);
    }

    private JobMetrics buildMetrics() {
        double eventsPerSecond = 0;
        if (startedAt != null && eventsProcessed > 0) {
            long elapsedSec = Math.max(1, java.time.Duration.between(startedAt, Instant.now()).getSeconds());
            eventsPerSecond = (double) eventsProcessed / elapsedSec;
        }
        double latencyMsAvg = (latencyCount > 0) ? (double) latencySumMs / latencyCount : 0;
        return new JobMetrics(eventsPerSecond, latencyMsAvg, errorCount);
    }

    private void onBatchRead(DataReaderMessages.BatchRead msg) {
        if (status == ReplayJob.ReplayJobStatus.RUNNING && msg.records() != null && !msg.records().isEmpty()) {
            emitterRef.tell(new DataEmitterMessages.EmitBatch(jobId, msg.records()), getSelf());
        }
        if (msg.lastBatch()) {
            if (status != ReplayJob.ReplayJobStatus.CANCELLED && status != ReplayJob.ReplayJobStatus.FAILED) {
                status = ReplayJob.ReplayJobStatus.COMPLETED;
                log.info("Job [{}] completed", jobId);
            }
            saveJob();
            emitterRef.tell(new DataEmitterMessages.StopEmitting(), getSelf());
        }
    }

    private void saveJob() {
        Instant now = Instant.now();
        Instant created = repository.findById(jobId).map(ReplayJob::getCreatedAt).orElse(now);
        ReplayJob job = new ReplayJob(jobId, status, parameters, created, now, null);
        repository.save(job);
    }
}
