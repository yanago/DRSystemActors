package com.example.replay.actors;

import com.example.replay.actors.messages.DataReaderMessages;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.event.Logging;
import org.apache.pekko.event.LoggingAdapter;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Reads data for a replay job (e.g. from Kafka or storage). Sends batches to the emitter.
 */
public final class DataReaderActor extends AbstractActor {

    private final String jobId;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private boolean paused;
    private boolean stopped;

    private DataReaderActor(String jobId) {
        this.jobId = jobId;
        this.paused = false;
        this.stopped = false;
    }

    public static Props props(String jobId) {
        return Props.create(DataReaderActor.class, jobId);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DataReaderMessages.StartReading.class, this::onStartReading)
                .match(DataReaderMessages.PauseReading.class, this::onPauseReading)
                .match(DataReaderMessages.ResumeReading.class, this::onResumeReading)
                .match(DataReaderMessages.StopReading.class, this::onStopReading)
                .matchAny(msg -> log.warning("Unhandled message: {}", msg))
                .build();
    }

    private void onStartReading(DataReaderMessages.StartReading msg) {
        if (stopped) return;
        Map<String, Object> config = msg.config() != null ? msg.config() : Collections.emptyMap();
        log.info("DataReader [{}] started with config: {}", jobId, config.keySet());
        // In a full implementation: start reading from source and send BatchRead to parent.
        // For now we can send an empty batch to signal "ready" or simulate one batch.
        getContext().getParent().tell(new DataReaderMessages.BatchRead(jobId, List.of(), false), getSelf());
    }

    private void onPauseReading(DataReaderMessages.PauseReading msg) {
        paused = true;
        log.info("DataReader [{}] paused", jobId);
    }

    private void onResumeReading(DataReaderMessages.ResumeReading msg) {
        paused = false;
        log.info("DataReader [{}] resumed", jobId);
    }

    private void onStopReading(DataReaderMessages.StopReading msg) {
        stopped = true;
        log.info("DataReader [{}] stopped", jobId);
        getContext().getParent().tell(new DataReaderMessages.BatchRead(jobId, List.of(), true), getSelf());
    }
}
