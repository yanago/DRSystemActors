package com.example.replay.actors;

import com.example.replay.actors.messages.DataReaderMessages;
import com.example.replay.datalake.EventBatch;
import com.example.replay.datalake.ReplayEventSource;
import com.example.replay.datalake.ReplayEventSourceFactory;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import org.apache.pekko.event.Logging;
import org.apache.pekko.event.LoggingAdapter;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Reads data for a replay job from Iceberg/Delta/Parquet or simulated catalog.
 * Streams batches to parent (ReplayJobActor) which forwards to DataEmitterActor.
 */
public final class DataReaderActor extends AbstractActor {

    private final String jobId;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private boolean paused;
    private boolean stopped;
    private ReplayEventSource source;

    private DataReaderActor(String jobId) {
        this.jobId = jobId;
        this.paused = false;
        this.stopped = false;
        this.source = null;
    }

    public static Props props(String jobId) {
        return Props.create(DataReaderActor.class, jobId);
    }

    @Override
    public void postStop() {
        if (source != null) {
            try {
                source.close();
            } catch (Exception ignored) {
            }
            source = null;
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DataReaderMessages.StartReading.class, this::onStartReading)
                .match(DataReaderMessages.ReadNextBatch.class, this::onReadNextBatch)
                .match(DataReaderMessages.PauseReading.class, this::onPauseReading)
                .match(DataReaderMessages.ResumeReading.class, this::onResumeReading)
                .match(DataReaderMessages.StopReading.class, this::onStopReading)
                .matchAny(msg -> log.warning("Unhandled message: {}", msg))
                .build();
    }

    private void onStartReading(DataReaderMessages.StartReading msg) {
        if (stopped) return;
        Map<String, Object> config = msg.config() != null ? msg.config() : Collections.emptyMap();
        log.info("DataReader [{}] started with config: {} (source_type={})", jobId, config.keySet(), config.getOrDefault(EventBatch.SOURCE_TYPE_KEY, "simulated"));
        if (source != null) {
            source.close();
            source = null;
        }
        source = ReplayEventSourceFactory.create(config);
        getSelf().tell(new DataReaderMessages.ReadNextBatch(), getSelf());
    }

    private void onReadNextBatch(DataReaderMessages.ReadNextBatch msg) {
        if (stopped || source == null) return;
        if (paused) {
            getSelf().tell(new DataReaderMessages.ReadNextBatch(), getSelf());
            return;
        }
        EventBatch batch = source.nextBatch();
        List<Object> events = batch.events();
        boolean last = batch.lastBatch();
        getContext().getParent().tell(new DataReaderMessages.BatchRead(jobId, events, last), getSelf());
        if (last) {
            try {
                source.close();
            } catch (Exception ignored) {
            }
            source = null;
            log.info("DataReader [{}] finished streaming (last batch)", jobId);
            return;
        }
        if (source.hasMore() && !stopped && !paused) {
            getSelf().tell(new DataReaderMessages.ReadNextBatch(), getSelf());
        }
    }

    private void onPauseReading(DataReaderMessages.PauseReading msg) {
        paused = true;
        log.info("DataReader [{}] paused", jobId);
    }

    private void onResumeReading(DataReaderMessages.ResumeReading msg) {
        paused = false;
        log.info("DataReader [{}] resumed", jobId);
        if (source != null && source.hasMore() && !stopped) {
            getSelf().tell(new DataReaderMessages.ReadNextBatch(), getSelf());
        }
    }

    private void onStopReading(DataReaderMessages.StopReading msg) {
        stopped = true;
        if (source != null) {
            try {
                source.close();
            } catch (Exception ignored) {
            }
            source = null;
        }
        log.info("DataReader [{}] stopped", jobId);
        getContext().getParent().tell(new DataReaderMessages.BatchRead(jobId, List.of(), true), getSelf());
    }
}
