package com.example.replay.actors;

import com.example.replay.api.EventDestination;
import com.example.replay.api.EventDestinationFactory;
import com.example.replay.actors.messages.DataEmitterMessages;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import org.apache.pekko.event.Logging;
import org.apache.pekko.event.LoggingAdapter;

import java.util.List;
import java.util.Map;

/**
 * Emits/writes data for a replay job to a configurable destination (Kafka or REST).
 * Receives ConfigureDestination before or with first batch; uses cid-based partitioning for Kafka.
 */
public final class DataEmitterActor extends AbstractActor {

    private final String jobId;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private boolean stopped;
    private EventDestination destination;

    private DataEmitterActor(String jobId) {
        this.jobId = jobId;
        this.stopped = false;
        this.destination = null;
    }

    public static Props props(String jobId) {
        return Props.create(DataEmitterActor.class, jobId);
    }

    @Override
    public void postStop() {
        if (destination != null) {
            try {
                destination.close();
            } catch (Exception e) {
                log.warning("Error closing destination: {}", e.getMessage());
            }
            destination = null;
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DataEmitterMessages.ConfigureDestination.class, this::onConfigureDestination)
                .match(DataEmitterMessages.EmitBatch.class, this::onEmitBatch)
                .match(DataEmitterMessages.StopEmitting.class, this::onStopEmitting)
                .matchAny(msg -> log.warning("Unhandled message: {}", msg))
                .build();
    }

    private void onConfigureDestination(DataEmitterMessages.ConfigureDestination msg) {
        if (stopped) return;
        Map<String, Object> config = msg.config() != null ? msg.config() : Map.of();
        if (destination != null) {
            try {
                destination.close();
            } catch (Exception ignored) {
            }
        }
        destination = EventDestinationFactory.create(config);
        log.info("DataEmitter [{}] destination configured: {}", jobId, config.getOrDefault(EventDestinationFactory.DESTINATION_KEY, "rest"));
    }

    private void onEmitBatch(DataEmitterMessages.EmitBatch msg) {
        if (stopped) return;
        List<Object> records = msg.records() != null ? msg.records() : List.of();
        if (records.isEmpty()) {
            getSender().tell(new DataEmitterMessages.BatchEmitted(jobId, 0, 0L), getSelf());
            return;
        }
        if (destination == null) {
            destination = EventDestinationFactory.create(Map.of());
        }
        long startMs = System.currentTimeMillis();
        try {
            destination.sendBatch(records);
            long latencyMs = System.currentTimeMillis() - startMs;
            log.debug("DataEmitter [{}] emitted batch of {} records in {} ms", jobId, records.size(), latencyMs);
            getSender().tell(new DataEmitterMessages.BatchEmitted(jobId, records.size(), latencyMs), getSelf());
        } catch (Exception e) {
            log.error(e, "DataEmitter [{}] failed to send batch", jobId);
            getSender().tell(new DataEmitterMessages.BatchEmitFailed(jobId, records.size()), getSelf());
        }
    }

    private void onStopEmitting(DataEmitterMessages.StopEmitting msg) {
        stopped = true;
        if (destination != null) {
            try {
                destination.close();
            } catch (Exception e) {
                log.warning("Error closing destination: {}", e.getMessage());
            }
            destination = null;
        }
        log.info("DataEmitter [{}] stopped", jobId);
    }
}
