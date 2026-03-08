package com.example.replay.actors;

import com.example.replay.actors.messages.DataEmitterMessages;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import org.apache.pekko.event.Logging;
import org.apache.pekko.event.LoggingAdapter;

import java.util.List;

/**
 * Emits/writes data for a replay job (e.g. to Kafka or datalake). Receives batches from the reader.
 */
public final class DataEmitterActor extends AbstractActor {

    private final String jobId;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private boolean stopped;

    private DataEmitterActor(String jobId) {
        this.jobId = jobId;
        this.stopped = false;
    }

    public static Props props(String jobId) {
        return Props.create(DataEmitterActor.class, jobId);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DataEmitterMessages.EmitBatch.class, this::onEmitBatch)
                .match(DataEmitterMessages.StopEmitting.class, this::onStopEmitting)
                .matchAny(msg -> log.warning("Unhandled message: {}", msg))
                .build();
    }

    private void onEmitBatch(DataEmitterMessages.EmitBatch msg) {
        if (stopped) return;
        List<Object> records = msg.records() != null ? msg.records() : List.of();
        log.debug("DataEmitter [{}] emitting batch of {} records", jobId, records.size());
        getSender().tell(new DataEmitterMessages.BatchEmitted(jobId, records.size()), getSelf());
    }

    private void onStopEmitting(DataEmitterMessages.StopEmitting msg) {
        stopped = true;
        log.info("DataEmitter [{}] stopped", jobId);
    }
}
