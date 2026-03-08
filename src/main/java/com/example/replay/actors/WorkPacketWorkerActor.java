package com.example.replay.actors;

import com.example.replay.actors.messages.DataReaderMessages;
import com.example.replay.actors.messages.WorkDistributorMessages;
import com.example.replay.datalake.EventBatch;
import com.example.replay.datalake.ReplayEventSource;
import com.example.replay.datalake.ReplayEventSourceFactory;
import com.example.replay.datalake.WorkPacket;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import org.apache.pekko.event.Logging;
import org.apache.pekko.event.LoggingAdapter;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Processes a single work packet: creates a partition-scoped event source,
 * reads batches, and sends them to the parent (WorkDistributor).
 */
public final class WorkPacketWorkerActor extends AbstractActor {

    private final String workerId;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private boolean stopped;

    private WorkPacketWorkerActor(String workerId) {
        this.workerId = workerId;
        this.stopped = false;
    }

    public static Props props(String workerId) {
        return Props.create(WorkPacketWorkerActor.class, workerId);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(WorkDistributorMessages.AssignPacket.class, this::onAssignPacket)
                .match(WorkDistributorMessages.CancelDistribution.class, this::onCancel)
                .matchAny(msg -> log.warning("Unhandled: {}", msg))
                .build();
    }

    private void onAssignPacket(WorkDistributorMessages.AssignPacket msg) {
        if (stopped) return;
        WorkPacket packet = msg.packet();
        Map<String, Object> config = msg.config() != null ? msg.config() : Collections.emptyMap();
        String jobId = msg.jobId() != null ? msg.jobId() : "unknown";
        try {
            ReplayEventSource source = ReplayEventSourceFactory.createForWorkPacket(config, packet);
            try {
                while (source.hasMore()) {
                    if (stopped) break;
                    EventBatch batch = source.nextBatch();
                    List<Object> events = batch.events();
                    boolean last = batch.lastBatch();
                    getContext().getParent().tell(
                            new WorkDistributorMessages.WorkerBatchRead(jobId, events, last),
                            getSelf());
                    if (last) break;
                }
            } finally {
                source.close();
            }
        } catch (Exception e) {
            log.error(e, "Worker [{}] failed for packet {}", workerId, packet.getPartitionId());
            getContext().getParent().tell(
                    new WorkDistributorMessages.WorkerBatchRead(jobId, List.of(), true),
                    getSelf());
        }
        getContext().getParent().tell(new WorkDistributorMessages.PacketComplete(workerId), getSelf());
    }

    private void onCancel(WorkDistributorMessages.CancelDistribution msg) {
        stopped = true;
    }
}
