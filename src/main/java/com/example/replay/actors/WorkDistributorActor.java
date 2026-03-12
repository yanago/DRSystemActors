package com.example.replay.actors;

import com.example.replay.actors.messages.DataEmitterMessages;
import com.example.replay.actors.messages.WorkDistributorMessages;
import com.example.replay.datalake.WorkPacket;
import com.example.replay.datalake.WorkPacketFactory;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import org.apache.pekko.event.Logging;
import org.apache.pekko.event.LoggingAdapter;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * Distributes work packets to worker actors with awareness of partition sizes:
 * packets are sorted by estimated size (largest first) and assigned in round-robin
 * so that total load per worker is balanced.
 */
public final class WorkDistributorActor extends AbstractActor {

    private final String jobId;
    private final ActorRef emitterRef;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private Deque<WorkPacket> packetQueue;
    private List<ActorRef> workers;
    private Map<String, Object> distributorConfig;
    private int busyCount;
    private boolean paused;
    private boolean cancelled;

    private WorkDistributorActor(String jobId, ActorRef emitterRef) {
        this.jobId = jobId;
        this.emitterRef = emitterRef;
    }

    public static Props props(String jobId, ActorRef emitterRef) {
        return Props.create(WorkDistributorActor.class, jobId, emitterRef);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(WorkDistributorMessages.StartDistribution.class, this::onStartDistribution)
                .match(WorkDistributorMessages.PacketComplete.class, this::onPacketComplete)
                .match(WorkDistributorMessages.WorkerBatchRead.class, this::onWorkerBatchRead)
                .match(DataEmitterMessages.BatchEmitted.class, this::onBatchEmitted)
                .match(DataEmitterMessages.BatchEmitFailed.class, this::onBatchEmitFailed)
                .match(WorkDistributorMessages.PauseDistribution.class, this::onPause)
                .match(WorkDistributorMessages.ResumeDistribution.class, this::onResume)
                .match(WorkDistributorMessages.CancelDistribution.class, this::onCancel)
                .matchAny(msg -> log.warning("Unhandled: {}", msg))
                .build();
    }

    private void onStartDistribution(WorkDistributorMessages.StartDistribution msg) {
        if (cancelled) return;
        Map<String, Object> config = msg.config() != null ? msg.config() : Map.of();
        this.distributorConfig = config;
        int workerCount = Math.max(1, msg.workerCount());
        List<WorkPacket> packets = WorkPacketFactory.createPackets(config);
        this.packetQueue = new ArrayDeque<>(packets);
        this.busyCount = 0;
        this.paused = false;
        this.workers = new ArrayList<>(workerCount);
        for (int i = 0; i < workerCount; i++) {
            String wid = "worker-" + i;
            ActorRef w = getContext().actorOf(WorkPacketWorkerActor.props(wid), wid);
            workers.add(w);
        }
        log.info("WorkDistributor [{}] started with {} packets, {} workers", jobId, packets.size(), workerCount);
        assignNextPackets();
    }

    private void assignNextPackets() {
        if (paused || cancelled || packetQueue == null || distributorConfig == null) return;
        for (ActorRef worker : workers) {
            if (packetQueue.isEmpty()) break;
            WorkPacket packet = packetQueue.poll();
            busyCount++;
            worker.tell(new WorkDistributorMessages.AssignPacket(jobId, packet, distributorConfig), getSelf());
        }
    }

    private void onPacketComplete(WorkDistributorMessages.PacketComplete msg) {
        busyCount--;
        if (cancelled) {
            if (busyCount <= 0) {
                getContext().getParent().tell(new WorkDistributorMessages.AllWorkComplete(jobId), getSelf());
            }
            return;
        }
        if (!paused && packetQueue != null && !packetQueue.isEmpty()) {
            WorkPacket next = packetQueue.poll();
            busyCount++;
            getSender().tell(new WorkDistributorMessages.AssignPacket(jobId, next, distributorConfig), getSelf());
        }
        if (busyCount <= 0 && (packetQueue == null || packetQueue.isEmpty())) {
            emitterRef.tell(new DataEmitterMessages.StopEmitting(), getSelf());
            getContext().getParent().tell(new WorkDistributorMessages.AllWorkComplete(jobId), getSelf());
            log.info("WorkDistributor [{}] all work complete", jobId);
        }
    }

    private void onWorkerBatchRead(WorkDistributorMessages.WorkerBatchRead msg) {
        if (cancelled) return;
        emitterRef.tell(new DataEmitterMessages.EmitBatch(msg.jobId(), msg.records()), getSelf());
    }

    private void onBatchEmitted(DataEmitterMessages.BatchEmitted msg) {
        getContext().getParent().tell(msg, getSelf());
    }

    private void onBatchEmitFailed(DataEmitterMessages.BatchEmitFailed msg) {
        getContext().getParent().tell(msg, getSelf());
    }

    private void onPause(WorkDistributorMessages.PauseDistribution msg) {
        paused = true;
    }

    private void onResume(WorkDistributorMessages.ResumeDistribution msg) {
        paused = false;
        assignNextPackets();
    }

    private void onCancel(WorkDistributorMessages.CancelDistribution msg) {
        cancelled = true;
        if (workers != null) {
            for (ActorRef w : workers) {
                w.tell(msg, getSelf());
            }
        }
        if (busyCount <= 0) {
            getContext().getParent().tell(new WorkDistributorMessages.AllWorkComplete(jobId), getSelf());
        }
    }
}
