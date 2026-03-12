package com.example.replay.actors;

import com.example.replay.actors.messages.WorkDistributorMessages;
import com.example.replay.datalake.WorkPacket;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.TestActorRef;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WorkDistributorActorTest {

    private static ActorSystem system;

    @BeforeAll
    static void setup() {
        system = ActorSystem.create("WorkDistributorActorTest");
    }

    @AfterAll
    static void teardown() {
        TestKit.shutdownActorSystem(system);
    }

    @Test
    void resumeAssignsQueuedPacketsToIdleWorkers() throws Exception {
        new TestKit(system) {{
            TestActorRef<WorkDistributorActor> distributor = TestActorRef.create(
                    system,
                    WorkDistributorActor.props("job-1", getRef()),
                    "work-distributor-" + System.currentTimeMillis());

            WorkPacket queuedPacket = WorkPacket.partition("day=2025-03-01", 100);
            setField(distributor.underlyingActor(), "packetQueue", new ArrayDeque<>(List.of(queuedPacket)));
            setField(distributor.underlyingActor(), "workers", List.of(getRef()));
            setField(distributor.underlyingActor(), "distributorConfig", Map.of("source", "simulated"));
            setField(distributor.underlyingActor(), "busyCount", 0);
            setField(distributor.underlyingActor(), "paused", true);
            setField(distributor.underlyingActor(), "cancelled", false);

            distributor.tell(new WorkDistributorMessages.ResumeDistribution(), getRef());

            WorkDistributorMessages.AssignPacket assigned = expectMsgClass(WorkDistributorMessages.AssignPacket.class);
            assertEquals("job-1", assigned.jobId());
            assertEquals(queuedPacket, assigned.packet());
        }};
    }

    @Test
    void packetCompletionDoesNotAssignNewWorkWhilePaused() throws Exception {
        new TestKit(system) {{
            TestActorRef<WorkDistributorActor> distributor = TestActorRef.create(
                    system,
                    WorkDistributorActor.props("job-2", getRef()),
                    "work-distributor-paused-" + System.currentTimeMillis());
            TestKit worker = new TestKit(system);

            WorkPacket queuedPacket = WorkPacket.partition("day=2025-03-02", 200);
            setField(distributor.underlyingActor(), "packetQueue", new ArrayDeque<>(List.of(queuedPacket)));
            setField(distributor.underlyingActor(), "workers", List.of(worker.getRef()));
            setField(distributor.underlyingActor(), "distributorConfig", Map.of("source", "simulated"));
            setField(distributor.underlyingActor(), "busyCount", 1);
            setField(distributor.underlyingActor(), "paused", true);
            setField(distributor.underlyingActor(), "cancelled", false);

            distributor.tell(new WorkDistributorMessages.PacketComplete("worker-0"), worker.getRef());

            worker.expectNoMessage(Duration.ofMillis(200));
            assertEquals(1, ((ArrayDeque<?>) getField(distributor.underlyingActor(), "packetQueue")).size());

            distributor.tell(new WorkDistributorMessages.ResumeDistribution(), getRef());

            WorkDistributorMessages.AssignPacket assigned = worker.expectMsgClass(WorkDistributorMessages.AssignPacket.class);
            assertEquals(queuedPacket, assigned.packet());
        }};
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
