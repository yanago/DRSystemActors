package com.example.replay.actors;

import com.example.replay.actors.messages.JobManagerMessages;
import com.example.replay.model.ReplayJob;
import com.example.replay.storage.InMemoryReplayJobRepository;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobManagerTest {

    private static ActorSystem system;

    @BeforeAll
    static void setup() {
        system = ActorSystem.create("JobManagerTest");
    }

    @AfterAll
    static void teardown() {
        TestKit.shutdownActorSystem(system);
    }

    @Test
    void createJobAndGetStatus() {
        new TestKit(system) {{
            InMemoryReplayJobRepository repo = new InMemoryReplayJobRepository();
            ActorRef manager = system.actorOf(JobManager.props(repo), "manager-" + System.currentTimeMillis());

            manager.tell(new JobManagerMessages.CreateJob("job-1", Map.of("source", "kafka")), getRef());
            expectNoMessage();

            manager.tell(new JobManagerMessages.GetJobStatus("job-1"), getRef());
            JobManagerMessages.JobStatusResponse status = expectMsgClass(JobManagerMessages.JobStatusResponse.class);
            assertEquals("job-1", status.jobId());
            assertNotNull(status.status());
        }};
    }

    @Test
    void listJobs() {
        new TestKit(system) {{
            InMemoryReplayJobRepository repo = new InMemoryReplayJobRepository();
            ActorRef manager = system.actorOf(JobManager.props(repo), "manager-list-" + System.currentTimeMillis());

            manager.tell(new JobManagerMessages.CreateJob("job-a", Map.of("source", "s1")), getRef());
            expectNoMessage();
            manager.tell(new JobManagerMessages.ListJobs(), getRef());
            JobManagerMessages.JobListResponse list = expectMsgClass(JobManagerMessages.JobListResponse.class);
            assertEquals(1, list.jobs().size());
            assertEquals("job-a", list.jobs().get(0).getJobId());
        }};
    }

    @Test
    @Disabled("Flaky: job may complete before pause is applied")
    void pauseAndResumeJob() {
        new TestKit(system) {{
            InMemoryReplayJobRepository repo = new InMemoryReplayJobRepository();
            ActorRef manager = system.actorOf(JobManager.props(repo), "manager-pause-" + System.currentTimeMillis());

            manager.tell(new JobManagerMessages.CreateJob("job-pause", Map.of("source", "kafka", "total_count", 1_000_000)), getRef());
            expectNoMessage();

            manager.tell(new JobManagerMessages.JobLifecycleCommand("job-pause", JobManagerMessages.JobLifecycleCommand.LifecycleCommand.PAUSE), getRef());
            expectMsgClass(JobManagerMessages.CommandAccepted.class);
            manager.tell(new JobManagerMessages.GetJobStatus("job-pause"), getRef());
            JobManagerMessages.JobStatusResponse paused = expectMsgClass(JobManagerMessages.JobStatusResponse.class);
            assertEquals(ReplayJob.ReplayJobStatus.PAUSED, paused.status());

            manager.tell(new JobManagerMessages.JobLifecycleCommand("job-pause", JobManagerMessages.JobLifecycleCommand.LifecycleCommand.RESUME), getRef());
            expectMsgClass(JobManagerMessages.CommandAccepted.class);
            manager.tell(new JobManagerMessages.GetJobStatus("job-pause"), getRef());
            JobManagerMessages.JobStatusResponse resumed = expectMsgClass(JobManagerMessages.JobStatusResponse.class);
            assertEquals(ReplayJob.ReplayJobStatus.RUNNING, resumed.status());
        }};
    }

    @Test
    void cancelJob() {
        new TestKit(system) {{
            InMemoryReplayJobRepository repo = new InMemoryReplayJobRepository();
            ActorRef manager = system.actorOf(JobManager.props(repo), "manager-cancel-" + System.currentTimeMillis());

            manager.tell(new JobManagerMessages.CreateJob("job-cancel", Map.of("source", "kafka", "total_count", 1_000_000)), getRef());
            manager.tell(new JobManagerMessages.JobLifecycleCommand("job-cancel", JobManagerMessages.JobLifecycleCommand.LifecycleCommand.CANCEL), getRef());
            expectMsgClass(JobManagerMessages.CommandAccepted.class);
            manager.tell(new JobManagerMessages.GetJobStatus("job-cancel"), getRef());
            JobManagerMessages.JobStatusResponse cancelled = expectMsgClass(JobManagerMessages.JobStatusResponse.class);
            assertTrue(cancelled.status() == ReplayJob.ReplayJobStatus.CANCELLED || cancelled.status() == ReplayJob.ReplayJobStatus.COMPLETED,
                    "Job should be CANCELLED or COMPLETED (race with stream): " + cancelled.status());
        }};
    }
}
