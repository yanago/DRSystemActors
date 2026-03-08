package com.example.replay.actors;

import com.example.replay.actors.messages.JobManagerMessages;
import com.example.replay.model.JobProgress;
import com.example.replay.model.ReplayJob;
import com.example.replay.storage.InMemoryReplayJobRepository;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests: large-scale replay job creation, control, and status/metrics.
 */
class LargeScaleReplayJobTest {

    private static ActorSystem system;

    @BeforeAll
    static void setup() {
        system = ActorSystem.create("LargeScaleReplayJobTest");
    }

    @AfterAll
    static void teardown() {
        TestKit.shutdownActorSystem(system);
    }

    @Test
    void largeScaleJobCreationAndControl() {
        new TestKit(system) {{
            InMemoryReplayJobRepository repo = new InMemoryReplayJobRepository();
            ActorRef manager = system.actorOf(JobManager.props(repo), "manager-large-" + System.currentTimeMillis());

            int totalCount = 5_000;
            String jobId = "large-job-1";
            manager.tell(new JobManagerMessages.CreateJob(jobId, Map.of(
                    "source", "kafka",
                    "total_count", totalCount,
                    "batch_size", 1000,
                    "partition_aware", true,
                    "worker_count", 2
            )), getRef());
            expectNoMessage();

            manager.tell(new JobManagerMessages.GetJobStatus(jobId), getRef());
            JobManagerMessages.JobStatusResponse status = expectMsgClass(JobManagerMessages.JobStatusResponse.class);
            assertEquals(jobId, status.jobId());
            assertNotNull(status.status());
            assertTrue(status.status() == ReplayJob.ReplayJobStatus.RUNNING || status.status() == ReplayJob.ReplayJobStatus.COMPLETED);

            manager.tell(new JobManagerMessages.JobLifecycleCommand(jobId, JobManagerMessages.JobLifecycleCommand.LifecycleCommand.PAUSE), getRef());
            expectMsgClass(JobManagerMessages.CommandAccepted.class);

            manager.tell(new JobManagerMessages.GetJobStatus(jobId), getRef());
            JobManagerMessages.JobStatusResponse paused = expectMsgClass(JobManagerMessages.JobStatusResponse.class);
            assertTrue(paused.status() == ReplayJob.ReplayJobStatus.PAUSED || paused.status() == ReplayJob.ReplayJobStatus.COMPLETED,
                    "Job may complete before pause is applied: " + paused.status());
            if (paused.progress() != null) {
                assertTrue(paused.progress().getEventsProcessed() >= 0);
            }

            manager.tell(new JobManagerMessages.JobLifecycleCommand(jobId, JobManagerMessages.JobLifecycleCommand.LifecycleCommand.CANCEL), getRef());
            expectMsgClass(JobManagerMessages.CommandAccepted.class);
            manager.tell(new JobManagerMessages.GetJobStatus(jobId), getRef());
            JobManagerMessages.JobStatusResponse cancelled = expectMsgClass(JobManagerMessages.JobStatusResponse.class);
            assertTrue(cancelled.status() == ReplayJob.ReplayJobStatus.CANCELLED || cancelled.status() == ReplayJob.ReplayJobStatus.COMPLETED);
        }};
    }

    @Test
    void jobMetricsReflectProgress() {
        new TestKit(system) {{
            InMemoryReplayJobRepository repo = new InMemoryReplayJobRepository();
            ActorRef manager = system.actorOf(JobManager.props(repo), "manager-metrics-" + System.currentTimeMillis());

            String jobId = "metrics-job-1";
            manager.tell(new JobManagerMessages.CreateJob(jobId, Map.of(
                    "source", "kafka",
                    "total_count", 500,
                    "batch_size", 500
            )), getRef());
            expectNoMessage();

            Duration timeout = Duration.ofSeconds(8);
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            boolean completed = false;
            while (System.currentTimeMillis() < deadline) {
                manager.tell(new JobManagerMessages.GetJobStatus(jobId), getRef());
                JobManagerMessages.JobStatusResponse r = expectMsgClass(Duration.ofSeconds(2), JobManagerMessages.JobStatusResponse.class);
                if (r.status() == ReplayJob.ReplayJobStatus.COMPLETED) {
                    assertNotNull(r.progress());
                    assertEquals(500, r.progress().getEventsProcessed());
                    if (r.metrics() != null) {
                        assertTrue(r.metrics().getEventsPerSecond() >= 0);
                        assertTrue(r.metrics().getErrorCount() >= 0);
                    }
                    completed = true;
                    break;
                }
            }
            assertTrue(completed, "Job should complete and report metrics");
        }};
    }
}
