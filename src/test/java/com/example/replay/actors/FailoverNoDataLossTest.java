package com.example.replay.actors;

import com.example.replay.actors.messages.JobManagerMessages;
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
 * Validates that pause/resume/cancel work without data loss (no duplicate or missing events).
 */
class FailoverNoDataLossTest {

    private static ActorSystem system;

    @BeforeAll
    static void setup() {
        system = ActorSystem.create("FailoverNoDataLossTest");
    }

    @AfterAll
    static void teardown() {
        TestKit.shutdownActorSystem(system);
    }

    @Test
    void runToCompletionEmitsExactTotalNoLoss() {
        new TestKit(system) {{
            InMemoryReplayJobRepository repo = new InMemoryReplayJobRepository();
            ActorRef manager = system.actorOf(JobManager.props(repo), "manager-complete-" + System.currentTimeMillis());

            int totalCount = 1_000;
            String jobId = "complete-job-1";
            manager.tell(new JobManagerMessages.CreateJob(jobId, Map.of(
                    "source", "kafka",
                    "total_count", totalCount,
                    "batch_size", 250
            )), getRef());
            expectNoMessage();

            Duration timeout = Duration.ofSeconds(15);
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            boolean completed = false;
            while (System.currentTimeMillis() < deadline) {
                manager.tell(new JobManagerMessages.GetJobStatus(jobId), getRef());
                JobManagerMessages.JobStatusResponse r = expectMsgClass(Duration.ofSeconds(2), JobManagerMessages.JobStatusResponse.class);
                if (r.status() == ReplayJob.ReplayJobStatus.COMPLETED) {
                    assertNotNull(r.progress(), "Progress should be present when completed");
                    assertEquals(totalCount, r.progress().getEventsProcessed(),
                            "All events must be emitted exactly once (no data loss, no duplicates)");
                    completed = true;
                    break;
                }
            }
            assertTrue(completed, "Job did not complete within timeout");
        }};
    }

    @Test
    void pauseResumeThenCompleteNoDataLoss() {
        new TestKit(system) {{
            InMemoryReplayJobRepository repo = new InMemoryReplayJobRepository();
            ActorRef manager = system.actorOf(JobManager.props(repo), "manager-pause-resume-" + System.currentTimeMillis());

            int totalCount = 800;
            String jobId = "pause-resume-job-1";
            manager.tell(new JobManagerMessages.CreateJob(jobId, Map.of(
                    "source", "kafka",
                    "total_count", totalCount,
                    "batch_size", 200
            )), getRef());
            expectNoMessage();

            manager.tell(new JobManagerMessages.JobLifecycleCommand(jobId, JobManagerMessages.JobLifecycleCommand.LifecycleCommand.PAUSE), getRef());
            expectMsgClass(JobManagerMessages.CommandAccepted.class);

            manager.tell(new JobManagerMessages.JobLifecycleCommand(jobId, JobManagerMessages.JobLifecycleCommand.LifecycleCommand.RESUME), getRef());
            expectMsgClass(JobManagerMessages.CommandAccepted.class);

            Duration timeout = Duration.ofSeconds(15);
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            boolean completed = false;
            while (System.currentTimeMillis() < deadline) {
                manager.tell(new JobManagerMessages.GetJobStatus(jobId), getRef());
                JobManagerMessages.JobStatusResponse r = expectMsgClass(Duration.ofSeconds(2), JobManagerMessages.JobStatusResponse.class);
                if (r.status() == ReplayJob.ReplayJobStatus.COMPLETED) {
                    assertNotNull(r.progress());
                    assertEquals(totalCount, r.progress().getEventsProcessed(),
                            "After pause/resume, total emitted must equal total (no loss, no duplicates)");
                    completed = true;
                    break;
                }
            }
            assertTrue(completed, "Job did not complete after resume within timeout");
        }};
    }

    @Test
    void cancelDoesNotExceedTotal() {
        new TestKit(system) {{
            InMemoryReplayJobRepository repo = new InMemoryReplayJobRepository();
            ActorRef manager = system.actorOf(JobManager.props(repo), "manager-cancel-" + System.currentTimeMillis());

            int totalCount = 10_000;
            String jobId = "cancel-job-1";
            manager.tell(new JobManagerMessages.CreateJob(jobId, Map.of(
                    "source", "kafka",
                    "total_count", totalCount,
                    "batch_size", 1000
            )), getRef());
            expectNoMessage();

            manager.tell(new JobManagerMessages.JobLifecycleCommand(jobId, JobManagerMessages.JobLifecycleCommand.LifecycleCommand.CANCEL), getRef());
            expectMsgClass(JobManagerMessages.CommandAccepted.class);

            manager.tell(new JobManagerMessages.GetJobStatus(jobId), getRef());
            JobManagerMessages.JobStatusResponse r = expectMsgClass(Duration.ofSeconds(3), JobManagerMessages.JobStatusResponse.class);
            assertTrue(r.status() == ReplayJob.ReplayJobStatus.CANCELLED || r.status() == ReplayJob.ReplayJobStatus.COMPLETED);
            if (r.progress() != null) {
                assertTrue(r.progress().getEventsProcessed() <= totalCount,
                        "Emitted count must never exceed total (no duplicates on cancel)");
            }
        }};
    }
}
