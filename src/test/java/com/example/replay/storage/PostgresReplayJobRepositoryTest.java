package com.example.replay.storage;

import com.example.replay.model.ReplayJob;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for Postgres repository. Require Docker.
 * Run with: mvn test -Dtest=PostgresReplayJobRepositoryTest (with Docker available),
 * or remove @Disabled to run with the rest of the suite.
 */
@Testcontainers
@Disabled("Requires Docker; enable when testing Postgres integration")
class PostgresReplayJobRepositoryTest {

    @Container
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("replay")
            .withUsername("replay")
            .withPassword("replay");

    @Test
    void saveAndFindById() {
        DataSource ds = DataSourceConfig.createAndMigrate(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword()
        );
        PostgresReplayJobRepository repo = new PostgresReplayJobRepository(ds);

        Instant now = Instant.now();
        ReplayJob job = new ReplayJob(
                "job-pg-1",
                ReplayJob.ReplayJobStatus.PENDING,
                Map.of("source", "kafka", "name", "test"),
                now,
                now,
                null
        );
        repo.save(job);

        ReplayJob found = repo.findById("job-pg-1").orElseThrow();
        assertEquals(job.getJobId(), found.getJobId());
        assertEquals(job.getStatus(), found.getStatus());
        assertEquals(job.getParameters(), found.getParameters());
    }

    @Test
    void findAllReturnsSavedJobs() {
        DataSource ds = DataSourceConfig.createAndMigrate(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword()
        );
        PostgresReplayJobRepository repo = new PostgresReplayJobRepository(ds);

        Instant now = Instant.now();
        repo.save(new ReplayJob("j1", ReplayJob.ReplayJobStatus.PENDING, Map.of("source", "a"), now, now, null));
        repo.save(new ReplayJob("j2", ReplayJob.ReplayJobStatus.RUNNING, Map.of("source", "b"), now, now, null));

        List<ReplayJob> all = repo.findAll();
        assertTrue(all.size() >= 2);
        assertTrue(all.stream().anyMatch(j -> "j1".equals(j.getJobId())));
        assertTrue(all.stream().anyMatch(j -> "j2".equals(j.getJobId())));
    }

    @Test
    void findByIdReturnsEmptyWhenNotFound() {
        DataSource ds = DataSourceConfig.createAndMigrate(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword()
        );
        PostgresReplayJobRepository repo = new PostgresReplayJobRepository(ds);
        assertFalse(repo.findById("nonexistent-id").isPresent());
    }
}
