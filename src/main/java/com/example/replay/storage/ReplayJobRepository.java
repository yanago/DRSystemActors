package com.example.replay.storage;

import com.example.replay.model.ReplayJob;

import java.util.List;
import java.util.Optional;

/**
 * Persistence for replay jobs. Implementations: in-memory ({@link InMemoryReplayJobRepository}) or PostgreSQL ({@link PostgresReplayJobRepository}).
 */
public interface ReplayJobRepository {

    ReplayJob save(ReplayJob job);

    Optional<ReplayJob> findById(String jobId);

    List<ReplayJob> findAll();
}
