package com.example.replay.storage;

import com.example.replay.model.ReplayJob;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link ReplayJobRepository}. Replace with PostgreSQL when needed.
 */
public final class InMemoryReplayJobRepository implements ReplayJobRepository {

    private final ConcurrentHashMap<String, ReplayJob> store = new ConcurrentHashMap<>();

    @Override
    public ReplayJob save(ReplayJob job) {
        store.put(job.getJobId(), job);
        return job;
    }

    @Override
    public Optional<ReplayJob> findById(String jobId) {
        return Optional.ofNullable(store.get(jobId));
    }

    @Override
    public List<ReplayJob> findAll() {
        return List.copyOf(store.values());
    }
}
