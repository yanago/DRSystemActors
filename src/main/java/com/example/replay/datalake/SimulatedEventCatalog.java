package com.example.replay.datalake;

import com.example.replay.model.SecurityEvent;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * In-memory / simulated catalog that yields 50k+ events in batches for demo.
 * Events are logically partitioned by day; batch size and total count are configurable.
 */
public final class SimulatedEventCatalog implements ReplayEventSource {

    private final int batchSize;
    private final int totalCount;
    private final String cidPrefix;
    private int emitted;
    private boolean closed;

    public SimulatedEventCatalog(int batchSize, int totalCount, String cidPrefix) {
        this.batchSize = Math.max(1, batchSize);
        this.totalCount = Math.max(0, totalCount);
        this.cidPrefix = cidPrefix != null ? cidPrefix : "sim";
    }

    public static SimulatedEventCatalog fromConfig(Map<String, Object> config) {
        int batchSize = config.containsKey(EventBatch.BATCH_SIZE_KEY)
                ? EventBatch.numberFromConfig(config.get(EventBatch.BATCH_SIZE_KEY), 5000)
                : 5000;
        int totalCount = config.containsKey(EventBatch.TOTAL_COUNT_KEY)
                ? EventBatch.numberFromConfig(config.get(EventBatch.TOTAL_COUNT_KEY), 50_000)
                : 50_000;
        String cidPrefix = config.containsKey("cid_prefix") ? String.valueOf(config.get("cid_prefix")) : "sim";
        return new SimulatedEventCatalog(batchSize, totalCount, cidPrefix);
    }

    @Override
    public EventBatch nextBatch() {
        if (closed || emitted >= totalCount) {
            return new EventBatch(List.of(), true);
        }
        int toEmit = Math.min(batchSize, totalCount - emitted);
        List<Object> batch = new ArrayList<>(toEmit);
        Instant baseTime = LocalDate.of(2025, 3, 1).atStartOfDay().toInstant(ZoneOffset.UTC);
        for (int i = 0; i < toEmit; i++) {
            int idx = emitted + i;
            Instant ts = baseTime.plusSeconds(idx);
            SecurityEvent evt = new SecurityEvent(
                    cidPrefix + "-" + (idx % 100),
                    ts,
                    ts,
                    "LOGIN",
                    "evt-" + idx
            );
            batch.add(evt);
        }
        emitted += toEmit;
        boolean lastBatch = emitted >= totalCount;
        return new EventBatch(batch, lastBatch);
    }

    @Override
    public boolean hasMore() {
        return !closed && emitted < totalCount;
    }

    @Override
    public void close() {
        closed = true;
    }
}
