package com.example.replay.datalake;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimulatedEventCatalogTest {

    @Test
    void producesConfiguredTotalInBatches() {
        int total = 55_000;
        int batchSize = 10_000;
        ReplayEventSource source = SimulatedEventCatalog.fromConfig(Map.of(
                EventBatch.TOTAL_COUNT_KEY, total,
                EventBatch.BATCH_SIZE_KEY, batchSize
        ));
        int count = 0;
        int batchCount = 0;
        EventBatch batch;
        while (source.hasMore()) {
            batch = source.nextBatch();
            count += batch.events().size();
            batchCount++;
            if (batch.lastBatch()) break;
        }
        source.close();
        assertEquals(total, count);
        assertTrue(batchCount >= 5);
        assertTrue(batchCount <= 6);
    }

    @Test
    void lastBatchHasLastBatchTrue() {
        ReplayEventSource source = new SimulatedEventCatalog(100, 250, "t");
        EventBatch b1 = source.nextBatch();
        EventBatch b2 = source.nextBatch();
        EventBatch b3 = source.nextBatch();
        assertFalse(b1.lastBatch());
        assertFalse(b2.lastBatch());
        assertTrue(b3.lastBatch());
        assertEquals(50, b3.events().size());
        source.close();
    }
}
