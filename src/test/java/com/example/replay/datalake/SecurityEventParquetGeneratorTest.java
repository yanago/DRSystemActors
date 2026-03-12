package com.example.replay.datalake;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SecurityEventParquetGeneratorTest {

    @TempDir
    Path tempDir;

    @Test
    void generatedParquetUsesRequiredFieldsAndIsReadable() throws Exception {
        Path outputDir = tempDir.resolve("parquet-events");
        new SecurityEventParquetGenerator(3, 1, outputDir).generate();

        ReplayEventSource source = ReplayEventSourceFactory.create(Map.of(
                EventBatch.SOURCE_TYPE_KEY, EventBatch.SOURCE_TYPE_PARQUET,
                EventBatch.PARQUET_PATH_KEY, outputDir.toString(),
                EventBatch.BATCH_SIZE_KEY, 2
        ));

        List<Map<String, Object>> records = new ArrayList<>();
        while (source.hasMore()) {
            EventBatch batch = source.nextBatch();
            for (Object event : batch.events()) {
                assertInstanceOf(Map.class, event);
                @SuppressWarnings("unchecked")
                Map<String, Object> record = (Map<String, Object>) event;
                records.add(record);
            }
            if (batch.lastBatch()) {
                break;
            }
        }
        source.close();

        assertEquals(3, records.size());

        Map<String, Object> record = records.get(0);
        assertNotNull(record.get("cid"));
        assertNotNull(record.get("event_timestamp"));
        assertNotNull(record.get("event_time"));
        assertNotNull(record.get("event_type"));
        assertNotNull(record.get("event_id"));

        assertInstanceOf(String.class, record.get("cid"));
        assertInstanceOf(String.class, record.get("event_timestamp"));
        assertInstanceOf(Long.class, record.get("event_time"));
        assertInstanceOf(String.class, record.get("event_type"));
        assertInstanceOf(String.class, record.get("event_id"));

        Instant.parse((String) record.get("event_timestamp"));
        UUID.fromString((String) record.get("event_id"));
        assertTrue(((String) record.get("cid")).startsWith("customer-"));
        assertTrue(((Long) record.get("event_time")) > 0);
    }
}
