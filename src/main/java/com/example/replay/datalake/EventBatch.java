package com.example.replay.datalake;

import java.util.List;

/**
 * One page of events from a replay source (Iceberg/Delta/simulated/Parquet).
 */
public record EventBatch(List<Object> events, boolean lastBatch) {

    public static final String BATCH_SIZE_KEY = "batch_size";
    public static final String TOTAL_COUNT_KEY = "total_count";
    public static final String SOURCE_TYPE_KEY = "source_type";
    public static final String SOURCE_TYPE_SIMULATED = "simulated";
    public static final String SOURCE_TYPE_PARQUET = "parquet";
    public static final String PARQUET_PATH_KEY = "parquet_path";
    public static final String PARTITION_DAY_KEY = "partition_day";

    public EventBatch {
        events = events != null ? List.copyOf(events) : List.of();
    }

    public static int numberFromConfig(Object o, int defaultVal) {
        if (o == null) return defaultVal;
        if (o instanceof Number n) return n.intValue();
        try {
            return Integer.parseInt(String.valueOf(o));
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }
}
