package com.example.replay.datalake;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Virtual partitions for simulated source: by day and optionally by customer skew
 * (heavy / medium / light buckets) so work packets reflect skewed load.
 */
public final class SimulatedPartitionMetadata implements PartitionMetadataProvider {

    private static final int DEFAULT_NUM_DAYS = 7;
    /** Share of events in "heavy" customer bucket (first 5 cids). */
    private static final double HEAVY_FRACTION = 0.65;
    /** Share in "medium" bucket (next 10 cids). */
    private static final double MEDIUM_FRACTION = 0.25;
    /** Share in "light" bucket (rest). */
    private static final double LIGHT_FRACTION = 0.10;

    @Override
    public List<PartitionInfo> getPartitions(Map<String, Object> config) {
        int totalCount = EventBatch.numberFromConfig(
                config != null ? config.get(EventBatch.TOTAL_COUNT_KEY) : null, 50_000);
        boolean useCustomerSkew = config != null && Boolean.TRUE.equals(config.get("partition_by_skew"));
        if (useCustomerSkew) {
            return partitionsByCustomerSkew(totalCount);
        }
        return partitionsByDay(totalCount, config);
    }

    private static List<PartitionInfo> partitionsByDay(int totalCount, Map<String, Object> config) {
        int numDays = config != null
                ? EventBatch.numberFromConfig(config.get("num_days"), DEFAULT_NUM_DAYS)
                : DEFAULT_NUM_DAYS;
        numDays = Math.max(1, Math.min(numDays, 31));
        List<PartitionInfo> out = new ArrayList<>(numDays);
        long perDay = totalCount / numDays;
        long remainder = totalCount % numDays;
        for (int i = 0; i < numDays; i++) {
            long count = perDay + (i < remainder ? 1 : 0);
            out.add(new PartitionInfo("day-" + i, count));
        }
        return out;
    }

    private static List<PartitionInfo> partitionsByCustomerSkew(int totalCount) {
        List<PartitionInfo> out = new ArrayList<>(3);
        out.add(new PartitionInfo("heavy", (long) (totalCount * HEAVY_FRACTION)));
        out.add(new PartitionInfo("medium", (long) (totalCount * MEDIUM_FRACTION)));
        out.add(new PartitionInfo("light", (long) (totalCount * LIGHT_FRACTION)));
        return out;
    }
}
