package com.example.replay.datalake;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides partition metadata for Iceberg tables by grouping planned scan files
 * using Iceberg partition data where available.
 */
public final class IcebergPartitionMetadata implements PartitionMetadataProvider {

    private static final String DEFAULT_PARTITION_FIELD = "day";

    public static List<PartitionInfo> getPartitionsFromConfig(Map<String, Object> config) throws IOException {
        Map<String, Object> cfg = config != null ? config : Map.of();
        Object tablePathObj = cfg.get(EventBatch.ICEBERG_TABLE_PATH_KEY);
        if (tablePathObj == null || String.valueOf(tablePathObj).isBlank()) {
            return List.of();
        }

        String tablePath = String.valueOf(tablePathObj).trim();
        String partitionField = cfg.containsKey(EventBatch.ICEBERG_PARTITION_FIELD_KEY)
                ? String.valueOf(cfg.get(EventBatch.ICEBERG_PARTITION_FIELD_KEY)).trim()
                : DEFAULT_PARTITION_FIELD;
        String partitionValue = cfg.containsKey(EventBatch.PARTITION_DAY_KEY)
                ? String.valueOf(cfg.get(EventBatch.PARTITION_DAY_KEY)).trim()
                : null;

        HadoopTables tables = new HadoopTables(new org.apache.hadoop.conf.Configuration());
        Table table = tables.load(tablePath);
        TableScan scan = table.newScan();
        if (partitionValue != null && !partitionValue.isBlank()) {
            scan = scan.filter(Expressions.equal(partitionField, partitionValue));
        }

        Map<String, Long> counts = new LinkedHashMap<>();
        try (org.apache.iceberg.io.CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                String partitionId = normalizePartitionId(task.file().partition() != null
                        ? task.file().partition().toString()
                        : null, partitionField);
                long estimate = task.file().recordCount() > 0
                        ? task.file().recordCount()
                        : Math.max(1L, task.file().fileSizeInBytes() / 500L);
                counts.merge(partitionId, estimate, Long::sum);
            }
        }

        List<PartitionInfo> out = new ArrayList<>(counts.size());
        counts.forEach((partitionId, count) -> out.add(new PartitionInfo(partitionId, count)));
        if (out.isEmpty()) {
            out.add(new PartitionInfo("default", 0));
        }
        return out;
    }

    static String normalizePartitionId(String partition, String partitionField) {
        if (partition == null
                || partition.isBlank()
                || "null".equalsIgnoreCase(partition)
                || "PartitionData{}".equals(partition)) {
            return "default";
        }
        String[] parts = partition.split(",");
        String expected = partitionField + "=";
        for (String part : parts) {
            String trimmed = part.trim();
            if (trimmed.startsWith(expected)) {
                return trimmed.substring(expected.length());
            }
        }
        return partition;
    }

    @Override
    public List<PartitionInfo> getPartitions(Map<String, Object> config) {
        try {
            return getPartitionsFromConfig(config);
        } catch (IOException e) {
            throw new RuntimeException("Failed to list Iceberg partitions: " + e.getMessage(), e);
        }
    }
}
