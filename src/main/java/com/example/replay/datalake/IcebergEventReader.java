package com.example.replay.datalake;

import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Reads events from an Apache Iceberg table by planning data files through the
 * Iceberg API and then reading Parquet-backed files in batches.
 * <p>
 * This implementation supports Hadoop-table style Iceberg layouts and optional
 * partition pruning by a configured partition field (default: {@code day}).
 */
public final class IcebergEventReader implements ReplayEventSource {

    private static final String DEFAULT_PARTITION_FIELD = "day";

    private final List<Path> files;
    private final int batchSize;
    private int fileIndex;
    private ParquetReader<GenericRecord> currentReader;
    private boolean closed;
    private boolean lastBatchSent;

    public IcebergEventReader(List<Path> files, int batchSize) {
        this.files = files != null ? List.copyOf(files) : List.of();
        this.batchSize = Math.max(1, batchSize);
        this.fileIndex = 0;
        this.currentReader = null;
        this.closed = false;
        this.lastBatchSent = this.files.isEmpty();
    }

    public static IcebergEventReader fromConfig(Map<String, Object> config) {
        Map<String, Object> cfg = config != null ? config : Map.of();
        Object tablePathObj = cfg.get(EventBatch.ICEBERG_TABLE_PATH_KEY);
        if (tablePathObj == null || String.valueOf(tablePathObj).isBlank()) {
            throw new IllegalArgumentException("iceberg_table_path required for iceberg source");
        }

        String tablePath = String.valueOf(tablePathObj).trim();
        int batchSize = EventBatch.numberFromConfig(cfg.get(EventBatch.BATCH_SIZE_KEY), 5000);
        String partitionField = cfg.containsKey(EventBatch.ICEBERG_PARTITION_FIELD_KEY)
                ? String.valueOf(cfg.get(EventBatch.ICEBERG_PARTITION_FIELD_KEY)).trim()
                : DEFAULT_PARTITION_FIELD;
        String partitionDay = cfg.containsKey(EventBatch.PARTITION_DAY_KEY)
                ? String.valueOf(cfg.get(EventBatch.PARTITION_DAY_KEY)).trim()
                : null;

        return new IcebergEventReader(planDataFiles(tablePath, partitionField, partitionDay), batchSize);
    }

    static List<Path> planDataFiles(String tablePath, String partitionField, String partitionValue) {
        try {
            HadoopTables tables = new HadoopTables(new org.apache.hadoop.conf.Configuration());
            Table table = tables.load(tablePath);
            TableScan scan = table.newScan();
            if (partitionValue != null && !partitionValue.isBlank() && partitionField != null && !partitionField.isBlank()) {
                scan = scan.filter(Expressions.equal(partitionField, partitionValue));
            }

            List<Path> out = new ArrayList<>();
            try (org.apache.iceberg.io.CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
                for (FileScanTask task : tasks) {
                    out.add(toPath(task.file().path().toString()));
                }
            }
            return out;
        } catch (IOException e) {
            throw new RuntimeException("Failed to plan Iceberg data files: " + e.getMessage(), e);
        }
    }

    private static Path toPath(String rawPath) {
        Objects.requireNonNull(rawPath, "rawPath");
        try {
            if (rawPath.startsWith("file:")) {
                return Path.of(URI.create(rawPath));
            }
            return Path.of(rawPath);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unsupported Iceberg file path: " + rawPath, e);
        }
    }

    private ParquetReader<GenericRecord> openNext() throws IOException {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }
        if (fileIndex >= files.size()) {
            return null;
        }
        Path file = files.get(fileIndex++);
        return AvroParquetReader.<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.toUri()))
                .withConf(new org.apache.hadoop.conf.Configuration())
                .build();
    }

    @Override
    public EventBatch nextBatch() {
        if (closed || lastBatchSent) {
            return new EventBatch(List.of(), true);
        }
        List<Object> batch = new ArrayList<>(batchSize);
        try {
            while (batch.size() < batchSize) {
                if (currentReader == null) {
                    currentReader = openNext();
                    if (currentReader == null) {
                        break;
                    }
                }
                GenericRecord record = currentReader.read();
                if (record == null) {
                    currentReader.close();
                    currentReader = null;
                    continue;
                }
                batch.add(genericRecordToMap(record));
            }
            boolean last = currentReader == null && fileIndex >= files.size();
            if (last) {
                lastBatchSent = true;
            }
            return new EventBatch(batch, last);
        } catch (IOException e) {
            lastBatchSent = true;
            return new EventBatch(batch, true);
        }
    }

    private static Object genericRecordToMap(GenericRecord record) {
        if (record == null) {
            return null;
        }
        Map<String, Object> map = new LinkedHashMap<>();
        record.getSchema().getFields().forEach(field -> map.put(field.name(), normalizeValue(record.get(field.name()))));
        return map;
    }

    private static Object normalizeValue(Object value) {
        if (value instanceof CharSequence chars) {
            return chars.toString();
        }
        return value;
    }

    @Override
    public boolean hasMore() {
        return !closed && !lastBatchSent;
    }

    @Override
    public void close() {
        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (IOException ignored) {
            }
            currentReader = null;
        }
        closed = true;
    }
}
