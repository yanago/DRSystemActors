package com.example.replay.datalake;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Reads events from Parquet files with optional partition pruning by day.
 * Path layout: basePath / [day=yyyy-MM-dd/] *.parquet or basePath / *.parquet.
 */
public final class ParquetEventReader implements ReplayEventSource {

    private final Path basePath;
    private final String partitionDay;
    private final int batchSize;
    private final List<Path> files;
    private int fileIndex;
    private ParquetReader<GenericRecord> currentReader;
    private boolean closed;
    private boolean lastBatchSent;

    public ParquetEventReader(Path basePath, String partitionDay, int batchSize) throws IOException {
        this.basePath = Objects.requireNonNull(basePath);
        this.partitionDay = partitionDay;
        this.batchSize = Math.max(1, batchSize);
        this.files = listParquetFiles(basePath, partitionDay);
        this.fileIndex = 0;
        this.currentReader = null;
        this.closed = false;
        this.lastBatchSent = files.isEmpty();
    }

    public static ParquetEventReader fromConfig(Map<String, Object> config) throws IOException {
        Object pathObj = config.get(EventBatch.PARQUET_PATH_KEY);
        if (pathObj == null || pathObj.toString().isBlank()) {
            throw new IllegalArgumentException("parquet_path required for parquet source");
        }
        Path base = Path.of(pathObj.toString());
        String day = config.containsKey(EventBatch.PARTITION_DAY_KEY)
                ? String.valueOf(config.get(EventBatch.PARTITION_DAY_KEY))
                : null;
        int batchSize = config.containsKey(EventBatch.BATCH_SIZE_KEY)
                ? EventBatch.numberFromConfig(config.get(EventBatch.BATCH_SIZE_KEY), 5000)
                : 5000;
        return new ParquetEventReader(base, day, batchSize);
    }

    private static List<Path> listParquetFiles(Path base, String partitionDay) throws IOException {
        List<Path> result = new ArrayList<>();
        if (!Files.isDirectory(base)) {
            if (base.toString().toLowerCase().endsWith(".parquet")) {
                result.add(base);
            }
            return result;
        }
        if (partitionDay != null && !partitionDay.isBlank()) {
            Path dayPath = base.resolve("day=" + partitionDay);
            if (Files.exists(dayPath)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(dayPath, "*.parquet")) {
                    for (Path p : stream) result.add(p);
                }
                return result;
            }
        }
        try (Stream<Path> walk = Files.walk(base, 4)) {
            walk.filter(p -> p.toString().toLowerCase().endsWith(".parquet"))
                    .forEach(result::add);
        }
        return result;
    }

    private ParquetReader<GenericRecord> openNext() throws IOException {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }
        if (fileIndex >= files.size()) return null;
        Path f = files.get(fileIndex++);
        return AvroParquetReader.<GenericRecord>builder(new org.apache.hadoop.fs.Path(f.toUri()))
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
                    if (currentReader == null) break;
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
            if (batch.isEmpty()) lastBatchSent = true;
            return new EventBatch(batch, last);
        } catch (IOException e) {
            lastBatchSent = true;
            return new EventBatch(batch, true);
        }
    }

    private static Object genericRecordToMap(GenericRecord record) {
        if (record == null) return null;
        Map<String, Object> map = new LinkedHashMap<>();
        record.getSchema().getFields().forEach(f -> map.put(f.name(), normalizeValue(record.get(f.name()))));
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
