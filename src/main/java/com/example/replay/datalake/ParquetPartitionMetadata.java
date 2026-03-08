package com.example.replay.datalake;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Provides partition metadata for Parquet layout: day=yyyy-MM-dd with estimated size by file count/size.
 */
public final class ParquetPartitionMetadata implements PartitionMetadataProvider {

    private static final long DEFAULT_ESTIMATE_PER_FILE = 10_000L;

    private final Path basePath;

    public ParquetPartitionMetadata(Path basePath) {
        this.basePath = Objects.requireNonNull(basePath);
    }

    public static List<PartitionInfo> getPartitionsFromConfig(Map<String, Object> config) throws IOException {
        Object pathObj = config.get(EventBatch.PARQUET_PATH_KEY);
        if (pathObj == null || pathObj.toString().isBlank()) {
            return List.of();
        }
        Path base = Path.of(pathObj.toString());
        if (!Files.isDirectory(base)) {
            return List.of(new PartitionInfo("default", DEFAULT_ESTIMATE_PER_FILE));
        }
        List<PartitionInfo> out = new ArrayList<>();
        try (Stream<Path> list = Files.list(base)) {
            list.filter(Files::isDirectory)
                    .filter(p -> p.getFileName().toString().startsWith("day="))
                    .forEach(dayDir -> {
                        String partitionId = dayDir.getFileName().toString().substring("day=".length());
                        long est = estimatePartitionSize(dayDir);
                        out.add(new PartitionInfo(partitionId, est));
                    });
        }
        if (out.isEmpty()) {
            try (Stream<Path> walk = Files.list(base)) {
                long count = walk.filter(p -> p.toString().toLowerCase().endsWith(".parquet")).count();
                if (count > 0) {
                    out.add(new PartitionInfo("default", count * DEFAULT_ESTIMATE_PER_FILE));
                }
            }
        }
        return out;
    }

    private static long estimatePartitionSize(Path partitionDir) {
        try {
            long totalBytes = 0;
            try (Stream<Path> files = Files.list(partitionDir)) {
                List<Path> list = files.filter(f -> f.toString().toLowerCase().endsWith(".parquet")).toList();
                for (Path p : list) {
                    totalBytes += Files.size(p);
                }
            }
            if (totalBytes > 0) {
                return Math.max(1, totalBytes / 500);
            }
            return DEFAULT_ESTIMATE_PER_FILE;
        } catch (IOException e) {
            return DEFAULT_ESTIMATE_PER_FILE;
        }
    }

    @Override
    public List<PartitionInfo> getPartitions(Map<String, Object> config) {
        try {
            return getPartitionsFromConfig(config);
        } catch (IOException e) {
            throw new RuntimeException("Failed to list Parquet partitions: " + e.getMessage(), e);
        }
    }
}
