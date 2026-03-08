package com.example.replay.datalake;

import java.util.List;
import java.util.Map;

/**
 * Supplies partition metadata (id + estimated size) for work distribution.
 * Implementations may reflect real datalake partitions (e.g. day=) or virtual partitions (simulated).
 */
public interface PartitionMetadataProvider {

    /**
     * Returns partitions for the given job config, with estimated event counts.
     * Order may be used for distribution (e.g. largest first for better balance).
     */
    List<PartitionInfo> getPartitions(Map<String, Object> config);
}
