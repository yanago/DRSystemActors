package com.example.replay.kafka;

import com.example.replay.model.SecurityEvent;

import java.util.Map;

/**
 * Extracts customer id (cid) from a record for use as Kafka partition key.
 * Ensures heavy customers consistently use the same key so they land in the same partition.
 */
public final class CustomerPartitionKey {

    private static final String CID_FIELD = "cid";

    private CustomerPartitionKey() {
    }

    /**
     * Returns the partition key for this record (cid). Never null; falls back to empty string.
     */
    public static String keyFor(Object record) {
        if (record == null) return "";
        if (record instanceof SecurityEvent evt) {
            String cid = evt.getCid();
            return cid != null ? cid : "";
        }
        if (record instanceof Map<?, ?> map) {
            Object cid = map.get(CID_FIELD);
            return cid != null ? cid.toString() : "";
        }
        return "";
    }
}
