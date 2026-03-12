package com.example.replay.datalake;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Generates 50k+ security event records with customer skew and writes them
 * to a folder in Parquet format with datalake-style day partitions (day=yyyy-MM-dd).
 * <p>
 * Customer skew: the first 5 customers get ~65% of events, the next 10 get ~35%,
 * so a small set of customers is much heavier than the rest.
 * <p>
 * Run via Maven (default: 52k events, 7 days, output under target/parquet-events):
 * <pre>
 *   mvn exec:java -Pgenerate-events
 * </pre>
 * On Java 23+ you may need: {@code MAVEN_OPTS="-Djava.security.manager=allow" mvn exec:java -Pgenerate-events}
 * <p>
 * Or with custom output and count: pass args as outputDir [totalRecords] [numDays].
 * Example: {@code target/my-events 100000 14}
 */
public final class SecurityEventParquetGenerator {

    private static final String AVRO_SCHEMA_JSON = """
            {
              "type": "record",
              "name": "SecurityEvent",
              "fields": [
                {"name": "cid", "type": "string"},
                {"name": "event_timestamp", "type": "string"},
                {"name": "event_time", "type": "long"},
                {"name": "event_type", "type": "string"},
                {"name": "event_id", "type": "string"}
              ]
            }
            """;

    private static final String[] EVENT_TYPES = {
            "ProcessStart", "NetworkConnect", "FileAccess", "ProcessExit", "DnsQuery", "AuthenticationSuccess"
    };
    private static final int DEFAULT_TOTAL_RECORDS = 50_000;
    private static final int DEFAULT_NUM_DAYS = 7;
    private static final int NUM_CUSTOMERS = 50;
    /** Top 5 customers get ~55% of events, next 10 get ~25%, rest ~20%. */
    private static final double[] SKEW_WEIGHTS = {0.35, 0.12, 0.08, 0.05, 0.05, 0.08, 0.06, 0.04, 0.04, 0.03, 0.02, 0.02, 0.02, 0.02, 0.02};

    private final int totalRecords;
    private final int numDays;
    private final java.nio.file.Path outputDir;
    private final Random random;
    private final Schema avroSchema;

    public SecurityEventParquetGenerator(int totalRecords, int numDays, java.nio.file.Path outputDir) {
        this.totalRecords = Math.max(1, totalRecords);
        this.numDays = Math.max(1, numDays);
        this.outputDir = Objects.requireNonNull(outputDir);
        this.random = new Random(42);
        this.avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
    }

    /**
     * Generates events with customer skew and writes partitioned Parquet under outputDir.
     * Partition layout: outputDir/day=yyyy-MM-dd/part-00000.parquet, ...
     */
    public void generate() throws IOException {
        Files.createDirectories(outputDir);
        List<String> customerIds = customerIds();
        double[] cumulativeWeights = cumulativeSkewWeights();

        // Assign each record to (dayIndex, cid) using skew for cid
        Map<String, List<RecordSpec>> byDay = new TreeMap<>();
        LocalDate startDate = LocalDate.of(2025, 3, 1);
        long eventIdBase = 1;

        for (int i = 0; i < totalRecords; i++) {
            int dayIndex = random.nextInt(numDays);
            String dayKey = startDate.plusDays(dayIndex).toString();
            String cid = pickCustomer(customerIds, cumulativeWeights);
            String eventType = EVENT_TYPES[random.nextInt(EVENT_TYPES.length)];
            long tsMillis = startDate.plusDays(dayIndex).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()
                    + random.nextInt(86400_000);
            String eventTimestamp = Instant.ofEpochMilli(tsMillis).toString();
            byDay.computeIfAbsent(dayKey, k -> new ArrayList<>())
                    .add(new RecordSpec(cid, eventTimestamp, tsMillis, eventType, deterministicEventId(cid, tsMillis, eventIdBase + i)));
        }

        Configuration conf = new Configuration();
        int partIndex = 0;
        for (Map.Entry<String, List<RecordSpec>> e : byDay.entrySet()) {
            String day = e.getKey();
            List<RecordSpec> records = e.getValue();
            java.nio.file.Path dayDir = outputDir.resolve("day=" + day);
            Files.createDirectories(dayDir);
            org.apache.hadoop.fs.Path parquetPath = new org.apache.hadoop.fs.Path(dayDir.resolve("part-" + String.format("%05d", partIndex++) + ".parquet").toUri().toString());
            writeParquetFile(parquetPath, records, conf);
        }
    }

    private List<String> customerIds() {
        List<String> ids = new ArrayList<>(NUM_CUSTOMERS);
        for (int i = 0; i < NUM_CUSTOMERS; i++) {
            ids.add("customer-" + String.format("%06d", i));
        }
        return ids;
    }

    private static double[] cumulativeSkewWeights() {
        double[] cum = new double[SKEW_WEIGHTS.length];
        cum[0] = SKEW_WEIGHTS[0];
        for (int i = 1; i < SKEW_WEIGHTS.length; i++) {
            cum[i] = cum[i - 1] + SKEW_WEIGHTS[i];
        }
        return cum;
    }

    private String pickCustomer(List<String> customerIds, double[] cumulativeWeights) {
        double r = random.nextDouble();
        int bucket = 0;
        for (int i = 0; i < cumulativeWeights.length; i++) {
            if (r <= cumulativeWeights[i]) {
                bucket = i;
                break;
            }
            bucket = i;
        }
        int customerIndex = Math.min(bucket, customerIds.size() - 1);
        return customerIds.get(customerIndex);
    }

    private void writeParquetFile(org.apache.hadoop.fs.Path outputPath, List<RecordSpec> records, Configuration conf) throws IOException {
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputPath)
                .withSchema(avroSchema)
                .withConf(conf)
                .build()) {
            GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
            for (RecordSpec r : records) {
                GenericRecord record = builder
                        .set("cid", r.cid)
                        .set("event_timestamp", r.eventTimestamp)
                        .set("event_time", r.eventTime)
                        .set("event_type", r.eventType)
                        .set("event_id", r.eventId)
                        .build();
                writer.write(record);
            }
        }
    }

    private static String deterministicEventId(String cid, long timestampMillis, long index) {
        String seed = cid + ":" + timestampMillis + ":" + index;
        return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
    }


    private record RecordSpec(String cid, String eventTimestamp, long eventTime, String eventType, String eventId) {}

    // --- CLI ---

    public static void main(String[] args) throws IOException {
        if (args.length == 1) {
            if (args[0].contains(",")) {
                args = args[0].split(",");
            } else if (args[0].contains(" ")) {
                args = args[0].split("\\s+");
            }
        }
        String outArg = args.length > 0 ? args[0].trim() : "target/parquet-events";
        int total = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_TOTAL_RECORDS;
        int days = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_NUM_DAYS;

        java.nio.file.Path outputDir = java.nio.file.Path.of(outArg);
        SecurityEventParquetGenerator gen = new SecurityEventParquetGenerator(total, days, outputDir);
        gen.generate();
        System.out.println("Generated " + total + " security events into " + outputDir.toAbsolutePath() + " (partitions by day over " + days + " days).");
    }
}
