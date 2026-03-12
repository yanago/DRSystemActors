package com.example.replay.datalake;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergEventReaderTest {

    private static final String AVRO_SCHEMA_JSON = """
            {
              "type": "record",
              "name": "SecurityEvent",
              "fields": [
                {"name": "cid", "type": "string"},
                {"name": "event_timestamp", "type": "long"},
                {"name": "event_time", "type": "long"},
                {"name": "event_type", "type": "string"},
                {"name": "event_id", "type": "string"}
              ]
            }
            """;

    @TempDir
    Path tempDir;

    @Test
    void icebergSourceReadsAppendedParquetDataInBatches() throws Exception {
        Path tablePath = createLocalIcebergTableWithData(3);
        ReplayEventSource source = ReplayEventSourceFactory.create(Map.of(
                EventBatch.SOURCE_TYPE_KEY, EventBatch.SOURCE_TYPE_ICEBERG,
                EventBatch.ICEBERG_TABLE_PATH_KEY, tablePath.toString(),
                EventBatch.BATCH_SIZE_KEY, 2
        ));

        int count = 0;
        int batchCount = 0;
        while (source.hasMore()) {
            EventBatch batch = source.nextBatch();
            count += batch.events().size();
            batchCount++;
            if (batch.lastBatch()) {
                break;
            }
        }
        source.close();

        assertEquals(3, count);
        assertEquals(2, batchCount);
    }

    @Test
    void icebergWorkPacketFactoryProducesDefaultPacketForUnpartitionedTable() throws Exception {
        Path tablePath = createLocalIcebergTableWithData(4);
        List<WorkPacket> packets = WorkPacketFactory.createPackets(Map.of(
                EventBatch.SOURCE_TYPE_KEY, EventBatch.SOURCE_TYPE_ICEBERG,
                EventBatch.ICEBERG_TABLE_PATH_KEY, tablePath.toString()
        ));

        assertNotNull(packets);
        assertFalse(packets.isEmpty());
        assertEquals("default", packets.get(0).getPartitionId());
        assertTrue(packets.get(0).getEstimatedEventCount() >= 4);
    }

    private Path createLocalIcebergTableWithData(int recordCount) throws Exception {
        Path tablePath = tempDir.resolve("iceberg-table");
        Files.createDirectories(tablePath);

        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "cid", Types.StringType.get()),
                Types.NestedField.required(2, "event_timestamp", Types.LongType.get()),
                Types.NestedField.required(3, "event_time", Types.LongType.get()),
                Types.NestedField.required(4, "event_type", Types.StringType.get()),
                Types.NestedField.required(5, "event_id", Types.StringType.get())
        );

        HadoopTables tables = new HadoopTables(new Configuration());
        Table table = tables.create(icebergSchema, PartitionSpec.unpartitioned(), tablePath.toString());

        Path parquetPath = tempDir.resolve("source-data.parquet");
        writeParquetFile(parquetPath, recordCount);

        DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(parquetPath.toAbsolutePath().toString())
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(Files.size(parquetPath))
                .withRecordCount(recordCount)
                .build();

        table.newAppend().appendFile(dataFile).commit();
        return tablePath;
    }

    private void writeParquetFile(Path parquetPath, int recordCount) throws IOException {
        Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                new org.apache.hadoop.fs.Path(parquetPath.toUri()))
                .withSchema(avroSchema)
                .withConf(new Configuration())
                .build()) {
            GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
            long baseTs = 1_730_000_000_000L;
            for (int i = 0; i < recordCount; i++) {
                GenericRecord record = builder
                        .set("cid", "cid-" + i)
                        .set("event_timestamp", baseTs + i)
                        .set("event_time", baseTs + i)
                        .set("event_type", "LOGIN")
                        .set("event_id", "evt-" + i)
                        .build();
                writer.write(record);
            }
        }
    }
}
