package org.apache.iceberg.rust.compat;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Creates test tables using Java Iceberg for compatibility testing with Rust implementation.
 */
public class CreateTestTables {

    private final String baseDataPath;
    private final String javaCreatedPath;

    public CreateTestTables(String baseDataPath) {
        this.baseDataPath = baseDataPath;
        this.javaCreatedPath = Paths.get(baseDataPath, "java_created").toString();
    }

    public static void main(String[] args) throws Exception {
        String baseDataPath = args.length > 0
            ? args[0]
            : "crates/integration_tests/tests/java_compat/data";

        CreateTestTables creator = new CreateTestTables(baseDataPath);

        System.out.println("=== Creating Java Iceberg Test Tables ===");
        System.out.println("Output directory: " + creator.javaCreatedPath);
        System.out.println();

        creator.createAllTestTables();

        System.out.println("\n=== Test Table Creation Complete ===");
    }

    public void createAllTestTables() throws Exception {
        // Test 1: Basic unpartitioned table with simple data
        createBasicTable();

        // Test 2: Partitioned table
        createPartitionedTable();

        // Test 3: Table with position deletes
        createTableWithPositionDeletes();

        // Test 4: Table with multiple snapshots
        createTableWithMultipleSnapshots();

        // Test 5: Table after DELETE operation
        createTableAfterDelete();

        // Test 6: Table after OVERWRITE operation
        createTableAfterOverwrite();

        // Test 7: Table with schema evolution
        createTableWithSchemaEvolution();

        // Test 8: Table with all data types
        createTableWithAllDataTypes();
    }

    /**
     * Test 1: Basic unpartitioned table
     */
    private void createBasicTable() throws Exception {
        System.out.println("Creating Test 1: Basic unpartitioned table...");

        String tablePath = Paths.get(javaCreatedPath, "test1_basic_table").toString();
        cleanDirectory(tablePath);

        // Define schema
        Schema schema = new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "name", Types.StringType.get()),
            optional(3, "value", Types.DoubleType.get())
        );

        // Create table
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = createTable(tablePath, schema, spec);

        // Write data
        List<Record> records = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            Record record = GenericRecord.create(schema);
            record.setField("id", (long) i);
            record.setField("name", "name_" + i);
            record.setField("value", i * 1.5);
            records.add(record);
        }

        String dataFile = writeDataFile(table, schema, spec, records, null);

        // Commit
        table.newAppend()
            .appendFile(DataFiles.builder(spec)
                .withPath(dataFile)
                .withFileSizeInBytes(new File(dataFile).length())
                .withRecordCount(records.size())
                .build())
            .commit();

        System.out.println("  ✓ Created table with " + records.size() + " rows");
        System.out.println("  ✓ Snapshot ID: " + table.currentSnapshot().snapshotId());
        System.out.println();
    }

    /**
     * Test 2: Partitioned table
     */
    private void createPartitionedTable() throws Exception {
        System.out.println("Creating Test 2: Partitioned table...");

        String tablePath = Paths.get(javaCreatedPath, "test2_partitioned_table").toString();
        cleanDirectory(tablePath);

        Schema schema = new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "date", Types.DateType.get()),
            optional(3, "category", Types.StringType.get()),
            optional(4, "value", Types.DoubleType.get())
        );

        PartitionSpec spec = PartitionSpec.builderFor(schema)
            .day("date")
            .identity("category")
            .build();

        Table table = createTable(tablePath, schema, spec);

        // Write data for multiple partitions
        String[] categories = {"A", "B", "C"};
        int baseDate = 19000; // 2022-01-01 in days since epoch

        for (String category : categories) {
            for (int dayOffset = 0; dayOffset < 3; dayOffset++) {
                List<Record> records = new ArrayList<>();
                int date = baseDate + dayOffset;

                for (int i = 0; i < 20; i++) {
                    Record record = GenericRecord.create(schema);
                    record.setField("id", (long) (category.charAt(0) * 1000 + dayOffset * 100 + i));
                    record.setField("date", date);
                    record.setField("category", category);
                    record.setField("value", i * 2.5);
                    records.add(record);
                }

                PartitionData partition = new PartitionData(spec.partitionType());
                partition.set(0, dayOffset); // day transform
                partition.set(1, category);  // identity

                String dataFile = writeDataFile(table, schema, spec, records, partition);

                table.newAppend()
                    .appendFile(DataFiles.builder(spec)
                        .withPath(dataFile)
                        .withFileSizeInBytes(new File(dataFile).length())
                        .withPartition(partition)
                        .withRecordCount(records.size())
                        .build())
                    .commit();
            }
        }

        System.out.println("  ✓ Created partitioned table with 9 partitions (3 categories × 3 days)");
        System.out.println("  ✓ Total records: 180");
        System.out.println("  ✓ Snapshot ID: " + table.currentSnapshot().snapshotId());
        System.out.println();
    }

    /**
     * Test 3: Table with position deletes
     */
    private void createTableWithPositionDeletes() throws Exception {
        System.out.println("Creating Test 3: Table with position deletes...");

        String tablePath = Paths.get(javaCreatedPath, "test3_position_deletes").toString();
        cleanDirectory(tablePath);

        Schema schema = new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get())
        );

        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = createTable(tablePath, schema, spec);

        // Write initial data
        List<Record> records = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            Record record = GenericRecord.create(schema);
            record.setField("id", (long) i);
            record.setField("data", "data_" + i);
            records.add(record);
        }

        String dataFile = writeDataFile(table, schema, spec, records, null);

        table.newAppend()
            .appendFile(DataFiles.builder(spec)
                .withPath(dataFile)
                .withFileSizeInBytes(new File(dataFile).length())
                .withRecordCount(records.size())
                .build())
            .commit();

        // Create position deletes for rows 10-14 (positions 9-13 in 0-indexed)
        String deleteFile = writePositionDeleteFile(table, schema, spec, dataFile,
            Arrays.asList(9L, 10L, 11L, 12L, 13L));

        table.newRowDelta()
            .addDeletes(FileMetadata.deleteFileBuilder(spec)
                .ofPositionDeletes()
                .withPath(deleteFile)
                .withFileSizeInBytes(new File(deleteFile).length())
                .withRecordCount(5)
                .build())
            .commit();

        System.out.println("  ✓ Created table with 50 rows");
        System.out.println("  ✓ Added position deletes for 5 rows (positions 9-13)");
        System.out.println("  ✓ Effective row count: 45");
        System.out.println("  ✓ Snapshot ID: " + table.currentSnapshot().snapshotId());
        System.out.println();
    }

    /**
     * Test 4: Table with multiple snapshots
     */
    private void createTableWithMultipleSnapshots() throws Exception {
        System.out.println("Creating Test 4: Table with multiple snapshots...");

        String tablePath = Paths.get(javaCreatedPath, "test4_multiple_snapshots").toString();
        cleanDirectory(tablePath);

        Schema schema = new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "batch", Types.IntegerType.get())
        );

        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = createTable(tablePath, schema, spec);

        // Create 5 snapshots
        for (int batch = 1; batch <= 5; batch++) {
            List<Record> records = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                Record record = GenericRecord.create(schema);
                record.setField("id", (long) ((batch - 1) * 20 + i + 1));
                record.setField("batch", batch);
                records.add(record);
            }

            String dataFile = writeDataFile(table, schema, spec, records, null);

            table.newAppend()
                .appendFile(DataFiles.builder(spec)
                    .withPath(dataFile)
                    .withFileSizeInBytes(new File(dataFile).length())
                    .withRecordCount(records.size())
                    .build())
                .commit();

            // Small delay to ensure different timestamps
            Thread.sleep(10);
        }

        System.out.println("  ✓ Created 5 snapshots");
        System.out.println("  ✓ Total records: 100");
        System.out.println("  ✓ Current snapshot ID: " + table.currentSnapshot().snapshotId());
        System.out.println();
    }

    /**
     * Test 5: Table after DELETE operation
     */
    private void createTableAfterDelete() throws Exception {
        System.out.println("Creating Test 5: Table after DELETE operation...");

        String tablePath = Paths.get(javaCreatedPath, "test5_after_delete").toString();
        cleanDirectory(tablePath);

        Schema schema = new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "category", Types.StringType.get()),
            optional(3, "value", Types.IntegerType.get())
        );

        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = createTable(tablePath, schema, spec);

        // Write initial data
        List<Record> records = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            Record record = GenericRecord.create(schema);
            record.setField("id", (long) i);
            record.setField("category", i % 2 == 0 ? "even" : "odd");
            record.setField("value", i);
            records.add(record);
        }

        String dataFile = writeDataFile(table, schema, spec, records, null);

        table.newAppend()
            .appendFile(DataFiles.builder(spec)
                .withPath(dataFile)
                .withFileSizeInBytes(new File(dataFile).length())
                .withRecordCount(records.size())
                .build())
            .commit();

        // Delete even rows (50 rows)
        List<Long> deletePositions = new ArrayList<>();
        for (long i = 1; i < 100; i += 2) { // positions 1, 3, 5, ... (0-indexed)
            deletePositions.add(i);
        }

        String deleteFile = writePositionDeleteFile(table, schema, spec, dataFile, deletePositions);

        table.newRowDelta()
            .addDeletes(FileMetadata.deleteFileBuilder(spec)
                .ofPositionDeletes()
                .withPath(deleteFile)
                .withFileSizeInBytes(new File(deleteFile).length())
                .withRecordCount(deletePositions.size())
                .build())
            .commit();

        System.out.println("  ✓ Created table with 100 rows");
        System.out.println("  ✓ Deleted 50 even-numbered rows");
        System.out.println("  ✓ Effective row count: 50");
        System.out.println("  ✓ Snapshot ID: " + table.currentSnapshot().snapshotId());
        System.out.println();
    }

    /**
     * Test 6: Table after OVERWRITE operation
     */
    private void createTableAfterOverwrite() throws Exception {
        System.out.println("Creating Test 6: Table after OVERWRITE operation...");

        String tablePath = Paths.get(javaCreatedPath, "test6_after_overwrite").toString();
        cleanDirectory(tablePath);

        Schema schema = new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "partition_col", Types.StringType.get()),
            optional(3, "value", Types.IntegerType.get())
        );

        PartitionSpec spec = PartitionSpec.builderFor(schema)
            .identity("partition_col")
            .build();

        Table table = createTable(tablePath, schema, spec);

        // Write initial data for partitions A, B, C
        String[] partitions = {"A", "B", "C"};
        List<DataFile> initialFiles = new ArrayList<>();

        for (String part : partitions) {
            List<Record> records = new ArrayList<>();
            for (int i = 1; i <= 30; i++) {
                Record record = GenericRecord.create(schema);
                record.setField("id", (long) (part.charAt(0) * 100 + i));
                record.setField("partition_col", part);
                record.setField("value", i);
                records.add(record);
            }

            PartitionData partition = new PartitionData(spec.partitionType());
            partition.set(0, part);

            String dataFile = writeDataFile(table, schema, spec, records, partition);

            DataFile df = DataFiles.builder(spec)
                .withPath(dataFile)
                .withFileSizeInBytes(new File(dataFile).length())
                .withPartition(partition)
                .withRecordCount(records.size())
                .build();

            initialFiles.add(df);
        }

        // Append all initial files
        AppendFiles append = table.newAppend();
        for (DataFile df : initialFiles) {
            append.appendFile(df);
        }
        append.commit();

        // Now OVERWRITE partition B with new data
        List<Record> newRecords = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            Record record = GenericRecord.create(schema);
            record.setField("id", (long) (9000 + i));
            record.setField("partition_col", "B");
            record.setField("value", i * 10);
            newRecords.add(record);
        }

        PartitionData partitionB = new PartitionData(spec.partitionType());
        partitionB.set(0, "B");

        String newDataFile = writeDataFile(table, schema, spec, newRecords, partitionB);

        // Perform overwrite
        OverwriteFiles overwrite = table.newOverwrite();
        overwrite.overwriteByRowFilter(
            org.apache.iceberg.expressions.Expressions.equal("partition_col", "B")
        );
        overwrite.addFile(DataFiles.builder(spec)
            .withPath(newDataFile)
            .withFileSizeInBytes(new File(newDataFile).length())
            .withPartition(partitionB)
            .withRecordCount(newRecords.size())
            .build());
        overwrite.commit();

        System.out.println("  ✓ Created partitioned table with 3 partitions (90 rows total)");
        System.out.println("  ✓ Overwrote partition B (30 rows → 20 rows)");
        System.out.println("  ✓ Final row count: 80 (30 + 20 + 30)");
        System.out.println("  ✓ Snapshot ID: " + table.currentSnapshot().snapshotId());
        System.out.println();
    }

    /**
     * Test 7: Table with schema evolution
     */
    private void createTableWithSchemaEvolution() throws Exception {
        System.out.println("Creating Test 7: Table with schema evolution...");

        String tablePath = Paths.get(javaCreatedPath, "test7_schema_evolution").toString();
        cleanDirectory(tablePath);

        // Initial schema
        Schema schema1 = new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "name", Types.StringType.get())
        );

        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = createTable(tablePath, schema1, spec);

        // Write data with schema v1
        List<Record> records1 = new ArrayList<>();
        for (int i = 1; i <= 30; i++) {
            Record record = GenericRecord.create(schema1);
            record.setField("id", (long) i);
            record.setField("name", "name_" + i);
            records1.add(record);
        }

        String dataFile1 = writeDataFile(table, schema1, spec, records1, null);

        table.newAppend()
            .appendFile(DataFiles.builder(spec)
                .withPath(dataFile1)
                .withFileSizeInBytes(new File(dataFile1).length())
                .withRecordCount(records1.size())
                .build())
            .commit();

        // Evolve schema: add new column
        table.updateSchema()
            .addColumn("age", Types.IntegerType.get())
            .commit();

        // Write data with schema v2
        Schema schema2 = table.schema();
        List<Record> records2 = new ArrayList<>();
        for (int i = 31; i <= 60; i++) {
            Record record = GenericRecord.create(schema2);
            record.setField("id", (long) i);
            record.setField("name", "name_" + i);
            record.setField("age", 20 + i % 50);
            records2.add(record);
        }

        String dataFile2 = writeDataFile(table, schema2, spec, records2, null);

        table.newAppend()
            .appendFile(DataFiles.builder(spec)
                .withPath(dataFile2)
                .withFileSizeInBytes(new File(dataFile2).length())
                .withRecordCount(records2.size())
                .build())
            .commit();

        System.out.println("  ✓ Created table with schema v1 (id, name)");
        System.out.println("  ✓ Wrote 30 rows");
        System.out.println("  ✓ Evolved schema to v2 (added 'age' column)");
        System.out.println("  ✓ Wrote 30 more rows");
        System.out.println("  ✓ Total rows: 60");
        System.out.println("  ✓ Snapshot ID: " + table.currentSnapshot().snapshotId());
        System.out.println();
    }

    /**
     * Test 8: Table with all data types
     */
    private void createTableWithAllDataTypes() throws Exception {
        System.out.println("Creating Test 8: Table with all data types...");

        String tablePath = Paths.get(javaCreatedPath, "test8_all_data_types").toString();
        cleanDirectory(tablePath);

        Schema schema = new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "bool_col", Types.BooleanType.get()),
            optional(3, "int_col", Types.IntegerType.get()),
            optional(4, "long_col", Types.LongType.get()),
            optional(5, "float_col", Types.FloatType.get()),
            optional(6, "double_col", Types.DoubleType.get()),
            optional(7, "string_col", Types.StringType.get()),
            optional(8, "date_col", Types.DateType.get()),
            optional(9, "timestamp_col", Types.TimestampType.withoutZone()),
            optional(10, "binary_col", Types.BinaryType.get())
        );

        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = createTable(tablePath, schema, spec);

        // Write diverse data
        List<Record> records = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            Record record = GenericRecord.create(schema);
            record.setField("id", (long) i);
            record.setField("bool_col", i % 2 == 0);
            record.setField("int_col", i * 100);
            record.setField("long_col", (long) i * 1000000);
            record.setField("float_col", (float) (i * 1.5));
            record.setField("double_col", i * 2.718281828);
            record.setField("string_col", "test_" + i);
            record.setField("date_col", 19000 + i); // days since epoch
            record.setField("timestamp_col", 1640000000000000L + i * 1000000L); // microseconds
            record.setField("binary_col", ("binary_" + i).getBytes());
            records.add(record);
        }

        String dataFile = writeDataFile(table, schema, spec, records, null);

        table.newAppend()
            .appendFile(DataFiles.builder(spec)
                .withPath(dataFile)
                .withFileSizeInBytes(new File(dataFile).length())
                .withRecordCount(records.size())
                .build())
            .commit();

        System.out.println("  ✓ Created table with all common data types");
        System.out.println("  ✓ Rows: 50");
        System.out.println("  ✓ Snapshot ID: " + table.currentSnapshot().snapshotId());
        System.out.println();
    }

    // Helper methods

    private Table createTable(String path, Schema schema, PartitionSpec spec) throws IOException {
        Files.createDirectories(Paths.get(path));

        return new BaseTable(
            new TableOperations() {
                private TableMetadata metadata = TableMetadata.newTableMetadata(
                    schema, spec, path, new HashMap<>()
                );

                @Override
                public TableMetadata current() {
                    return metadata;
                }

                @Override
                public TableMetadata refresh() {
                    return metadata;
                }

                @Override
                public void commit(TableMetadata base, TableMetadata newMetadata) {
                    this.metadata = newMetadata;

                    try {
                        // Write metadata file
                        String metadataFile = path + "/metadata/v" + newMetadata.lastSequenceNumber() + ".metadata.json";
                        Files.createDirectories(Paths.get(path, "metadata"));
                        TableMetadataParser.write(newMetadata,
                            org.apache.iceberg.io.OutputFile.of(metadataFile));
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to write metadata", e);
                    }
                }

                @Override
                public org.apache.iceberg.io.FileIO io() {
                    return new org.apache.iceberg.io.ResolvingFileIO();
                }

                @Override
                public String metadataFileLocation(String fileName) {
                    return path + "/metadata/" + fileName;
                }

                @Override
                public LocationProvider locationProvider() {
                    return LocationProviders.locationsFor(path, metadata.properties());
                }
            },
            path
        );
    }

    private String writeDataFile(Table table, Schema schema, PartitionSpec spec,
                                  List<Record> records, PartitionData partition) throws IOException {
        String filename = UUID.randomUUID().toString() + ".parquet";
        String filepath = partition == null
            ? Paths.get(table.location(), "data", filename).toString()
            : Paths.get(table.location(), "data", partition.toString(), filename).toString();

        Files.createDirectories(Paths.get(filepath).getParent());

        OutputFile outputFile = table.io().newOutputFile(filepath);

        FileAppender<Record> appender = Parquet.write(outputFile)
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .build();

        try {
            appender.addAll(records);
        } finally {
            appender.close();
        }

        return filepath;
    }

    private String writePositionDeleteFile(Table table, Schema schema, PartitionSpec spec,
                                            String dataFilePath, List<Long> positions) throws IOException {
        String filename = UUID.randomUUID().toString() + "-deletes.parquet";
        String filepath = Paths.get(table.location(), "data", filename).toString();

        Files.createDirectories(Paths.get(filepath).getParent());

        OutputFile outputFile = table.io().newOutputFile(filepath);

        GenericAppenderFactory appenderFactory = new GenericAppenderFactory(schema, spec);
        PositionDeleteWriter<Record> writer = appenderFactory.newPosDeleteWriter(
            outputFile, table.io().newInputFile(dataFilePath), spec, null
        );

        try {
            for (Long pos : positions) {
                writer.delete(dataFilePath, pos);
            }
        } finally {
            writer.close();
        }

        return filepath;
    }

    private void cleanDirectory(String path) throws IOException {
        Path dirPath = Paths.get(path);
        if (Files.exists(dirPath)) {
            Files.walk(dirPath)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        }
    }
}
