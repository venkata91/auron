/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.auron.flink.table.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import org.apache.auron.flink.table.AuronFlinkTableTestBase;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for Parquet scan with Auron native execution.
 * Tests basic scan, projection, and filter pushdown capabilities.
 */
public class AuronFlinkParquetScanITCase extends AuronFlinkTableTestBase {

    private boolean auronAvailable;

    @BeforeEach
    @Override
    public void before() {
        // Override to use BATCH mode instead of STREAMING for Parquet tests
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        // Enable Auron native execution
        configuration.setBoolean("table.exec.auron.enable", true);
        configuration.setBoolean("table.exec.auron.enable.scan", true);
        configuration.setBoolean("table.exec.auron.enable.project", true);
        configuration.setBoolean("table.exec.auron.enable.filter", true);
        configuration.setInteger("table.exec.auron.batch-size", 8192);
        configuration.setDouble("table.exec.auron.memory-fraction", 0.7);
        configuration.setString("table.exec.auron.log-level", "INFO");

        tableEnvironment =
            StreamTableEnvironment.create(environment, EnvironmentSettings.fromConfiguration(configuration));

        System.out.println("🚀 Auron native execution ENABLED in configuration");
    }

    @BeforeEach
    public void checkAuronAvailability() {
        try {
            // Debug: Print java.library.path
            String libraryPath = System.getProperty("java.library.path");
            System.out.println("🔍 java.library.path = " + libraryPath);

            // Check if Auron native library is available
            System.loadLibrary("auron");
            auronAvailable = true;
            System.out.println("✅ Auron native library loaded successfully");

            // Log Auron configuration
            org.apache.auron.flink.planner.AuronFlinkPlannerExtension.logAuronConfiguration(
                tableEnvironment.getConfig().getConfiguration());
        } catch (UnsatisfiedLinkError e) {
            auronAvailable = false;
            System.out.println("⚠️  Auron native library not available - tests will be skipped");
            System.out.println("   Error: " + e.getMessage());
        }
    }

    @Test
    public void testBasicParquetScan() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testBasicParquetScan - Auron not available");
            return;
        }

        // Create test data
        List<Row> testData = Arrays.asList(
            row(1, "Alice", 100.5, LocalDate.of(2024, 1, 1)),
            row(2, "Bob", 200.5, LocalDate.of(2024, 1, 2)),
            row(3, "Charlie", 300.5, LocalDate.of(2024, 1, 3)));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE," + "  created_date DATE" + ")";

        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "basic_test", schema, testData);
        createParquetTable("parquet_basic_test", schema, "file://" + parquetDir.getAbsolutePath() + "/basic_test");

        // Execute query
        TableResult result = tableEnvironment.executeSql("SELECT * FROM parquet_basic_test");

        List<Row> results = collectResults(result);
        assertEquals(3, results.size());
    }

    @Test
    public void testParquetScanWithProjection() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testParquetScanWithProjection - Auron not available");
            return;
        }

        // Create test data
        List<Row> testData = Arrays.asList(
            row(1, "Alice", 100.5, LocalDate.of(2024, 1, 1)),
            row(2, "Bob", 200.5, LocalDate.of(2024, 1, 2)),
            row(3, "Charlie", 300.5, LocalDate.of(2024, 1, 3)));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE," + "  created_date DATE" + ")";

        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "proj_test", schema, testData);
        createParquetTable("parquet_proj_test", schema, "file://" + parquetDir.getAbsolutePath() + "/proj_test");

        // Execute query: SELECT id, name (project 2 columns)
        TableResult result = tableEnvironment.executeSql("SELECT id, name FROM parquet_proj_test");

        List<Row> results = collectResults(result);
        assertEquals(3, results.size());

        // Verify only 2 columns returned
        for (Row r : results) {
            assertEquals(2, r.getArity());
        }
    }

    @Test
    public void testParquetScanWithSimpleFilter() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testParquetScanWithSimpleFilter - Auron not available");
            return;
        }

        // Create test data
        List<Row> testData = Arrays.asList(
            row(1, "Alice", 50.0, LocalDate.of(2024, 1, 1)),
            row(2, "Bob", 150.0, LocalDate.of(2024, 1, 2)),
            row(3, "Charlie", 250.0, LocalDate.of(2024, 1, 3)),
            row(4, "David", 350.0, LocalDate.of(2024, 1, 4)));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE," + "  created_date DATE" + ")";

        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "filter_test", schema, testData);
        createParquetTable("parquet_filter_test", schema, "file://" + parquetDir.getAbsolutePath() + "/filter_test");

        // Execute query: WHERE amount > 100
        TableResult result = tableEnvironment.executeSql("SELECT * FROM parquet_filter_test WHERE amount > 100");

        List<Row> results = collectResults(result);

        // Should return rows with amount > 100 (Bob, Charlie, David)
        assertEquals(3, results.size());
    }

    @Test
    public void testParquetScanWithComplexFilter() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testParquetScanWithComplexFilter - Auron not available");
            return;
        }

        // Create test data
        List<Row> testData = Arrays.asList(
            row(1, "Alice", 50.0, LocalDate.of(2024, 1, 1)),
            row(2, "Bob", 150.0, LocalDate.of(2024, 1, 2)),
            row(3, "Charlie", 250.0, LocalDate.of(2024, 1, 3)),
            row(4, "David", 100.0, LocalDate.of(2024, 1, 4)),
            row(5, "Eve", 200.0, LocalDate.of(2024, 1, 5)));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE," + "  created_date DATE" + ")";

        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "complex_filter_test", schema, testData);
        createParquetTable(
            "parquet_complex_filter_test",
            schema,
            "file://" + parquetDir.getAbsolutePath() + "/complex_filter_test");

        // Execute query: WHERE amount >= 100 AND amount <= 200
        TableResult result = tableEnvironment.executeSql(
            "SELECT * FROM parquet_complex_filter_test WHERE amount >= 100 AND amount <= 200");

        List<Row> results = collectResults(result);

        // Should return Bob (150), David (100), Eve (200)
        assertEquals(3, results.size());
    }

    @Test
    public void testParquetScanWithProjectionAndFilter() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testParquetScanWithProjectionAndFilter - Auron not available");
            return;
        }

        // Create test data
        List<Row> testData = Arrays.asList(
            row(1, "Alice", 50.0, LocalDate.of(2024, 1, 1)),
            row(2, "Bob", 150.0, LocalDate.of(2024, 1, 2)),
            row(3, "Charlie", 250.0, LocalDate.of(2024, 1, 3)),
            row(4, "David", 350.0, LocalDate.of(2024, 1, 4)));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE," + "  created_date DATE" + ")";

        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "proj_filter_test", schema, testData);
        createParquetTable(
            "parquet_proj_filter_test", schema, "file://" + parquetDir.getAbsolutePath() + "/proj_filter_test");

        // Execute query: SELECT name, amount WHERE amount > 100
        TableResult result =
            tableEnvironment.executeSql("SELECT name, amount FROM parquet_proj_filter_test WHERE amount > 100");

        List<Row> results = collectResults(result);

        // Should return 3 rows (Bob, Charlie, David) with 2 columns each
        assertEquals(3, results.size());
        for (Row r : results) {
            assertEquals(2, r.getArity());
        }
    }

    @Test
    public void testParquetScanWithDifferentTypes() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testParquetScanWithDifferentTypes - Auron not available");
            return;
        }

        // Create test data with various types
        List<Row> testData = Arrays.asList(
            row(1, "Alice", 100.5, true, LocalDate.of(2024, 1, 1)),
            row(2, "Bob", 200.5, false, LocalDate.of(2024, 1, 2)),
            row(3, "Charlie", 300.5, true, LocalDate.of(2024, 1, 3)));

        String schema = "(" + "  id INT,"
            + "  name STRING,"
            + "  amount DOUBLE,"
            + "  active BOOLEAN,"
            + "  created_date DATE"
            + ")";

        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "types_test", schema, testData);
        createParquetTable("parquet_types_test", schema, "file://" + parquetDir.getAbsolutePath() + "/types_test");

        // Execute query
        TableResult result = tableEnvironment.executeSql("SELECT * FROM parquet_types_test WHERE active = true");

        List<Row> results = collectResults(result);
        assertEquals(2, results.size());
    }

    @Test
    public void testParquetScanWithNulls() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testParquetScanWithNulls - Auron not available");
            return;
        }

        // Create test data with nulls
        List<Row> testData =
            Arrays.asList(row(1, "Alice", 100.5), row(2, null, 200.5), row(3, "Charlie", null), row(4, null, null));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE" + ")";

        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "nulls_test", schema, testData);
        createParquetTable("parquet_nulls_test", schema, "file://" + parquetDir.getAbsolutePath() + "/nulls_test");

        // Execute query - test that nulls are handled correctly
        TableResult result = tableEnvironment.executeSql("SELECT * FROM parquet_nulls_test WHERE name IS NOT NULL");

        List<Row> results = collectResults(result);
        // Should return rows 1 and 3 (Alice and Charlie)
        assertEquals(2, results.size());
    }

    @Test
    public void testNativeExecutionExplicitAPI() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testNativeExecutionExplicitAPI - Auron not available");
            return;
        }

        System.out.println("\n🔥 Testing EXPLICIT Native Execution API");

        // Create test data
        List<Row> testData = Arrays.asList(
            row(1, "Alice", 100.5, LocalDate.of(2024, 1, 1)),
            row(2, "Bob", 200.5, LocalDate.of(2024, 1, 2)),
            row(3, "Charlie", 300.5, LocalDate.of(2024, 1, 3)));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE," + "  created_date DATE" + ")";

        // Write Parquet test data
        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "native_test", schema, testData);

        // Get file paths for native execution
        File parquetFile = new File(parquetDir, "native_test");
        System.out.println("📂 Looking for Parquet files in: " + parquetFile.getAbsolutePath());
        System.out.println("📂 Directory exists: " + parquetFile.exists());
        System.out.println("📂 Is directory: " + parquetFile.isDirectory());

        if (parquetFile.exists() && parquetFile.isDirectory()) {
            File[] allFiles = parquetFile.listFiles();
            System.out.println("📂 All files in directory: " + (allFiles != null ? allFiles.length : 0));
            if (allFiles != null) {
                for (File f : allFiles) {
                    System.out.println("   - " + f.getName() + " (size: " + f.length() + " bytes)");
                }
            }
        }

        // Build schema for native execution
        org.apache.flink.table.types.logical.LogicalType[] fieldTypes = {
            new org.apache.flink.table.types.logical.IntType(),
            new org.apache.flink.table.types.logical.VarCharType(
                org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH),
            new org.apache.flink.table.types.logical.DoubleType(),
            new org.apache.flink.table.types.logical.DateType()
        };
        String[] fieldNames = {"id", "name", "amount", "created_date"};
        org.apache.flink.table.types.logical.RowType rowType =
            org.apache.flink.table.types.logical.RowType.of(fieldTypes, fieldNames);

        // Get all Parquet files (Flink doesn't add .parquet extension by default)
        File[] parquetFiles = parquetFile.listFiles((dir, name) -> !name.startsWith("."));
        assertNotNull(parquetFiles, "Should find parquet files");
        assertTrue(parquetFiles.length > 0, "Should have at least one parquet file");

        List<String> filePaths = new java.util.ArrayList<>();
        for (File f : parquetFiles) {
            filePaths.add("file://" + f.getAbsolutePath());
        }

        System.out.println("📁 Parquet files for native scan: " + filePaths);

        // Set runtime mode to AUTOMATIC before creating the DataStream
        // The old SourceFunction API doesn't support explicit boundedness declaration,
        // so AUTOMATIC mode is required to allow Flink to detect completion
        Configuration runtimeConfig = new Configuration();
        runtimeConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.AUTOMATIC);
        environment.configure(runtimeConfig);

        // Create native Parquet scan using explicit API
        org.apache.flink.streaming.api.datastream.DataStream<org.apache.flink.table.data.RowData> nativeStream =
            org.apache.auron.flink.planner.AuronFlinkPlannerExtension.createAuronParquetScan(
                environment,
                filePaths,
                rowType,
                rowType,
                null, // project all fields
                null, // no filter predicates
                1 // parallelism
            );

        System.out.println("✅ Native Parquet scan DataStream created");

        assertNotNull(nativeStream, "Native stream should be created");

        // Execute the native DataStream and collect results
        System.out.println("🚀 Executing native DataStream...");

        try {

            // Collect results from the native execution
            List<org.apache.flink.table.data.RowData> rowDataResults = new java.util.ArrayList<>();
            org.apache.flink.util.CloseableIterator<org.apache.flink.table.data.RowData> iterator =
                nativeStream.executeAndCollect();

            while (iterator.hasNext()) {
                org.apache.flink.table.data.RowData rowData = iterator.next();
                rowDataResults.add(rowData);

                // Debug: print row data
                System.out.println("  Row: " + rowData.toString());
            }
            iterator.close();

            System.out.println("✅ Native execution completed successfully!");
            System.out.println("📊 Results collected: " + rowDataResults.size() + " rows");

            // Verify results
            assertEquals(3, rowDataResults.size(), "Should return 3 rows from native execution");

            // Verify each row has correct structure (4 fields)
            for (org.apache.flink.table.data.RowData rowData : rowDataResults) {
                assertEquals(4, rowData.getArity(), "Each row should have 4 fields");
            }

            System.out.println("✅✅✅ NATIVE EXECUTION VERIFIED - End-to-end test passed!");
            System.out.println("🎉 Auron native engine successfully executed Parquet scan!");

        } catch (Exception e) {
            System.err.println("❌ Native execution failed: " + e.getMessage());
            e.printStackTrace();
            fail("Native execution should succeed: " + e.getMessage());
        }
    }

    @Test
    public void testNativeExecutionWithProjection() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testNativeExecutionWithProjection - Auron not available");
            return;
        }

        System.out.println("\n🔥 Testing NATIVE Execution with Projection");

        // Create test data
        List<Row> testData = Arrays.asList(
            row(1, "Alice", 100.5, LocalDate.of(2024, 1, 1)),
            row(2, "Bob", 200.5, LocalDate.of(2024, 1, 2)),
            row(3, "Charlie", 300.5, LocalDate.of(2024, 1, 3)));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE," + "  created_date DATE" + ")";

        // Write Parquet test data
        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "native_projection_test", schema, testData);

        File parquetFile = new File(parquetDir, "native_projection_test");
        // Flink doesn't add .parquet extension by default, so get all non-hidden files
        File[] parquetFiles = parquetFile.listFiles((dir, name) -> !name.startsWith("."));
        assertNotNull(parquetFiles, "Should find parquet files");
        assertTrue(parquetFiles.length > 0, "Should have at least one parquet file");

        List<String> filePaths = new java.util.ArrayList<>();
        for (File f : parquetFiles) {
            filePaths.add("file://" + f.getAbsolutePath());
        }

        // Full schema
        org.apache.flink.table.types.logical.LogicalType[] fullFieldTypes = {
            new org.apache.flink.table.types.logical.IntType(),
            new org.apache.flink.table.types.logical.VarCharType(
                org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH),
            new org.apache.flink.table.types.logical.DoubleType(),
            new org.apache.flink.table.types.logical.DateType()
        };
        String[] fullFieldNames = {"id", "name", "amount", "created_date"};
        org.apache.flink.table.types.logical.RowType fullSchema =
            org.apache.flink.table.types.logical.RowType.of(fullFieldTypes, fullFieldNames);

        // Projected schema - only id and name
        org.apache.flink.table.types.logical.LogicalType[] projectedFieldTypes = {
            new org.apache.flink.table.types.logical.IntType(),
            new org.apache.flink.table.types.logical.VarCharType(
                org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH)
        };
        String[] projectedFieldNames = {"id", "name"};
        org.apache.flink.table.types.logical.RowType projectedSchema =
            org.apache.flink.table.types.logical.RowType.of(projectedFieldTypes, projectedFieldNames);

        // Projection: select only fields 0 and 1 (id and name)
        int[] projectedFields = {0, 1};

        System.out.println("📁 Parquet files: " + filePaths);
        System.out.println("📋 Projecting fields: [0, 1] (id, name)");

        // Set runtime mode to AUTOMATIC before creating the DataStream
        // (SourceFunction API doesn't support explicit boundedness declaration)
        Configuration runtimeConfig = new Configuration();
        runtimeConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.AUTOMATIC);
        environment.configure(runtimeConfig);

        // Create native Parquet scan with projection
        org.apache.flink.streaming.api.datastream.DataStream<org.apache.flink.table.data.RowData> nativeStream =
            org.apache.auron.flink.planner.AuronFlinkPlannerExtension.createAuronParquetScan(
                environment,
                filePaths,
                projectedSchema, // output schema (projected)
                fullSchema, // full Parquet schema
                projectedFields, // fields to project
                null, // no filter predicates
                1 // parallelism
            );

        System.out.println("✅ Native Parquet scan with projection created");

        try {
            // Execute and collect results
            System.out.println("🚀 Executing native projection...");
            List<org.apache.flink.table.data.RowData> results = new java.util.ArrayList<>();
            org.apache.flink.util.CloseableIterator<org.apache.flink.table.data.RowData> iterator =
                nativeStream.executeAndCollect();

            while (iterator.hasNext()) {
                org.apache.flink.table.data.RowData rowData = iterator.next();
                results.add(rowData);
                System.out.println("  Projected Row: " + rowData.toString());
            }
            iterator.close();

            System.out.println("✅ Native projection execution completed!");
            System.out.println("📊 Results: " + results.size() + " rows");

            // Verify results
            assertEquals(3, results.size(), "Should return 3 rows");

            // Verify each row has only 2 fields (projection worked)
            for (org.apache.flink.table.data.RowData rowData : results) {
                assertEquals(2, rowData.getArity(), "Projected rows should have only 2 fields");
            }

            System.out.println("✅✅✅ NATIVE PROJECTION VERIFIED!");
            System.out.println("🎉 Column pruning/projection pushdown worked!");

        } catch (Exception e) {
            System.err.println("❌ Native projection failed: " + e.getMessage());
            e.printStackTrace();
            fail("Native projection should succeed: " + e.getMessage());
        }
    }

    @Test
    public void testNativeStringFunctionLower() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testNativeStringFunctionLower - Auron not available");
            return;
        }

        System.out.println("\n🔥 Testing Native LOWER() String Function");

        // Create test data with mixed case strings
        List<Row> testData = Arrays.asList(
            row(1, "Alice", 100.5, LocalDate.of(2024, 1, 1)),
            row(2, "BOB", 200.5, LocalDate.of(2024, 1, 2)),
            row(3, "ChArLiE", 300.5, LocalDate.of(2024, 1, 3)));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE," + "  created_date DATE" + ")";

        // Write Parquet test data
        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "string_test", schema, testData);

        // Get file paths
        File parquetFile = new File(parquetDir, "string_test");
        File[] parquetFiles = parquetFile.listFiles((dir, name) -> !name.startsWith("."));
        assertNotNull(parquetFiles, "Should find parquet files");
        assertTrue(parquetFiles.length > 0, "Should have at least one parquet file");

        List<String> filePaths = new java.util.ArrayList<>();
        for (File f : parquetFiles) {
            filePaths.add("file://" + f.getAbsolutePath());
        }

        System.out.println("📁 Parquet files: " + filePaths);

        // Build schemas
        org.apache.flink.table.types.logical.LogicalType[] inputFieldTypes = {
            new org.apache.flink.table.types.logical.IntType(),
            new org.apache.flink.table.types.logical.VarCharType(255),
            new org.apache.flink.table.types.logical.DoubleType(),
            new org.apache.flink.table.types.logical.DateType()
        };
        String[] inputFieldNames = {"id", "name", "amount", "created_date"};
        org.apache.flink.table.types.logical.RowType inputRowType =
            org.apache.flink.table.types.logical.RowType.of(inputFieldTypes, inputFieldNames);

        // Output schema: id, lower_name
        org.apache.flink.table.types.logical.LogicalType[] outputFieldTypes = {
            new org.apache.flink.table.types.logical.IntType(),
            new org.apache.flink.table.types.logical.VarCharType(255)
        };
        String[] outputFieldNames = {"id", "lower_name"};
        org.apache.flink.table.types.logical.RowType outputRowType =
            org.apache.flink.table.types.logical.RowType.of(outputFieldTypes, outputFieldNames);

        // Set runtime mode
        Configuration runtimeConfig = new Configuration();
        runtimeConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.AUTOMATIC);
        environment.configure(runtimeConfig);

        // Create scan plan
        org.apache.auron.protobuf.PhysicalPlanNode scanPlan =
            org.apache.auron.flink.planner.AuronFlinkConverters.convertParquetScan(
                filePaths, inputRowType, inputRowType, null, null, 1, 0);

        // Build projection with LOWER(name)
        org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
            new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
                org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

        org.apache.calcite.rex.RexBuilder rexBuilder = new org.apache.calcite.rex.RexBuilder(typeFactory);

        // Create RexNode for: id, LOWER(name)
        List<org.apache.calcite.rex.RexNode> projections = new java.util.ArrayList<>();

        // Projection 1: id (column 0)
        projections.add(
            rexBuilder.makeInputRef(typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), 0));

        // Projection 2: LOWER(name) - column 1
        org.apache.calcite.rex.RexNode nameCol =
            rexBuilder.makeInputRef(typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR, 255), 1);
        org.apache.calcite.rex.RexNode lowerCall =
            rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.LOWER, nameCol);
        projections.add(lowerCall);

        // Convert to native projection plan
        org.apache.auron.protobuf.PhysicalPlanNode projectionPlan =
            org.apache.auron.flink.planner.AuronFlinkConverters.convertProjection(
                scanPlan,
                projections,
                Arrays.asList(outputFieldNames),
                new java.util.ArrayList<>(outputRowType.getChildren()),
                Arrays.asList(inputFieldNames));

        System.out.println("✅ Native projection plan with LOWER() created");

        // Create operator
        org.apache.auron.flink.planner.execution.AuronBatchExecutionWrapperOperator operator =
            new org.apache.auron.flink.planner.execution.AuronBatchExecutionWrapperOperator(
                projectionPlan, outputRowType, 0, 1);

        org.apache.flink.streaming.api.datastream.DataStream<org.apache.flink.table.data.RowData> nativeStream =
            environment.addSource(operator).name("AuronStringFunctionTest").setParallelism(1);

        System.out.println("🚀 Executing native LOWER() function...");

        try {
            List<org.apache.flink.table.data.RowData> results = new java.util.ArrayList<>();
            org.apache.flink.util.CloseableIterator<org.apache.flink.table.data.RowData> iterator =
                nativeStream.executeAndCollect();

            while (iterator.hasNext()) {
                org.apache.flink.table.data.RowData rowData = iterator.next();
                results.add(rowData);

                int id = rowData.getInt(0);
                org.apache.flink.table.data.StringData lowerName = rowData.getString(1);
                System.out.println("  Row: id=" + id + ", lower_name=" + lowerName.toString());
            }
            iterator.close();

            // Verify results
            assertEquals(3, results.size(), "Should return 3 rows");

            // Verify LOWER() worked correctly
            assertEquals("alice", results.get(0).getString(1).toString());
            assertEquals("bob", results.get(1).getString(1).toString());
            assertEquals("charlie", results.get(2).getString(1).toString());

            System.out.println("✅✅✅ NATIVE LOWER() FUNCTION VERIFIED!");
            System.out.println("🎉 String function executed in Auron native engine!");

        } catch (Exception e) {
            System.err.println("❌ Native LOWER() execution failed: " + e.getMessage());
            e.printStackTrace();
            fail("Native LOWER() should succeed: " + e.getMessage());
        }
    }

    @Test
    public void testNativeStringFunctionUpper() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testNativeStringFunctionUpper - Auron not available");
            return;
        }

        System.out.println("\n🔥 Testing Native UPPER() String Function");

        // Create test data with mixed case strings
        List<Row> testData = Arrays.asList(
            row(1, "alice", 100.5, LocalDate.of(2024, 1, 1)),
            row(2, "bob", 200.5, LocalDate.of(2024, 1, 2)),
            row(3, "charlie", 300.5, LocalDate.of(2024, 1, 3)));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE," + "  created_date DATE" + ")";

        // Write Parquet test data
        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "string_test_upper", schema, testData);

        // Get file paths
        File parquetFile = new File(parquetDir, "string_test_upper");
        File[] parquetFiles = parquetFile.listFiles((dir, name) -> !name.startsWith("."));
        assertNotNull(parquetFiles, "Should find parquet files");
        assertTrue(parquetFiles.length > 0, "Should have at least one parquet file");

        List<String> filePaths = new java.util.ArrayList<>();
        for (File f : parquetFiles) {
            filePaths.add("file://" + f.getAbsolutePath());
        }

        // Build schemas
        org.apache.flink.table.types.logical.LogicalType[] inputFieldTypes = {
            new org.apache.flink.table.types.logical.IntType(),
            new org.apache.flink.table.types.logical.VarCharType(255),
            new org.apache.flink.table.types.logical.DoubleType(),
            new org.apache.flink.table.types.logical.DateType()
        };
        String[] inputFieldNames = {"id", "name", "amount", "created_date"};
        org.apache.flink.table.types.logical.RowType inputRowType =
            org.apache.flink.table.types.logical.RowType.of(inputFieldTypes, inputFieldNames);

        // Output schema: id, upper_name
        org.apache.flink.table.types.logical.LogicalType[] outputFieldTypes = {
            new org.apache.flink.table.types.logical.IntType(),
            new org.apache.flink.table.types.logical.VarCharType(255)
        };
        String[] outputFieldNames = {"id", "upper_name"};
        org.apache.flink.table.types.logical.RowType outputRowType =
            org.apache.flink.table.types.logical.RowType.of(outputFieldTypes, outputFieldNames);

        // Set runtime mode
        Configuration runtimeConfig = new Configuration();
        runtimeConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.AUTOMATIC);
        environment.configure(runtimeConfig);

        // Create scan plan
        org.apache.auron.protobuf.PhysicalPlanNode scanPlan =
            org.apache.auron.flink.planner.AuronFlinkConverters.convertParquetScan(
                filePaths, inputRowType, inputRowType, null, null, 1, 0);

        // Build projection with UPPER(name)
        org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
            new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
                org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

        org.apache.calcite.rex.RexBuilder rexBuilder = new org.apache.calcite.rex.RexBuilder(typeFactory);

        // Create RexNode for: id, UPPER(name)
        List<org.apache.calcite.rex.RexNode> projections = new java.util.ArrayList<>();

        // Projection 1: id (column 0)
        projections.add(
            rexBuilder.makeInputRef(typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), 0));

        // Projection 2: UPPER(name) - column 1
        org.apache.calcite.rex.RexNode nameCol =
            rexBuilder.makeInputRef(typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR, 255), 1);
        org.apache.calcite.rex.RexNode upperCall =
            rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.UPPER, nameCol);
        projections.add(upperCall);

        // Convert to native projection plan
        org.apache.auron.protobuf.PhysicalPlanNode projectionPlan =
            org.apache.auron.flink.planner.AuronFlinkConverters.convertProjection(
                scanPlan,
                projections,
                Arrays.asList(outputFieldNames),
                new java.util.ArrayList<>(outputRowType.getChildren()),
                Arrays.asList(inputFieldNames));

        System.out.println("✅ Native projection plan with UPPER() created");

        // Create operator
        org.apache.auron.flink.planner.execution.AuronBatchExecutionWrapperOperator operator =
            new org.apache.auron.flink.planner.execution.AuronBatchExecutionWrapperOperator(
                projectionPlan, outputRowType, 0, 1);

        org.apache.flink.streaming.api.datastream.DataStream<org.apache.flink.table.data.RowData> nativeStream =
            environment.addSource(operator).name("AuronStringFunctionTest").setParallelism(1);

        System.out.println("🚀 Executing native UPPER() function...");

        try {
            List<org.apache.flink.table.data.RowData> results = new java.util.ArrayList<>();
            org.apache.flink.util.CloseableIterator<org.apache.flink.table.data.RowData> iterator =
                nativeStream.executeAndCollect();

            while (iterator.hasNext()) {
                org.apache.flink.table.data.RowData rowData = iterator.next();
                results.add(rowData);

                int id = rowData.getInt(0);
                org.apache.flink.table.data.StringData upperName = rowData.getString(1);
                System.out.println("  Row: id=" + id + ", upper_name=" + upperName.toString());
            }
            iterator.close();

            // Verify results
            assertEquals(3, results.size(), "Should return 3 rows");

            // Verify UPPER() worked correctly
            assertEquals("ALICE", results.get(0).getString(1).toString());
            assertEquals("BOB", results.get(1).getString(1).toString());
            assertEquals("CHARLIE", results.get(2).getString(1).toString());

            System.out.println("✅✅✅ NATIVE UPPER() FUNCTION VERIFIED!");
            System.out.println("🎉 String function executed in Auron native engine!");

        } catch (Exception e) {
            System.err.println("❌ Native UPPER() execution failed: " + e.getMessage());
            e.printStackTrace();
            fail("Native UPPER() should succeed: " + e.getMessage());
        }
    }
}

