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
package org.apache.auron.flink.examples;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.List;

/**
 * Performance benchmark comparing Flink native execution vs Auron native execution on YARN.
 * 
 * <p>This benchmark tests the following operations:
 * <ul>
 *   <li>Full Scan - Read all columns</li>
 *   <li>Projection - Select specific columns</li>
 *   <li>Filter - WHERE clause with predicate</li>
 *   <li>LOWER() - String transformation to lowercase</li>
 *   <li>UPPER() - String transformation to uppercase</li>
 *   <li>Combined - Multiple operations together</li>
 * </ul>
 * 
 * <p>Usage:
 * <pre>
 * # Run with default settings (100 rows)
 * flink run -m yarn-cluster auron-flink-examples.jar
 * 
 * # Run with custom HDFS path and row count
 * flink run -m yarn-cluster auron-flink-examples.jar hdfs:///user/myuser/benchmark 10000
 * </pre>
 * 
 * <p>Arguments:
 * <ul>
 *   <li>args[0] - HDFS base directory (default: hdfs:///user/jifan/tmp/auron_benchmark/)</li>
 *   <li>args[1] - Number of rows to generate (default: 100)</li>
 * </ul>
 */
public class YarnPerformanceBenchmark {

    private static final String SEPARATOR = "================================================================================";
    private static final String SUBSEPARATOR = "--------------------------------------------------------------------------------";

    public static void main(String[] args) throws Exception {
        try {
            System.out.println("\n" + SEPARATOR);
            System.out.println("AURON FLINK PERFORMANCE BENCHMARK ON YARN");
            System.out.println(SEPARATOR);
            System.out.println("Comparing Flink Native vs Auron Native Execution\n");

            // Parse arguments
            String hdfsBaseDir = "hdfs:///user/jifan/tmp/auron_benchmark/";
            int rowCount = 100;
            
            if (args.length > 0) {
                hdfsBaseDir = args[0];
            }
            if (args.length > 1) {
                rowCount = Integer.parseInt(args[1]);
            }

            if (!hdfsBaseDir.endsWith("/")) {
                hdfsBaseDir += "/";
            }
            String testDataPath = hdfsBaseDir + "benchmark_" + System.currentTimeMillis();

            System.out.println("Configuration:");
            System.out.println("  HDFS Path: " + testDataPath);
            System.out.println("  Row Count: " + rowCount);
            System.out.println("");

            // Step 1: Create test data
            System.out.println(SEPARATOR);
            System.out.println("STEP 1: Creating Test Data");
            System.out.println(SEPARATOR);
            long dataCreationStart = System.currentTimeMillis();
            createTestParquetData(testDataPath, rowCount);
            long dataCreationEnd = System.currentTimeMillis();
            System.out.println("✅ Test data created in " + (dataCreationEnd - dataCreationStart) + " ms");
            System.out.println("");

            // Step 2: Run benchmarks with Flink (Auron disabled)
            System.out.println(SEPARATOR);
            System.out.println("STEP 2: Running Benchmarks with FLINK NATIVE");
            System.out.println(SEPARATOR);
            List<BenchmarkResult> flinkResults = runBenchmarks(testDataPath, false);
            System.out.println("");

            // Step 3: Run benchmarks with Auron enabled
            System.out.println(SEPARATOR);
            System.out.println("STEP 3: Running Benchmarks with AURON NATIVE");
            System.out.println(SEPARATOR);
            List<BenchmarkResult> auronResults = runBenchmarks(testDataPath, true);
            System.out.println("");

            // Step 4: Print comparison summary
            System.out.println(SEPARATOR);
            System.out.println("PERFORMANCE COMPARISON SUMMARY");
            System.out.println(SEPARATOR);
            printComparisonSummary(flinkResults, auronResults);

            System.out.println("\n" + SEPARATOR);
            System.out.println("✅ Benchmark Completed Successfully!");
            System.out.println("Test data location: " + testDataPath);
            System.out.println("To clean up: hdfs dfs -rm -r " + testDataPath);
            System.out.println(SEPARATOR + "\n");

        } catch (Throwable t) {
            System.err.println("\n" + SEPARATOR);
            System.err.println("❌ BENCHMARK FAILED");
            System.err.println(SEPARATOR);
            System.err.println("Error: " + t.getMessage());
            t.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Creates test Parquet data on HDFS using Flink's datagen connector.
     */
    private static void createTestParquetData(String testDataPath, int rowCount) throws Exception {
        System.out.println("Creating " + rowCount + " rows of test data...");
        
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Create source with datagen
        System.out.println("  [1/3] Creating datagen source table...");
        tEnv.executeSql("CREATE TABLE benchmark_source ("
                + "  id BIGINT,"
                + "  name STRING,"
                + "  amount DOUBLE,"
                + "  created_date DATE"
                + ") WITH ("
                + "  'connector' = 'datagen',"
                + "  'number-of-rows' = '" + rowCount + "',"
                + "  'fields.id.kind' = 'sequence',"
                + "  'fields.id.start' = '1',"
                + "  'fields.id.end' = '" + rowCount + "',"
                + "  'fields.name.length' = '20',"
                + "  'fields.amount.min' = '10.0',"
                + "  'fields.amount.max' = '500.0'"
                + ")");

        // Create Parquet sink
        System.out.println("  [2/3] Creating Parquet sink table at: " + testDataPath);
        tEnv.executeSql("CREATE TABLE benchmark_sink ("
                + "  id BIGINT,"
                + "  name STRING,"
                + "  amount DOUBLE,"
                + "  created_date DATE"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + testDataPath + "',"
                + "  'format' = 'parquet'"
                + ")");

        // Write data
        System.out.println("  [3/3] Writing data to HDFS...");
        tEnv.executeSql("INSERT INTO benchmark_sink SELECT * FROM benchmark_source").await();
        System.out.println("  ✓ Data written successfully");
    }

    /**
     * Runs all benchmark queries with either Flink or Auron execution.
     */
    private static List<BenchmarkResult> runBenchmarks(String dataPath, boolean enableAuron) throws Exception {
        String mode = enableAuron ? "AURON" : "FLINK";
        System.out.println("Initializing " + mode + " environment...");
        
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Configure Auron
        if (enableAuron) {
            System.out.println("  ✓ Enabling Auron native execution");
            tEnv.getConfig().getConfiguration().setBoolean("table.exec.auron.enable", true);
            tEnv.getConfig().getConfiguration().setBoolean("table.exec.auron.enable.scan", true);
            tEnv.getConfig().getConfiguration().setBoolean("table.exec.auron.enable.project", true);
            tEnv.getConfig().getConfiguration().setBoolean("table.exec.auron.enable.filter", true);
        } else {
            System.out.println("  ✓ Using Flink native execution (Auron disabled)");
            tEnv.getConfig().getConfiguration().setBoolean("table.exec.auron.enable", false);
        }

        // Create table
        System.out.println("  ✓ Creating table pointing to: " + dataPath);
        tEnv.executeSql("CREATE TABLE benchmark_table ("
                + "  id BIGINT,"
                + "  name STRING,"
                + "  amount DOUBLE,"
                + "  created_date DATE"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + dataPath + "',"
                + "  'format' = 'parquet'"
                + ")");

        System.out.println("");

        List<BenchmarkResult> results = new ArrayList<>();

        // Benchmark 1: Full Scan
        results.add(runBenchmark(tEnv, "Full Scan", 
            "SELECT * FROM benchmark_table", mode));

        // Benchmark 2: Projection
        results.add(runBenchmark(tEnv, "Projection", 
            "SELECT id, name FROM benchmark_table", mode));

        // Benchmark 3: Filter
        results.add(runBenchmark(tEnv, "Filter", 
            "SELECT id, name, amount FROM benchmark_table WHERE amount > 100.0", mode));

        // Benchmark 4: Projection + Filter
        results.add(runBenchmark(tEnv, "Projection + Filter", 
            "SELECT id, name FROM benchmark_table WHERE amount > 100.0", mode));

        // Benchmark 5: LOWER()
        results.add(runBenchmark(tEnv, "CALC - LOWER()", 
            "SELECT id, LOWER(name) as lower_name FROM benchmark_table", mode));

        // Benchmark 6: UPPER()
        results.add(runBenchmark(tEnv, "CALC - UPPER()", 
            "SELECT id, UPPER(name) as upper_name FROM benchmark_table", mode));

        // Benchmark 7: Multiple CALC
        results.add(runBenchmark(tEnv, "Multiple CALC", 
            "SELECT id, LOWER(name) as lower_name, UPPER(name) as upper_name FROM benchmark_table", mode));

        // Benchmark 8: CALC + Filter
        results.add(runBenchmark(tEnv, "CALC + Filter", 
            "SELECT id, UPPER(name) as upper_name FROM benchmark_table WHERE amount > 100.0", mode));

        return results;
    }

    /**
     * Runs a single benchmark query and measures execution time.
     */
    private static BenchmarkResult runBenchmark(TableEnvironment tEnv, String name, String sql, String mode) {
        System.out.println(SUBSEPARATOR);
        System.out.println("Benchmark: " + name + " (" + mode + ")");
        System.out.println(SUBSEPARATOR);
        System.out.println("SQL: " + sql);
        
        try {
            // Execute query and measure time
            System.out.println("Executing query...");
            long startTime = System.currentTimeMillis();
            
            TableResult result = tEnv.executeSql(sql);
            
            // Collect all results to ensure full execution
            int rowCount = 0;
            try (CloseableIterator<Row> iterator = result.collect()) {
                while (iterator.hasNext()) {
                    iterator.next();
                    rowCount++;
                }
            }
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            System.out.println("✅ Completed: " + rowCount + " rows in " + duration + " ms");
            System.out.println("");
            
            return new BenchmarkResult(name, sql, mode, duration, rowCount, true, null);
            
        } catch (Exception e) {
            System.err.println("❌ Failed: " + e.getMessage());
            e.printStackTrace();
            System.out.println("");
            return new BenchmarkResult(name, sql, mode, -1, 0, false, e.getMessage());
        }
    }

    /**
     * Prints comparison summary between Flink and Auron results.
     */
    private static void printComparisonSummary(List<BenchmarkResult> flinkResults, List<BenchmarkResult> auronResults) {
        System.out.printf("%-30s | %12s | %12s | %10s%n", "Benchmark", "Flink (ms)", "Auron (ms)", "Speedup");
        System.out.println(SEPARATOR);

        double totalSpeedup = 0;
        int successCount = 0;

        for (int i = 0; i < flinkResults.size(); i++) {
            BenchmarkResult flink = flinkResults.get(i);
            BenchmarkResult auron = auronResults.get(i);

            if (flink.success && auron.success) {
                double speedup = (double) flink.durationMs / auron.durationMs;
                totalSpeedup += speedup;
                successCount++;

                String speedupStr = String.format("%.2fx", speedup);
                if (speedup > 1.0) {
                    speedupStr += " ⚡"; // Auron faster
                } else if (speedup < 1.0) {
                    speedupStr += " 🐌"; // Flink faster
                }

                System.out.printf("%-30s | %,12d | %,12d | %10s%n", 
                    flink.name, flink.durationMs, auron.durationMs, speedupStr);
            } else {
                System.out.printf("%-30s | %12s | %12s | %10s%n", 
                    flink.name, 
                    flink.success ? flink.durationMs + " ms" : "FAILED",
                    auron.success ? auron.durationMs + " ms" : "FAILED",
                    "N/A");
            }
        }

        System.out.println(SEPARATOR);
        if (successCount > 0) {
            double avgSpeedup = totalSpeedup / successCount;
            System.out.printf("Average Speedup: %.2fx (%d/%d benchmarks)%n", avgSpeedup, successCount, flinkResults.size());
        }
        System.out.println("");

        // Print detailed results
        System.out.println("Detailed Results:");
        System.out.println("");
        
        System.out.println("FLINK Results:");
        for (BenchmarkResult r : flinkResults) {
            System.out.printf("  %-30s: %s%n", r.name, 
                r.success ? r.durationMs + " ms (" + r.rowCount + " rows)" : "FAILED - " + r.errorMessage);
        }
        System.out.println("");
        
        System.out.println("AURON Results:");
        for (BenchmarkResult r : auronResults) {
            System.out.printf("  %-30s: %s%n", r.name, 
                r.success ? r.durationMs + " ms (" + r.rowCount + " rows)" : "FAILED - " + r.errorMessage);
        }
    }

    /**
     * Holds the result of a single benchmark execution.
     */
    private static class BenchmarkResult {
        final String name;
        final String sql;
        final String mode;
        final long durationMs;
        final int rowCount;
        final boolean success;
        final String errorMessage;

        BenchmarkResult(String name, String sql, String mode, long durationMs, int rowCount, boolean success, String errorMessage) {
            this.name = name;
            this.sql = sql;
            this.mode = mode;
            this.durationMs = durationMs;
            this.rowCount = rowCount;
            this.success = success;
            this.errorMessage = errorMessage;
        }
    }
}
