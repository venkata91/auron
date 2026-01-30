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

/**
 * Performance benchmark for Auron end-to-end native execution with ParquetSink.
 *
 * <p>This benchmark measures the performance of:
 * <ul>
 *   <li>Reading from Parquet (ParquetScan)</li>
 *   <li>Transforming data (Filter + Calc)</li>
 *   <li>Writing to Parquet (ParquetSink)</li>
 * </ul>
 *
 * <p>With Auron enabled, the ENTIRE pipeline executes in native engine with zero conversions!
 *
 * <p>Usage:
 * <pre>
 * # Run with default settings (10M rows)
 * yarn jar auron-flink-assembly.jar org.apache.auron.flink.examples.YarnPerformanceBenchmark
 *
 * # Run with custom data size
 * yarn jar auron-flink-assembly.jar org.apache.auron.flink.examples.YarnPerformanceBenchmark 1000000
 *
 * # Run with custom paths
 * yarn jar auron-flink-assembly.jar org.apache.auron.flink.examples.YarnPerformanceBenchmark 1000000 hdfs:///user/me/benchmark
 * </pre>
 */
public class YarnPerformanceBenchmark {

    private static final String SEPARATOR = repeatString("=", 100);
    private static final int DEFAULT_NUM_ROWS = 10_000_000;

    public static void main(String[] args) throws Exception {
        try {
            System.out.println("\n" + SEPARATOR);
            System.out.println("🚀 AURON FLINK PERFORMANCE BENCHMARK - ParquetSink End-to-End");
            System.out.println(SEPARATOR);

            // Parse arguments
            int numRows = DEFAULT_NUM_ROWS;
            String hdfsBaseDir = "hdfs:///user/" + System.getProperty("user.name") + "/auron_benchmark/";

            if (args.length > 0) {
                numRows = Integer.parseInt(args[0]);
            }
            if (args.length > 1) {
                hdfsBaseDir = args[1];
            }

            if (!hdfsBaseDir.endsWith("/")) {
                hdfsBaseDir += "/";
            }

            long timestamp = System.currentTimeMillis();
            String sourceDataPath = hdfsBaseDir + "source_" + timestamp;
            String outputDataPath = hdfsBaseDir + "output_" + timestamp;

            System.out.println("\nBenchmark Configuration:");
            System.out.println("  Rows to process: " + String.format("%,d", numRows));
            System.out.println("  Source path: " + sourceDataPath);
            System.out.println("  Output path: " + outputDataPath);
            System.out.println("");

            // Step 1: Create test data
            System.out.println(SEPARATOR);
            System.out.println("STEP 1: Generating Test Data");
            System.out.println(SEPARATOR);
            long genStart = System.currentTimeMillis();
            createTestParquetData(sourceDataPath, numRows);
            long genEnd = System.currentTimeMillis();
            System.out.println("✅ Test data generated in " + formatDuration(genEnd - genStart));
            System.out.println("");

            // Step 2: Run with Auron ENABLED
            System.out.println(SEPARATOR);
            System.out.println("STEP 2: Benchmark WITH Auron (End-to-End Native)");
            System.out.println(SEPARATOR);
            long auronStart = System.currentTimeMillis();
            runBenchmark(sourceDataPath, outputDataPath + "_auron", true);
            long auronEnd = System.currentTimeMillis();
            long auronDuration = auronEnd - auronStart;
            System.out.println("✅ Auron execution completed in " + formatDuration(auronDuration));
            System.out.println("");

            // Step 3: Run with Auron DISABLED (Flink native)
            System.out.println(SEPARATOR);
            System.out.println("STEP 3: Benchmark WITHOUT Auron (Flink Native)");
            System.out.println(SEPARATOR);
            long flinkStart = System.currentTimeMillis();
            runBenchmark(sourceDataPath, outputDataPath + "_flink", false);
            long flinkEnd = System.currentTimeMillis();
            long flinkDuration = flinkEnd - flinkStart;
            System.out.println("✅ Flink native execution completed in " + formatDuration(flinkDuration));
            System.out.println("");

            // Step 4: Results
            System.out.println(SEPARATOR);
            System.out.println("📊 PERFORMANCE RESULTS");
            System.out.println(SEPARATOR);
            System.out.println(String.format("  Rows Processed:      %,d", numRows));
            System.out.println(String.format("  Auron Time:          %s", formatDuration(auronDuration)));
            System.out.println(String.format("  Flink Native Time:   %s", formatDuration(flinkDuration)));

            double speedup = (double) flinkDuration / auronDuration;
            System.out.println(String.format("  Speedup:             %.2fx", speedup));

            if (speedup > 1.0) {
                System.out.println("  Result:              🎉 Auron is FASTER!");
            } else {
                System.out.println("  Result:              ⚠️  Flink native is faster (unexpected)");
            }

            double auronThroughput = numRows / (auronDuration / 1000.0);
            double flinkThroughput = numRows / (flinkDuration / 1000.0);
            System.out.println(String.format("  Auron Throughput:    %,.0f rows/sec", auronThroughput));
            System.out.println(String.format("  Flink Throughput:    %,.0f rows/sec", flinkThroughput));

            System.out.println("\n" + SEPARATOR);
            System.out.println("✅ Benchmark Completed Successfully!");
            System.out.println(SEPARATOR);
            System.out.println("\nCleanup Commands:");
            System.out.println("  hdfs dfs -rm -r " + sourceDataPath);
            System.out.println("  hdfs dfs -rm -r " + outputDataPath + "_auron");
            System.out.println("  hdfs dfs -rm -r " + outputDataPath + "_flink");
            System.out.println("");

        } catch (Throwable t) {
            System.err.println("❌ Benchmark Failed: " + t.getMessage());
            t.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Creates test Parquet data for benchmarking.
     */
    private static void createTestParquetData(String dataPath, int numRows) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        System.out.println("Generating " + String.format("%,d", numRows) + " rows...");

        // Create datagen source
        tEnv.executeSql("CREATE TABLE datagen_source ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  amount DOUBLE,"
                + "  category STRING,"
                + "  created_date DATE"
                + ") WITH ("
                + "  'connector' = 'datagen',"
                + "  'number-of-rows' = '" + numRows + "',"
                + "  'fields.id.kind' = 'sequence',"
                + "  'fields.id.start' = '1',"
                + "  'fields.id.end' = '" + numRows + "',"
                + "  'fields.product.length' = '20',"
                + "  'fields.amount.min' = '10.0',"
                + "  'fields.amount.max' = '5000.0',"
                + "  'fields.category.length' = '15'"
                + ")");

        // Create Parquet sink
        tEnv.executeSql("CREATE TABLE parquet_sink ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  amount DOUBLE,"
                + "  category STRING,"
                + "  created_date DATE"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + dataPath + "',"
                + "  'format' = 'parquet'"
                + ")");

        // Write data
        tEnv.executeSql("INSERT INTO parquet_sink SELECT * FROM datagen_source").await();
    }

    /**
     * Runs the benchmark query: Read -> Transform -> Write.
     *
     * @param sourcePath Input Parquet files
     * @param outputPath Output Parquet files
     * @param enableAuron Whether to enable Auron native execution
     */
    private static void runBenchmark(String sourcePath, String outputPath, boolean enableAuron) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Configure Auron
        tEnv.getConfig().getConfiguration().setBoolean("table.optimizer.auron.enabled", enableAuron);
        tEnv.getConfig().getConfiguration().setInteger("table.exec.resource.default-parallelism", 4);

        System.out.println("Configuration:");
        System.out.println("  table.optimizer.auron.enabled = " + enableAuron);
        System.out.println("  parallelism = 4");

        // Create source table
        tEnv.executeSql("CREATE TABLE source_table ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  amount DOUBLE,"
                + "  category STRING,"
                + "  created_date DATE"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + sourcePath + "',"
                + "  'format' = 'parquet'"
                + ")");

        // Create sink table
        tEnv.executeSql("CREATE TABLE sink_table ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  total_amount DOUBLE,"
                + "  category STRING"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + outputPath + "',"
                + "  'format' = 'parquet'"
                + ")");

        // Execute INSERT query with transformations
        // This will test: ParquetScan -> Filter -> Calc -> ParquetSink
        String query = "INSERT INTO sink_table " +
                       "SELECT id, product, amount * 1.1 as total_amount, category " +
                       "FROM source_table " +
                       "WHERE amount > 100.0";

        System.out.println("\nExecuting query:");
        System.out.println("  " + query);
        System.out.println("");

        if (enableAuron) {
            System.out.println("⚡ Expected: End-to-end native execution (zero conversions)");
            System.out.println("   Look for log: 'Detected end-to-end native execution with ParquetSink'");
        } else {
            System.out.println("🐢 Flink native execution (with RowData conversions)");
        }

        System.out.println("\nExecuting...");
        TableResult result = tEnv.executeSql(query);
        result.await();
        System.out.println("Done!");
    }

    private static String formatDuration(long millis) {
        if (millis < 1000) {
            return millis + "ms";
        } else if (millis < 60000) {
            return String.format("%.2fs", millis / 1000.0);
        } else {
            long minutes = millis / 60000;
            long seconds = (millis % 60000) / 1000;
            return String.format("%dm %ds", minutes, seconds);
        }
    }

    private static String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}
