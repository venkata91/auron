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
package org.apache.auron.flink.benchmark;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
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

/**
 * Base class for Auron Flink benchmarks.
 * Provides utilities to run queries with/without Auron native execution.
 */
public class AuronFlinkBenchmarkBase extends AuronFlinkTableTestBase {

    protected StreamTableEnvironment flinkEnv;
    protected StreamTableEnvironment auronEnv;

    @BeforeEach
    @Override
    public void before() {
        // Don't call super.before() - we'll set up our own environments
        setupFlinkEnvironment();
        setupAuronEnvironment();
    }

    /**
     * Sets up Flink environment WITHOUT Auron native execution.
     */
    protected void setupFlinkEnvironment() {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        // Explicitly disable Auron
        config.setBoolean("table.exec.auron.enable", false);

        flinkEnv = StreamTableEnvironment.create(streamEnv, EnvironmentSettings.fromConfiguration(config));
        
        // Set parent class field for compatibility with writeParquetTestData
        this.tableEnvironment = flinkEnv;
    }

    /**
     * Sets up Flink environment WITH Auron native execution.
     */
    protected void setupAuronEnvironment() {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        // Enable Auron native execution
        config.setBoolean("table.exec.auron.enable", true);
        config.setBoolean("table.exec.auron.enable.scan", true);
        config.setBoolean("table.exec.auron.enable.project", true);
        config.setBoolean("table.exec.auron.enable.filter", true);
        config.setInteger("table.exec.auron.batch-size", 8192);
        config.setDouble("table.exec.auron.memory-fraction", 0.7);
        config.setString("table.exec.auron.log-level", "WARN");

        auronEnv = StreamTableEnvironment.create(streamEnv, EnvironmentSettings.fromConfiguration(config));
    }

    /**
     * Prepares test data and creates Parquet table in both environments.
     */
    protected void prepareTestData(String tableName, String schema, List<Row> data, DataScale scale)
            throws Exception {
        File benchmarkDir = DataGenerator.createBenchmarkDataDir();
        String dataDirName = DataGenerator.getDataDirName(tableName, scale);
        File dataDir = new File(benchmarkDir, dataDirName);

        // Only generate data if it doesn't exist
        if (!dataDir.exists()) {
            System.out.println("📊 Generating " + scale.getDisplayName() + " dataset for " + tableName + "...");
            writeParquetTestData(benchmarkDir, dataDirName, schema, data);
        } else {
            System.out.println("♻️  Reusing cached dataset: " + dataDirName);
        }

        // Create table in both environments
        String tablePath = "file://" + dataDir.getAbsolutePath();
        createParquetTableInEnv(flinkEnv, tableName, schema, tablePath);
        createParquetTableInEnv(auronEnv, tableName, schema, tablePath);
    }

    /**
     * Creates a Parquet table in the specified environment.
     */
    private void createParquetTableInEnv(StreamTableEnvironment env, String tableName,
                                        String schema, String path) {
        String createTableSql = String.format(
            "CREATE TABLE %s %s WITH (" +
            "  'connector' = 'filesystem'," +
            "  'path' = '%s'," +
            "  'format' = 'parquet'" +
            ")",
            tableName, schema, path
        );
        System.out.println("Creating table: " + tableName + " in environment: " + env.getClass().getSimpleName());
        env.executeSql(createTableSql);
        System.out.println("✓ Table created successfully");
    }

    /**
     * Runs a query with Flink native execution and returns metrics.
     */
    protected BenchmarkMetrics runWithFlink(String sql) {
        try {
            long startTime = System.currentTimeMillis();
            TableResult result = flinkEnv.executeSql(sql);
            List<Row> rows = collectResults(result);
            long endTime = System.currentTimeMillis();

            return new BenchmarkMetrics("Flink", endTime - startTime, rows.size());
        } catch (Exception e) {
            return BenchmarkMetrics.failed("Flink", e.getMessage());
        }
    }

    /**
     * Runs a query with Auron native execution and returns metrics.
     */
    protected BenchmarkMetrics runWithAuron(String sql) {
        try {
            long startTime = System.currentTimeMillis();
            TableResult result = auronEnv.executeSql(sql);
            List<Row> rows = collectResults(result);
            long endTime = System.currentTimeMillis();

            return new BenchmarkMetrics("Auron", endTime - startTime, rows.size());
        } catch (Exception e) {
            return BenchmarkMetrics.failed("Auron", e.getMessage());
        }
    }

    /**
     * Runs a benchmark: executes with both Flink and Auron, then reports comparison.
     * Returns the metrics for collection.
     */
    protected BenchmarkMetrics[] runBenchmark(String benchmarkName, DataScale scale, String sql) {
        System.out.println("\n Running: " + benchmarkName + " (" + scale + ")");

        // Run with Flink
        BenchmarkMetrics flinkMetrics = runWithFlink(sql);

        // Run with Auron
        BenchmarkMetrics auronMetrics = runWithAuron(sql);

        // Report results
        BenchmarkReporter.printComparison(benchmarkName, scale, sql, flinkMetrics, auronMetrics);
        
        // Return metrics for summary collection
        return new BenchmarkMetrics[] { flinkMetrics, auronMetrics };
    }

    /**
     * Collects all results from a TableResult into a List.
     */
    @Override
    protected List<Row> collectResults(TableResult result) {
        List<Row> rows = new ArrayList<>();
        try (org.apache.flink.util.CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext()) {
                rows.add(iterator.next());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to collect results", e);
        }
        return rows;
    }

    /**
     * Checks if Auron native library is available.
     */
    protected boolean isAuronAvailable() {
        try {
            System.loadLibrary("auron");
            return true;
        } catch (UnsatisfiedLinkError e) {
            return false;
        }
    }
}
