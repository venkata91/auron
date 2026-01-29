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

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Benchmark suite comparing Flink native vs Auron native execution.
 * 
 * Run with: mvn test -Dtest=AuronFlinkBenchmarkSuite -Dgroups=benchmark
 * 
 * Or specific scale: -Dbenchmark.scale=SMALL
 */
@Tag("benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AuronFlinkBenchmarkSuite extends AuronFlinkBenchmarkBase {

    private static final String TABLE_NAME = "benchmark_table";
    private DataScale scale;
    private List<BenchmarkReporter.BenchmarkResult> allResults;

    /**
     * Helper method to repeat a string (Java 8 compatible).
     */
    private static String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }

    @Override
    public void before() {
        // Override parent's @BeforeEach to prevent environment reset
        // We manage our own environments in @BeforeAll
    }

    @BeforeAll
    public void setupBenchmark() throws Exception {
        // Check if Auron is available
        if (!isAuronAvailable()) {
            System.out.println("⚠️  Auron native library not available - benchmarks will be skipped");
            return;
        }

        // Determine scale from system property (default: SMALL)
        String scaleProperty = System.getProperty("benchmark.scale", "SMALL");
        scale = DataScale.valueOf(scaleProperty.toUpperCase());
        
        String separator = repeatString("=", 80);
        System.out.println("\n" + separator);
        System.out.println("🚀 AURON FLINK BENCHMARK SUITE");
        System.out.println("   Scale: " + scale.getDisplayName() + " (" + scale.getRowCount() + " rows)");
        System.out.println(separator);

        allResults = new ArrayList<>();

        // Initialize both Flink and Auron environments (needed for data preparation)
        setupFlinkEnvironment();
        setupAuronEnvironment();

        // Generate test data
        List<Row> data = DataGenerator.generateBasicData(scale);
        String schema = DataGenerator.getBasicSchema();
        
        prepareTestData(TABLE_NAME, schema, data, scale);
    }

    @Test
    public void benchmark01_FullScan() {
        if (!isAuronAvailable()) return;

        String sql = "SELECT * FROM " + TABLE_NAME;
        runBenchmark("Full Scan", scale, sql);
    }

    @Test
    public void benchmark02_ProjectColumns() {
        if (!isAuronAvailable()) return;

        String sql = "SELECT id, name, amount FROM " + TABLE_NAME;
        runBenchmark("Project Columns", scale, sql);
    }

    @Test
    public void benchmark03_SimpleFilter() {
        if (!isAuronAvailable()) return;

        // Filter ~66% of data
        int threshold = scale.getRowCount() / 3;
        String sql = "SELECT * FROM " + TABLE_NAME + " WHERE amount > " + threshold;
        runBenchmark("Simple Filter", scale, sql);
    }

    @Test
    public void benchmark04_ProjectWithFilter() {
        if (!isAuronAvailable()) return;

        int threshold = scale.getRowCount() / 3;
        String sql = "SELECT id, name, amount FROM " + TABLE_NAME + " WHERE amount > " + threshold;
        runBenchmark("Project + Filter", scale, sql);
    }

    @Test
    public void benchmark05_ComplexFilter() {
        if (!isAuronAvailable()) return;

        int threshold = scale.getRowCount() / 3;
        String sql = "SELECT * FROM " + TABLE_NAME + 
                    " WHERE amount > " + threshold + " AND id < " + (scale.getRowCount() / 2);
        runBenchmark("Complex Filter", scale, sql);
    }

    @Test
    public void benchmark06_CalcLower() {
        if (!isAuronAvailable()) return;

        String sql = "SELECT id, LOWER(name) as lower_name FROM " + TABLE_NAME;
        runBenchmark("CALC - LOWER()", scale, sql);
    }

    @Test
    public void benchmark07_CalcUpper() {
        if (!isAuronAvailable()) return;

        String sql = "SELECT id, UPPER(name) as upper_name FROM " + TABLE_NAME;
        runBenchmark("CALC - UPPER()", scale, sql);
    }

    @Test
    public void benchmark08_CalcWithFilter() {
        if (!isAuronAvailable()) return;

        int threshold = scale.getRowCount() / 3;
        String sql = "SELECT id, UPPER(name) as upper_name, amount FROM " + TABLE_NAME + 
                    " WHERE amount > " + threshold;
        runBenchmark("CALC + Filter", scale, sql);
    }

    @Test
    public void benchmark09_MultipleCalc() {
        if (!isAuronAvailable()) return;

        String sql = "SELECT id, LOWER(name) as lower_name, UPPER(name) as upper_name FROM " + TABLE_NAME;
        runBenchmark("Multiple CALC Functions", scale, sql);
    }

    @Test
    public void benchmark10_Count() {
        if (!isAuronAvailable()) return;

        String sql = "SELECT COUNT(*) FROM " + TABLE_NAME;
        runBenchmark("COUNT(*)", scale, sql);
    }

    @Test
    public void benchmark11_CountWithFilter() {
        if (!isAuronAvailable()) return;

        int threshold = scale.getRowCount() / 3;
        String sql = "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE amount > " + threshold;
        runBenchmark("COUNT(*) with Filter", scale, sql);
    }

    @Test
    public void benchmark12_SumAggregation() {
        if (!isAuronAvailable()) return;

        String sql = "SELECT SUM(amount) FROM " + TABLE_NAME;
        runBenchmark("SUM Aggregation", scale, sql);
    }

    // Summary is printed after all tests
    @org.junit.jupiter.api.AfterAll
    public void printSummary() {
        if (!isAuronAvailable() || allResults.isEmpty()) {
            return;
        }
        
        BenchmarkReporter.printSummary(allResults);
    }

    @Override
    protected BenchmarkMetrics[] runBenchmark(String benchmarkName, DataScale scale, String sql) {
        // Run benchmark and get metrics
        BenchmarkMetrics[] metrics = super.runBenchmark(benchmarkName, scale, sql);
        
        // Collect results for summary (metrics[0] = flink, metrics[1] = auron)
        allResults.add(new BenchmarkReporter.BenchmarkResult(
            benchmarkName, scale, metrics[0], metrics[1]));
        
        return metrics;
    }
}
