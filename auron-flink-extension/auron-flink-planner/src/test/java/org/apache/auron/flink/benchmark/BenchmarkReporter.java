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

/**
 * Formats and reports benchmark results.
 */
public class BenchmarkReporter {

    /**
     * Helper method to repeat a string (Java 8 compatible).
     */
    private static String repeat(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }

    /**
     * Prints a comparison report for Flink vs Auron execution.
     */
    public static void printComparison(String benchmarkName, DataScale scale, String query,
                                      BenchmarkMetrics flinkMetrics, BenchmarkMetrics auronMetrics) {
        System.out.println("\n" + repeat("=", 80));
        System.out.println(String.format("Benchmark: %s (%s - %,d rows)", 
            benchmarkName, scale.getDisplayName(), scale.getRowCount()));
        System.out.println(repeat("=", 80));
        System.out.println("Query: " + query);
        System.out.println();

        if (!flinkMetrics.isSuccess()) {
            System.out.println("❌ Flink execution FAILED: " + flinkMetrics.getErrorMessage());
        } else {
            System.out.println(String.format("Flink Native:  %,6d ms  (%,d rows)",
                flinkMetrics.getExecutionTimeMs(), flinkMetrics.getRowsProcessed()));
        }

        if (!auronMetrics.isSuccess()) {
            System.out.println("❌ Auron execution FAILED: " + auronMetrics.getErrorMessage());
        } else {
            System.out.println(String.format("Auron Native:  %,6d ms  (%,d rows)",
                auronMetrics.getExecutionTimeMs(), auronMetrics.getRowsProcessed()));
        }

        if (flinkMetrics.isSuccess() && auronMetrics.isSuccess()) {
            double speedup = auronMetrics.getSpeedupVs(flinkMetrics);
            String speedupStr = speedup > 1.0 
                ? String.format("%.2fx faster", speedup)
                : String.format("%.2fx slower", 1.0 / speedup);
            
            System.out.println(String.format("Speedup:       %s", speedupStr));
            
            // Verify row counts match
            if (flinkMetrics.getRowsProcessed() == auronMetrics.getRowsProcessed()) {
                System.out.println("\n✅ PASSED - Row counts match");
            } else {
                System.out.println("\n⚠️  WARNING - Row count mismatch!");
            }
        }

        System.out.println(repeat("=", 80));
    }

    /**
     * Prints a summary of all benchmarks.
     */
    public static void printSummary(java.util.List<BenchmarkResult> results) {
        System.out.println("\n" + repeat("=", 80));
        System.out.println("BENCHMARK SUMMARY");
        System.out.println(repeat("=", 80));
        
        double totalSpeedup = 0.0;
        int successCount = 0;
        
        for (BenchmarkResult result : results) {
            if (result.flinkMetrics.isSuccess() && result.auronMetrics.isSuccess()) {
                double speedup = result.auronMetrics.getSpeedupVs(result.flinkMetrics);
                totalSpeedup += speedup;
                successCount++;
                
                System.out.println(String.format("%-30s %-8s: %.2fx speedup",
                    result.name, result.scale, speedup));
            } else {
                System.out.println(String.format("%-30s %-8s: FAILED",
                    result.name, result.scale));
            }
        }
        
        if (successCount > 0) {
            double avgSpeedup = totalSpeedup / successCount;
            System.out.println(repeat("-", 80));
            System.out.println(String.format("Average Speedup: %.2fx (%d/%d benchmarks passed)",
                avgSpeedup, successCount, results.size()));
        }
        
        System.out.println(repeat("=", 80));
    }
    

    /**
     * Container for benchmark results.
     */
    public static class BenchmarkResult {
        public final String name;
        public final DataScale scale;
        public final BenchmarkMetrics flinkMetrics;
        public final BenchmarkMetrics auronMetrics;

        public BenchmarkResult(String name, DataScale scale,
                              BenchmarkMetrics flinkMetrics, BenchmarkMetrics auronMetrics) {
            this.name = name;
            this.scale = scale;
            this.flinkMetrics = flinkMetrics;
            this.auronMetrics = auronMetrics;
        }
    }
}
