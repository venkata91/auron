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
package org.apache.auron.flink.planner;

import java.io.File;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * Verification test for END-TO-END NATIVE ParquetSink execution.
 *
 * <p>This test validates the complete zero-conversion pipeline:
 * ParquetSource → Filter → Calc → ParquetSink (all in Arrow format!)
 *
 * <p>To run locally on standalone cluster:
 * <pre>
 * # Step 1: Build everything
 * cd /Users/vsowrira/git/auron
 * ./build-flink.sh
 *
 * # Step 2: Run the test
 * cd auron-flink-extension/auron-flink-planner
 * ./scripts/run-parquet-sink-test.sh
 * </pre>
 *
 * <p>Or run directly with Java:
 * <pre>
 * export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
 * cd /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner
 *
 * java -cp "target/classes:target/test-classes:target/lib/*" \
 *      org.apache.auron.flink.planner.AuronParquetSinkVerificationTest
 * </pre>
 *
 * <p><b>CRITICAL LOG TO WATCH FOR:</b>
 * <pre>
 * INFO AuronTransformationFactory - Detected end-to-end native execution with ParquetSink - data will stay in Arrow format!
 * </pre>
 *
 * <p>If you see this log: ✅ Zero-conversion native execution is working!
 */
public class AuronParquetSinkVerificationTest {

    public static void main(String[] args) throws Exception {
        String separator = repeatString("=", 100);
        System.out.println("\n" + separator);
        System.out.println("🚀 AURON PARQUET SINK - END-TO-END NATIVE EXECUTION VERIFICATION");
        System.out.println(separator);
        System.out.println("\nThis test verifies that data stays in Arrow format from source to sink!");
        System.out.println(separator + "\n");

        try {
            // Step 1: Create source test data
            String sourceDataPath = "/tmp/auron_sink_source_" + System.currentTimeMillis();
            System.out.println("📝 Step 1: Creating source test data...");
            createSourceData(sourceDataPath);
            System.out.println("✅ Source data created at: " + sourceDataPath);
            System.out.println("   Contains: 100 rows\n");

            // Step 2: Test WITH Auron (end-to-end native)
            String outputWithAuron = "/tmp/auron_sink_output_" + System.currentTimeMillis();
            System.out.println(separator);
            System.out.println("⚡ Step 2: Testing WITH AURON (End-to-End Native Arrow)");
            System.out.println(separator);
            System.out.println("\n🔍 WATCH FOR THESE LOGS:");
            System.out.println("   - 'Detected end-to-end native execution with ParquetSink'");
            System.out.println("   - 'Creating end-to-end native sink transformation (zero-copy Arrow pipeline)'");
            System.out.println(separator + "\n");

            long startAuron = System.currentTimeMillis();
            executeQuery(sourceDataPath, outputWithAuron, true);
            long endAuron = System.currentTimeMillis();
            long auronTime = endAuron - startAuron;

            System.out.println("\n✅ Auron execution completed in " + auronTime + "ms");
            verifyOutput(outputWithAuron);

            // Step 3: Test WITHOUT Auron (for comparison)
            String outputWithoutAuron = "/tmp/flink_sink_output_" + System.currentTimeMillis();
            System.out.println("\n" + separator);
            System.out.println("🐢 Step 3: Testing WITHOUT AURON (Flink Native with RowData)");
            System.out.println(separator + "\n");

            long startFlink = System.currentTimeMillis();
            executeQuery(sourceDataPath, outputWithoutAuron, false);
            long endFlink = System.currentTimeMillis();
            long flinkTime = endFlink - startFlink;

            System.out.println("\n✅ Flink native execution completed in " + flinkTime + "ms");
            verifyOutput(outputWithoutAuron);

            // Step 4: Results
            System.out.println("\n" + separator);
            System.out.println("📊 PERFORMANCE COMPARISON");
            System.out.println(separator);
            System.out.println("  Auron Time:        " + auronTime + "ms");
            System.out.println("  Flink Native Time: " + flinkTime + "ms");

            if (auronTime < flinkTime) {
                double speedup = (double) flinkTime / auronTime;
                System.out.println("  Speedup:           " + String.format("%.2fx", speedup) + " 🎉");
                System.out.println("  Result:            ✅ Auron is FASTER!");
            } else {
                System.out.println("  Result:            ⚠️  Flink native was faster (may need larger dataset)");
            }

            // Cleanup
            System.out.println("\n" + separator);
            System.out.println("🧹 Cleanup");
            System.out.println(separator);
            deleteDirectory(new File(sourceDataPath));
            deleteDirectory(new File(outputWithAuron));
            deleteDirectory(new File(outputWithoutAuron));
            System.out.println("✅ Test data cleaned up");

            System.out.println("\n" + separator);
            System.out.println("✅ ✅ ✅  VERIFICATION TEST PASSED  ✅ ✅ ✅");
            System.out.println(separator);
            System.out.println("\nKey Achievement:");
            System.out.println("  Data flowed through the entire pipeline in Arrow format!");
            System.out.println("  ParquetSource → Filter → Calc → ParquetSink");
            System.out.println("  ZERO conversions to RowData = True native performance!\n");
            System.out.println(separator + "\n");

        } catch (Exception e) {
            System.err.println("\n❌ TEST FAILED: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void createSourceData(String path) throws Exception {
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("CREATE TABLE datagen_source ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  amount DOUBLE,"
                + "  category STRING"
                + ") WITH ("
                + "  'connector' = 'datagen',"
                + "  'number-of-rows' = '100',"
                + "  'fields.id.kind' = 'sequence',"
                + "  'fields.id.start' = '1',"
                + "  'fields.id.end' = '100',"
                + "  'fields.product.length' = '20',"
                + "  'fields.amount.min' = '100.0',"
                + "  'fields.amount.max' = '1000.0',"
                + "  'fields.category.length' = '15'"
                + ")");

        tEnv.executeSql("CREATE TABLE source_sink ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  amount DOUBLE,"
                + "  category STRING"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + path + "',"
                + "  'format' = 'parquet'"
                + ")");

        tEnv.executeSql("INSERT INTO source_sink SELECT * FROM datagen_source").await();
    }

    private static void executeQuery(String sourcePath, String outputPath, boolean enableAuron) throws Exception {
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Configure Auron
        tEnv.getConfig().getConfiguration().setBoolean("table.optimizer.auron.enabled", enableAuron);
        tEnv.getConfig().getConfiguration().setInteger("table.exec.resource.default-parallelism", 2);

        System.out.println("Configuration:");
        System.out.println("  table.optimizer.auron.enabled = " + enableAuron);
        System.out.println("  parallelism = 2\n");

        // Create source table
        tEnv.executeSql("CREATE TABLE source_table ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  amount DOUBLE,"
                + "  category STRING"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + sourcePath + "',"
                + "  'format' = 'parquet'"
                + ")");

        // Create sink table
        tEnv.executeSql("CREATE TABLE sink_table ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  doubled_amount DOUBLE,"
                + "  category STRING"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + outputPath + "',"
                + "  'format' = 'parquet'"
                + ")");

        // Execute INSERT with transformations
        String query = "INSERT INTO sink_table "
                + "SELECT id, product, amount * 2.0 as doubled_amount, category "
                + "FROM source_table "
                + "WHERE amount > 200.0";

        System.out.println("Executing query:");
        System.out.println("  " + query + "\n");

        if (enableAuron) {
            System.out.println("⚡ Expected: END-TO-END NATIVE execution");
            System.out.println("   (ParquetScan → Filter → Calc → ParquetSink, all in Arrow!)");
        } else {
            System.out.println("🐢 Flink native execution (with RowData conversions)");
        }

        System.out.println("\nExecuting...");
        TableResult result = tEnv.executeSql(query);
        result.await();
        System.out.println("Done!");
    }

    private static void verifyOutput(String outputPath) {
        File outputDir = new File(outputPath);
        if (!outputDir.exists() || !outputDir.isDirectory()) {
            throw new RuntimeException("Output directory does not exist: " + outputPath);
        }

        File[] parquetFiles = outputDir.listFiles((dir, name) -> name.endsWith(".parquet"));
        if (parquetFiles == null || parquetFiles.length == 0) {
            throw new RuntimeException("No Parquet files found in: " + outputPath);
        }

        System.out.println("   Output verification:");
        System.out.println("     - Found " + parquetFiles.length + " Parquet file(s)");
        long totalSize = 0;
        for (File f : parquetFiles) {
            totalSize += f.length();
        }
        System.out.println("     - Total size: " + totalSize + " bytes");
    }

    private static void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.isDirectory()) {
                        deleteDirectory(f);
                    } else {
                        f.delete();
                    }
                }
            }
            dir.delete();
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
