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

import java.util.Arrays;
import java.util.List;
import org.apache.auron.flink.planner.AuronFlinkPlannerExtension;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;

public class AuronYarnE2E {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: AuronYarnE2E <parquet-path> [parallelism]");
            System.exit(1);
        }

        String parquetPath = args[0];
        int parallelism = args.length > 1 ? Integer.parseInt(args[1]) : 2;

        System.out.println("=== Auron Flink YARN E2E Test ===");
        System.out.println("Parquet: " + parquetPath);
        System.out.println("Parallelism: " + parallelism);

        // Setup environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        Configuration config = new Configuration();
        // BATCH doesn't work due to: Caused by: java.lang.IllegalStateException: Detected an UNBOUNDED source with the 'execution.runtime-mode' set to 'BATCH'. This combination is not allowed, please set the 'execution.runtime-mode' to STREAMING or AUTOMATIC
//        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(config);

        // Enable Auron
        // This doesn't really work because not implemented in Flink yet.
        config.setBoolean("table.exec.auron.enable", true);
        config.setString("table.exec.auron.log-level", "INFO");

        // Define schema (adjust to your Parquet file)
        LogicalType[] types = {new IntType(), new VarCharType(VarCharType.MAX_LENGTH), new DoubleType(), new DateType()
        };
        String[] names = {"id", "name", "amount", "created_date"};
        RowType schema = RowType.of(types, names);

        // Create Auron native scan
        List<String> files = Arrays.asList(parquetPath);
        DataStream<RowData> stream =
                AuronFlinkPlannerExtension.createAuronParquetScan(env, files, schema, schema, null, null, parallelism);

        stream.print();
        env.execute("Auron YARN E2E Test");

        System.out.println("=== Test Completed ===");
    }
}
