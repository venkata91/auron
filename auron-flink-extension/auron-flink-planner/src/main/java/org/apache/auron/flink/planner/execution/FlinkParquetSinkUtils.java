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
package org.apache.auron.flink.planner.execution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JNI bridge utilities for ParquetSink operations.
 * These static methods are called from Auron's native engine.
 *
 * <p>IMPORTANT: Method signatures must match exactly what's defined in the native JNI bridge:
 * - getTaskOutputPath() -> String
 * - completeOutput(String path, long numRows, long numBytes) -> void
 */
public class FlinkParquetSinkUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkParquetSinkUtils.class);

    /**
     * Called by native engine to get the next output file path.
     * Uses thread-local context to generate Flink-compatible file names.
     *
     * @return Full path to output Parquet file
     * @throws IllegalStateException if context not set
     */
    public static String getTaskOutputPath() {
        ParquetSinkContext context = ParquetSinkContext.get();
        String path = context.generateOutputPath();
        LOG.debug(
                "Generated output path: {} for task {}, attempt {}",
                path,
                context.getTaskIndex(),
                context.getAttemptNumber());
        return path;
    }

    /**
     * Called by native engine when a Parquet file write completes.
     * Records the completed file metadata for tracking and metrics.
     *
     * @param path Output file path
     * @param numRows Number of rows written
     * @param numBytes Number of bytes written
     */
    public static void completeOutput(String path, long numRows, long numBytes) {
        ParquetSinkContext context = ParquetSinkContext.get();
        context.addCompletedFile(path, numRows, numBytes);
        LOG.info("Completed Parquet write: path={}, rows={}, bytes={}", path, numRows, numBytes);
    }
}
