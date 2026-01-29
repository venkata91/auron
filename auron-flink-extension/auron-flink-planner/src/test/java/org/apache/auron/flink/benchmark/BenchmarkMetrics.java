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
 * Metrics collected during a benchmark run.
 */
public class BenchmarkMetrics {
    private final String executionMode; // "Flink" or "Auron"
    private final long executionTimeMs;
    private final long rowsProcessed;
    private final boolean success;
    private final String errorMessage;

    public BenchmarkMetrics(String executionMode, long executionTimeMs, long rowsProcessed) {
        this(executionMode, executionTimeMs, rowsProcessed, true, null);
    }

    public BenchmarkMetrics(String executionMode, long executionTimeMs, long rowsProcessed,
                           boolean success, String errorMessage) {
        this.executionMode = executionMode;
        this.executionTimeMs = executionTimeMs;
        this.rowsProcessed = rowsProcessed;
        this.success = success;
        this.errorMessage = errorMessage;
    }

    public static BenchmarkMetrics failed(String executionMode, String errorMessage) {
        return new BenchmarkMetrics(executionMode, 0, 0, false, errorMessage);
    }

    public String getExecutionMode() {
        return executionMode;
    }

    public long getExecutionTimeMs() {
        return executionTimeMs;
    }

    public long getRowsProcessed() {
        return rowsProcessed;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public double getSpeedupVs(BenchmarkMetrics other) {
        if (!this.success || !other.success || this.executionTimeMs == 0) {
            return 0.0;
        }
        return (double) other.executionTimeMs / this.executionTimeMs;
    }

    @Override
    public String toString() {
        if (!success) {
            return String.format("%s: FAILED - %s", executionMode, errorMessage);
        }
        return String.format("%s: %d ms (%,d rows)", executionMode, executionTimeMs, rowsProcessed);
    }
}
