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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-local context for ParquetSink operations.
 * Stores metadata about the current sink task for JNI callbacks.
 */
public class ParquetSinkContext {

    /** Thread-local storage for context */
    private static final ThreadLocal<ParquetSinkContext> CONTEXT = new ThreadLocal<>();

    /** Base output directory path */
    private final String basePath;

    /** Flink task index (subtask index) */
    private final int taskIndex;

    /** Flink task attempt number */
    private final int attemptNumber;

    /** Counter for multiple files written by same task */
    private final AtomicInteger fileCounter = new AtomicInteger(0);

    /** List of completed file writes */
    private final List<CompletedFile> completedFiles = new ArrayList<>();

    /**
     * Represents a completed Parquet file write.
     */
    public static class CompletedFile {
        private final String path;
        private final long numRows;
        private final long numBytes;

        public CompletedFile(String path, long numRows, long numBytes) {
            this.path = path;
            this.numRows = numRows;
            this.numBytes = numBytes;
        }

        public String getPath() {
            return path;
        }

        public long getNumRows() {
            return numRows;
        }

        public long getNumBytes() {
            return numBytes;
        }

        @Override
        public String toString() {
            return String.format("CompletedFile{path='%s', rows=%d, bytes=%d}", path, numRows, numBytes);
        }
    }

    private ParquetSinkContext(String basePath, int taskIndex, int attemptNumber) {
        this.basePath = basePath;
        this.taskIndex = taskIndex;
        this.attemptNumber = attemptNumber;
    }

    /**
     * Sets the context for the current thread.
     */
    public static void set(String basePath, int taskIndex, int attemptNumber) {
        CONTEXT.set(new ParquetSinkContext(basePath, taskIndex, attemptNumber));
    }

    /**
     * Gets the context for the current thread.
     * @throws IllegalStateException if context not set
     */
    public static ParquetSinkContext get() {
        ParquetSinkContext context = CONTEXT.get();
        if (context == null) {
            throw new IllegalStateException("ParquetSinkContext not set for current thread");
        }
        return context;
    }

    /**
     * Clears the context for the current thread.
     */
    public static void clear() {
        CONTEXT.remove();
    }

    /**
     * Generates the next output file path following Flink naming convention:
     * part-{taskId:05d}-{attemptId}-{counter}.parquet
     */
    public String generateOutputPath() {
        int counter = fileCounter.getAndIncrement();
        String fileName = String.format("part-%05d-%d-%d.parquet", taskIndex, attemptNumber, counter);
        return basePath + "/" + fileName;
    }

    /**
     * Records a completed file write.
     */
    public void addCompletedFile(String path, long numRows, long numBytes) {
        completedFiles.add(new CompletedFile(path, numRows, numBytes));
    }

    /**
     * Gets all completed files for this task.
     */
    public List<CompletedFile> getCompletedFiles() {
        return new ArrayList<>(completedFiles);
    }

    public String getBasePath() {
        return basePath;
    }

    public int getTaskIndex() {
        return taskIndex;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    /**
     * Gets total number of rows written across all files.
     */
    public long getTotalRows() {
        return completedFiles.stream().mapToLong(CompletedFile::getNumRows).sum();
    }

    /**
     * Gets total number of bytes written across all files.
     */
    public long getTotalBytes() {
        return completedFiles.stream().mapToLong(CompletedFile::getNumBytes).sum();
    }
}
