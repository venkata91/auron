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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.types.Row;

/**
 * Generates test data for benchmarking at different scales.
 */
public class DataGenerator {

    /**
     * Generates basic schema data: (id INT, name STRING, amount DOUBLE, date DATE)
     */
    public static List<Row> generateBasicData(DataScale scale) {
        int rowCount = scale.getRowCount();
        List<Row> data = new ArrayList<>(rowCount);
        
        for (int i = 1; i <= rowCount; i++) {
            Row row = Row.of(
                i,                                          // id
                "Name_" + i,                                // name
                i * 1.5,                                    // amount
                LocalDate.of(2024, 1, (i % 28) + 1)        // date (cycles through month)
            );
            data.add(row);
        }
        
        return data;
    }

    /**
     * Generates data with nulls: ~10% null names, ~5% null amounts
     */
    public static List<Row> generateDataWithNulls(DataScale scale) {
        int rowCount = scale.getRowCount();
        List<Row> data = new ArrayList<>(rowCount);
        
        for (int i = 1; i <= rowCount; i++) {
            String name = (i % 10 == 0) ? null : "Name_" + i;
            Double amount = (i % 20 == 0) ? null : i * 1.5;
            
            Row row = Row.of(
                i,
                name,
                amount,
                LocalDate.of(2024, 1, (i % 28) + 1)
            );
            data.add(row);
        }
        
        return data;
    }

    /**
     * Generates string-heavy data for UDF testing
     */
    public static List<Row> generateStringHeavyData(DataScale scale) {
        int rowCount = scale.getRowCount();
        List<Row> data = new ArrayList<>(rowCount);
        
        String[] prefixes = {"UPPER", "lower", "MiXeD", "Title"};
        
        for (int i = 1; i <= rowCount; i++) {
            String prefix = prefixes[i % prefixes.length];
            Row row = Row.of(
                i,
                prefix + "_Name_" + i,
                "Description_" + i,
                "Category_" + (i % 100)
            );
            data.add(row);
        }
        
        return data;
    }

    /**
     * Generates data with skewed distribution (for aggregation testing)
     */
    public static List<Row> generateSkewedData(DataScale scale) {
        int rowCount = scale.getRowCount();
        List<Row> data = new ArrayList<>(rowCount);
        
        // 80% of data has category "A", 15% "B", 5% "C"
        for (int i = 1; i <= rowCount; i++) {
            String category;
            if (i % 100 < 80) {
                category = "A";
            } else if (i % 100 < 95) {
                category = "B";
            } else {
                category = "C";
            }
            
            Row row = Row.of(
                i,
                "Name_" + i,
                i * 1.5,
                category
            );
            data.add(row);
        }
        
        return data;
    }

    /**
     * Gets the basic schema definition
     */
    public static String getBasicSchema() {
        return "(" +
            "  id INT," +
            "  name STRING," +
            "  amount DOUBLE," +
            "  created_date DATE" +
            ")";
    }

    /**
     * Gets the string-heavy schema definition
     */
    public static String getStringHeavySchema() {
        return "(" +
            "  id INT," +
            "  name STRING," +
            "  description STRING," +
            "  category STRING" +
            ")";
    }

    /**
     * Gets the skewed data schema definition
     */
    public static String getSkewedSchema() {
        return "(" +
            "  id INT," +
            "  name STRING," +
            "  amount DOUBLE," +
            "  category STRING" +
            ")";
    }

    /**
     * Creates a cache directory for benchmark data
     */
    public static File createBenchmarkDataDir() {
        File dir = new File("target/benchmark-data");
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return dir;
    }

    /**
     * Gets a unique directory name for a specific scale and schema
     */
    public static String getDataDirName(String schemaType, DataScale scale) {
        return schemaType + "_" + scale.name().toLowerCase();
    }
}
