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

import java.util.ArrayList;
import java.util.List;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts Flink batch ExecNodes to Auron PhysicalPlanNode protobuf representations.
 *
 * <p>This converter extracts information from Flink's execution plan nodes and translates them
 * into Auron's native plan format. It handles:
 *
 * <ul>
 *   <li>Table source scans (Parquet files)
 *   <li>Filter predicates
 *   <li>Projection expressions
 *   <li>Combined Calc operations (filter + projection)
 * </ul>
 *
 * <p>The conversion process:
 *
 * <ol>
 *   <li>Identifies the pattern (scan, scan+calc, etc.)
 *   <li>Extracts table metadata (file paths, schema)
 *   <li>Extracts filter predicates and projection expressions
 *   <li>Converts to Auron protobuf using {@link AuronFlinkConverters}
 * </ol>
 */
public class AuronExecNodeConverter {

    private static final Logger LOG = LoggerFactory.getLogger(AuronExecNodeConverter.class);

    /**
     * Converts a Flink ExecNode (and its inputs) to an Auron PhysicalPlanNode.
     *
     * @param node The Flink ExecNode to convert
     * @param inputs The input nodes (already converted/processed)
     * @return The Auron PhysicalPlanNode protobuf
     * @throws UnsupportedOperationException if the node pattern is not supported
     */
    public static PhysicalPlanNode convert(ExecNode<?> node, List<ExecNode<?>> inputs) {
        LOG.info("Converting Flink ExecNode to Auron plan: {}", node.getDescription());

        // Pattern 1: End-to-end native execution (Source -> [Transforms] -> Sink)
        // BatchExecSink with native-compatible input chain
        if (node instanceof BatchExecSink && inputs.size() == 1) {
            return convertSinkWithInput((BatchExecSink) node, inputs.get(0));
        }

        // Pattern 2: BatchExecCalc on top of TableSourceScan
        if (node instanceof BatchExecCalc && inputs.size() == 1) {
            ExecNode<?> input = inputs.get(0);
            if (input instanceof CommonExecTableSourceScan) {
                return convertCalcWithScan((BatchExecCalc) node, (CommonExecTableSourceScan) input);
            }
        }

        // Pattern 3: Just TableSourceScan (no calc)
        if (node instanceof CommonExecTableSourceScan) {
            return convertScanOnly((CommonExecTableSourceScan) node);
        }

        throw new UnsupportedOperationException("Unsupported ExecNode pattern for Auron conversion: "
                + node.getClass().getSimpleName()
                + " with "
                + inputs.size()
                + " inputs");
    }

    /**
     * Converts a Calc node with a table source scan underneath.
     *
     * <p>This handles the common pattern where Flink's optimizer combines filter and projection
     * operations into a single Calc node on top of a table scan.
     *
     * @param calc The Calc node containing filter/projection
     * @param scan The table source scan node
     * @return The Auron PhysicalPlanNode
     */
    private static PhysicalPlanNode convertCalcWithScan(BatchExecCalc calc, CommonExecTableSourceScan scan) {
        LOG.debug("Converting Calc + Scan pattern to Auron");

        // Extract scan information
        DynamicTableSourceSpec sourceSpec = scan.getTableSourceSpec();
        ContextResolvedTable resolvedTable = sourceSpec.getContextResolvedTable();

        // Extract file paths from the resolved table
        List<String> filePaths = extractFilePaths(resolvedTable);
        LOG.debug("Extracted {} file paths from table source", filePaths.size());

        // Get the full schema from the scan
        RowType scanOutputSchema = (RowType) scan.getOutputType();
        LOG.debug("Scan output schema: {}", scanOutputSchema);

        // Extract calc information - access protected fields via reflection
        List<RexNode> projections = null;
        RexNode condition = null;
        try {
            java.lang.reflect.Field projectionField =
                    calc.getClass().getSuperclass().getDeclaredField("projection");
            projectionField.setAccessible(true);
            projections = (List<RexNode>) projectionField.get(calc);

            java.lang.reflect.Field conditionField =
                    calc.getClass().getSuperclass().getDeclaredField("condition");
            conditionField.setAccessible(true);
            condition = (RexNode) conditionField.get(calc);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract projection/condition from calc node", e);
        }

        LOG.debug(
                "Calc has {} projections and {} filter",
                projections != null ? projections.size() : 0,
                condition != null ? "a" : "no");

        // Get calc output schema
        RowType calcOutputSchema = (RowType) calc.getOutputType();

        // Split filter conditions if present
        List<RexNode> filterConditions = null;
        if (condition != null) {
            filterConditions = AuronFlinkConverters.splitAndConditions(condition);
            LOG.debug("Split filter into {} conditions", filterConditions.size());
        }

        // Build base Parquet scan plan
        PhysicalPlanNode scanPlan = AuronFlinkConverters.convertParquetScan(
                filePaths,
                scanOutputSchema, // Schema after scan (full schema)
                scanOutputSchema, // Full schema
                null, // No projection at scan level
                filterConditions, // Push filters down to scan
                1, // numPartitions - will be set by runtime
                0 // partitionIndex - will be set by runtime
                );

        // Apply projection if present
        if (projections != null && !projections.isEmpty()) {
            List<String> outputFieldNames = calcOutputSchema.getFieldNames();
            List<LogicalType> outputTypes = new ArrayList<>(calcOutputSchema.getChildren());
            List<String> inputFieldNames = scanOutputSchema.getFieldNames();

            return AuronFlinkConverters.convertProjection(
                    scanPlan, projections, outputFieldNames, outputTypes, inputFieldNames);
        }

        return scanPlan;
    }

    /**
     * Applies a Calc transformation (projections and filters) on top of an input plan.
     * This is used when the Calc is not directly on the source.
     *
     * @param calc The Calc node containing filter/projection
     * @param inputPlan The input physical plan
     * @return The Auron PhysicalPlanNode with Calc applied
     */
    private static PhysicalPlanNode convertCalcOnPlan(BatchExecCalc calc, PhysicalPlanNode inputPlan) {
        LOG.debug("Applying Calc transformation on existing plan");

        // Extract calc information - access protected fields via reflection
        List<RexNode> projections = null;
        RexNode condition = null;
        try {
            java.lang.reflect.Field projectionField =
                    calc.getClass().getSuperclass().getDeclaredField("projection");
            projectionField.setAccessible(true);
            projections = (List<RexNode>) projectionField.get(calc);

            java.lang.reflect.Field conditionField =
                    calc.getClass().getSuperclass().getDeclaredField("condition");
            conditionField.setAccessible(true);
            condition = (RexNode) conditionField.get(calc);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract projection/condition from calc node", e);
        }

        LOG.debug(
                "Calc has {} projections and {} filter",
                projections != null ? projections.size() : 0,
                condition != null ? "a" : "no");

        // Get input and output schemas
        // For now, we get the input schema from the Calc's input edges
        RowType inputSchema = null;
        List<org.apache.flink.table.planner.plan.nodes.exec.ExecEdge> edges = calc.getInputEdges();
        if (edges.size() == 1) {
            inputSchema = (RowType) edges.get(0).getSource().getOutputType();
        }
        RowType calcOutputSchema = (RowType) calc.getOutputType();

        PhysicalPlanNode resultPlan = inputPlan;

        // Apply filter if present
        if (condition != null) {
            List<RexNode> filterConditions = AuronFlinkConverters.splitAndConditions(condition);
            LOG.debug("Applying {} filter conditions", filterConditions.size());
            resultPlan = AuronFlinkConverters.convertFilter(resultPlan, filterConditions, inputSchema.getFieldNames());
        }

        // Apply projection if present
        if (projections != null && !projections.isEmpty()) {
            List<String> outputFieldNames = calcOutputSchema.getFieldNames();
            List<LogicalType> outputTypes = new ArrayList<>(calcOutputSchema.getChildren());
            List<String> inputFieldNames = inputSchema.getFieldNames();

            LOG.debug("Applying projection with {} expressions", projections.size());
            resultPlan = AuronFlinkConverters.convertProjection(
                    resultPlan, projections, outputFieldNames, outputTypes, inputFieldNames);
        }

        return resultPlan;
    }

    /**
     * Converts a table source scan without any calc operations.
     *
     * @param scan The table source scan node
     * @return The Auron PhysicalPlanNode
     */
    private static PhysicalPlanNode convertScanOnly(CommonExecTableSourceScan scan) {
        LOG.debug("Converting Scan-only pattern to Auron");

        // Extract scan information
        DynamicTableSourceSpec sourceSpec = scan.getTableSourceSpec();
        ContextResolvedTable resolvedTable = sourceSpec.getContextResolvedTable();

        // Extract file paths
        List<String> filePaths = extractFilePaths(resolvedTable);
        LOG.debug("Extracted {} file paths from table source", filePaths.size());

        // Get schema
        RowType outputSchema = (RowType) scan.getOutputType();
        LOG.debug("Scan output schema: {}", outputSchema);

        // Build Parquet scan plan (no filters, no projection)
        return AuronFlinkConverters.convertParquetScan(
                filePaths,
                outputSchema, // Output schema
                outputSchema, // Full schema
                null, // No projection
                null, // No filters
                1, // numPartitions - will be set by runtime
                0 // partitionIndex - will be set by runtime
                );
    }

    /**
     * Converts an end-to-end native execution chain: Source -> [Transforms] -> Sink.
     *
     * <p>This is the key method for achieving zero-conversion performance. It builds
     * a complete native plan where data stays in Arrow format from source to sink.
     *
     * @param sink The sink node
     * @param input The input node (already converted if it was native-compatible)
     * @return The Auron PhysicalPlanNode with complete source->sink pipeline
     */
    private static PhysicalPlanNode convertSinkWithInput(BatchExecSink sink, ExecNode<?> input) {
        LOG.info("Converting end-to-end native pipeline: Source -> Transforms -> Sink");

        // Step 1: Build the input plan (source + optional transforms)
        PhysicalPlanNode inputPlan = convertInputChain(input);

        // Step 2: Extract sink information
        DynamicTableSinkSpec sinkSpec = sink.getTableSinkSpec();
        String outputPath = extractSinkOutputPath(sinkSpec);
        LOG.info("Sink output path: {}", outputPath);

        // Step 3: Build ParquetSink wrapping the input plan
        RowType inputSchema = (RowType) input.getOutputType();
        return AuronFlinkConverters.convertParquetSink(
                inputPlan,
                outputPath,
                inputSchema,
                java.util.Collections.emptyList() // Parquet properties - TODO: extract from sink config
                );
    }

    /**
     * Converts an input chain (source + optional transforms) to a native plan.
     * Recursively handles: Source, Source+Calc, Source+Filter+Calc, etc.
     */
    private static PhysicalPlanNode convertInputChain(ExecNode<?> node) {
        // If it's already wrapped in an AuronBatchExecNode, unwrap it to get the original node
        if (node.getClass().getSimpleName().equals("AuronBatchExecNode")) {
            try {
                // Use reflection to get the original node (not the inputs)
                java.lang.reflect.Method getOriginalNodeMethod = node.getClass().getMethod("getOriginalNode");
                ExecNode<?> originalNode = (ExecNode<?>) getOriginalNodeMethod.invoke(node);

                if (originalNode != null) {
                    LOG.debug(
                            "Unwrapping AuronBatchExecNode to get original node: {}",
                            originalNode.getClass().getSimpleName());
                    return convertInputChain(originalNode);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unwrap AuronBatchExecNode: {}", e.getMessage());
            }
        }

        // If it's a Calc, recursively convert its input and apply Calc on top
        if (node instanceof BatchExecCalc) {
            List<org.apache.flink.table.planner.plan.nodes.exec.ExecEdge> edges = node.getInputEdges();
            if (edges.size() == 1) {
                ExecNode<?> input = edges.get(0).getSource();
                // Recursively convert the input (which could be a source, filter, or another calc)
                PhysicalPlanNode inputPlan = convertInputChain(input);
                // Apply the Calc transformation on top of the input
                return convertCalcOnPlan((BatchExecCalc) node, inputPlan);
            }
        }

        // If it's just a source
        if (node instanceof CommonExecTableSourceScan) {
            return convertScanOnly((CommonExecTableSourceScan) node);
        }

        throw new UnsupportedOperationException(
                "Unsupported input chain for sink: " + node.getClass().getSimpleName());
    }

    /**
     * Extracts the output path from a table sink spec.
     */
    private static String extractSinkOutputPath(DynamicTableSinkSpec sinkSpec) {
        try {
            // Get the table options which contain the 'path' property
            ContextResolvedTable resolvedTable = sinkSpec.getContextResolvedTable();
            java.util.Map<String, String> options =
                    resolvedTable.getResolvedTable().getOptions();

            String path = options.get("path");
            if (path == null) {
                throw new RuntimeException("Sink table does not have 'path' option");
            }

            LOG.debug("Extracted sink path: {}", path);
            return path;

        } catch (Exception e) {
            throw new RuntimeException("Failed to extract output path from sink spec", e);
        }
    }

    /**
     * Extracts file paths from a table source.
     *
     * <p>This method extracts file paths from the table metadata.
     * If the path is a directory, it scans for actual Parquet files.
     * Uses Flink FileSystem API to support both Local and HDFS paths.
     *
     * @param resolvedTable The resolved table metadata
     * @return List of file paths
     */
    private static List<String> extractFilePaths(ContextResolvedTable resolvedTable) {
        List<String> filePaths = new ArrayList<>();

        try {
            // Get the path from table options
            if (resolvedTable.getResolvedTable().getOptions().containsKey("path")) {
                String basePathString =
                        resolvedTable.getResolvedTable().getOptions().get("path");
                LOG.info("Extracted path from table options: {}", basePathString);

                Path path = new Path(basePathString);
                FileSystem fs = path.getFileSystem();

                if (fs.exists(path)) {
                    FileStatus status = fs.getFileStatus(path);
                    if (status.isDir()) {
                        LOG.debug("Path is a directory, scanning for Parquet files via FileSystem API...");
                        FileStatus[] listStatus = fs.listStatus(path);
                        if (listStatus != null) {
                            for (FileStatus fileStatus : listStatus) {
                                if (!fileStatus.isDir()) {
                                    String fileName = fileStatus.getPath().getName();
                                    // Match parquet files or Flink part files
                                    if (fileName.endsWith(".parquet") || fileName.contains("part-")) {
                                        filePaths.add(fileStatus.getPath().toString());
                                        LOG.debug(
                                                "Found file: {}",
                                                fileStatus.getPath().toString());
                                    }
                                }
                            }
                        }
                    } else {
                        // Single file path
                        filePaths.add(basePathString);
                    }
                } else {
                    // Path doesn't exist yet (common during initial INSERT), pass as-is
                    LOG.warn("Path does not exist, passing original path: {}", basePathString);
                    filePaths.add(basePathString);
                }
            }

            if (filePaths.isEmpty()) {
                LOG.info(
                        "Resolved table options: {}",
                        resolvedTable.getResolvedTable().getOptions());
                throw new IllegalStateException(
                        "Could not extract file paths from table source. " + "Check if the path exists: "
                                + resolvedTable.getResolvedTable().getOptions().get("path"));
            }

        } catch (Exception e) {
            LOG.error("Failed to extract file paths from table source", e);
            throw new RuntimeException("Failed to extract file paths for Auron execution", e);
        }

        return filePaths;
    }
}
