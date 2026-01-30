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

import org.apache.auron.flink.planner.execution.AuronBatchExecutionWrapperOperator;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating Flink transformations that execute Auron native plans.
 *
 * <p>This factory bridges between Auron's protobuf plan representation and Flink's transformation
 * API. It creates appropriate Flink transformations that wrap Auron's native execution operators.
 *
 * <p>The factory handles:
 *
 * <ul>
 *   <li>Creating source transformations for Auron scans
 *   <li>Configuring parallelism and resource requirements
 *   <li>Setting up proper data types and schemas
 *   <li>Integrating with Flink's execution environment
 * </ul>
 */
public class AuronTransformationFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AuronTransformationFactory.class);

    /**
     * Creates a Flink transformation that executes an Auron native plan.
     *
     * <p>This method is called by {@link
     * org.apache.flink.table.planner.plan.nodes.exec.batch.AuronBatchExecNode} during plan
     * translation.
     *
     * @param auronPlan The Auron PhysicalPlanNode (protobuf) to execute
     * @param outputSchema The output schema of the transformation
     * @param planner The Flink planner base for accessing environment and configuration
     * @return A Flink transformation that executes the Auron plan
     */
    public static Transformation<RowData> createTransformation(
            Object auronPlan, RowType outputSchema, PlannerBase planner) {

        if (!(auronPlan instanceof PhysicalPlanNode)) {
            throw new IllegalArgumentException("Expected PhysicalPlanNode but got: "
                    + (auronPlan != null ? auronPlan.getClass().getName() : "null"));
        }

        PhysicalPlanNode nativePlan = (PhysicalPlanNode) auronPlan;

        LOG.info("Creating Auron transformation for plan type: {}", nativePlan.getPhysicalPlanTypeCase());

        // Check if this is an end-to-end plan including a sink
        if (nativePlan.hasParquetSink()) {
            LOG.info("Detected end-to-end native execution with ParquetSink - data will stay in Arrow format!");
            return createSinkTransformation(nativePlan, planner);
        }

        // Get the execution environment
        StreamExecutionEnvironment env = planner.getExecEnv();
        Configuration config = planner.getTableConfig().getConfiguration();

        // Create the Auron batch execution wrapper operator
        // This operator will execute the native plan and produce RowData results
        AuronBatchExecutionWrapperOperator sourceFunction = new AuronBatchExecutionWrapperOperator(
                nativePlan,
                outputSchema,
                0, // partitionId - will be set by Flink runtime
                1 // stageId - incrementing stage ID for Auron metrics
                );

        // Create a BOUNDED source transformation directly
        // Note: For MVP, we use the old SourceFunction API
        // Future versions should migrate to the new Source API (FLIP-27)
        //
        // CRITICAL CONFIGURATION FOR CLUSTER EXECUTION:
        // 1. We create LegacySourceTransformation directly (not env.addSource())
        //    - env.addSource() defaults to CONTINUOUS_UNBOUNDED which breaks batch mode
        // 2. Boundedness MUST be set to BOUNDED for batch execution
        // 3. isParallelSource MUST be true for distributed cluster execution
        //    - The operator uses taskIndex/totalParallelism for data partitioning
        //    - Setting this to false causes boundedness detection issues on clusters
        org.apache.flink.streaming.api.operators.StreamSource<RowData, ?> streamSourceOperator =
                new org.apache.flink.streaming.api.operators.StreamSource<>(sourceFunction);

        Transformation<RowData> transformation = new LegacySourceTransformation<>(
                "Auron Native Scan",
                streamSourceOperator,
                InternalTypeInfo.of(outputSchema),
                env.getParallelism(),
                Boundedness.BOUNDED, // MUST be BOUNDED for batch mode
                true); // MUST be true for parallel cluster execution

        LOG.info(
                "Created Auron LegacySourceTransformation: boundedness=BOUNDED, parallel=true, parallelism={}",
                env.getParallelism());

        // Register the transformation with the environment
        env.addOperator(transformation);

        // Set parallelism - default to the environment's parallelism
        int parallelism = env.getParallelism();
        transformation.setParallelism(parallelism);

        // Set additional metadata
        transformation.setName("Auron Native Execution");
        transformation.setDescription("Auron native vectorized execution: " + nativePlan.getPhysicalPlanTypeCase());

        LOG.info(
                "Created Auron transformation with parallelism {} for plan: {}",
                parallelism,
                nativePlan.getPhysicalPlanTypeCase());

        return transformation;
    }

    /**
     * Alternative method for creating transformations with explicit parallelism.
     *
     * @param auronPlan The Auron PhysicalPlanNode (protobuf) to execute
     * @param outputSchema The output schema of the transformation
     * @param planner The Flink planner base
     * @param parallelism Explicit parallelism setting
     * @return A Flink transformation that executes the Auron plan
     */
    public static Transformation<RowData> createTransformation(
            Object auronPlan, RowType outputSchema, PlannerBase planner, int parallelism) {

        Transformation<RowData> transformation = createTransformation(auronPlan, outputSchema, planner);
        transformation.setParallelism(parallelism);

        LOG.info("Set explicit parallelism {} for Auron transformation", parallelism);

        return transformation;
    }

    /**
     * Creates a transformation for end-to-end native execution including a sink.
     *
     * <p><b>CRITICAL for Performance:</b> This method creates a transformation that executes
     * the complete native pipeline (Source -> Transforms -> Sink) WITHOUT converting to RowData.
     * Data stays in Arrow format throughout, enabling true native performance benefits.
     *
     * <p>The operator behaves like a source (pulls data through the native pipeline) but
     * produces no output since the sink writes directly to files.
     *
     * @param nativePlan The complete native plan including ParquetSink
     * @param planner The Flink planner
     * @return A transformation that executes the complete native pipeline
     */
    private static Transformation<RowData> createSinkTransformation(PhysicalPlanNode nativePlan, PlannerBase planner) {

        LOG.info("Creating end-to-end native sink transformation (zero-copy Arrow pipeline)");

        // Get the execution environment
        StreamExecutionEnvironment env = planner.getExecEnv();

        // Extract output path and schema from the sink node
        org.apache.auron.protobuf.ParquetSinkExecNode sinkNode = nativePlan.getParquetSink();
        PhysicalPlanNode inputPlan = sinkNode.getInput();
        String fsResourceId = sinkNode.getFsResourceId();

        // Get the input schema (the schema of data being written)
        // For now, use a simple approach - extract from the input plan
        // TODO: Add proper schema extraction from protobuf plan
        RowType dummySchema = RowType.of(); // Empty schema since we don't produce output

        // Create an operator that executes the complete native plan
        // The operator will:
        // 1. Execute native plan (which reads, transforms, and writes)
        // 2. Produce no output (or just completion metadata)
        AuronBatchExecutionWrapperOperator sinkOperator = new AuronBatchExecutionWrapperOperator(
                nativePlan,
                dummySchema, // No output schema needed for sinks
                0, // partitionId - will be set by Flink runtime
                1 // stageId
                );

        // Create a BOUNDED source transformation
        // Even though this is logically a sink, it's implemented as a source that
        // pulls data through the native engine
        org.apache.flink.streaming.api.operators.StreamSource<RowData, ?> streamSourceOperator =
                new org.apache.flink.streaming.api.operators.StreamSource<>(sinkOperator);

        Transformation<RowData> transformation = new LegacySourceTransformation<>(
                "Auron Native Sink (End-to-End)",
                streamSourceOperator,
                InternalTypeInfo.of(dummySchema),
                env.getParallelism(),
                Boundedness.BOUNDED,
                true // Enable parallel execution
                );

        LOG.info("Created end-to-end native sink transformation: parallelism={}", env.getParallelism());

        // Register with environment
        env.addOperator(transformation);

        // Set metadata
        transformation.setName("Auron Native End-to-End Pipeline (Source -> Sink)");
        transformation.setDescription("Auron zero-copy native execution with ParquetSink");

        return transformation;
    }
}
