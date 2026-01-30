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

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalScalarFunctionNode;
import org.apache.auron.protobuf.ScalarFunction;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for FlinkExpressionConverter CALC (scalar function) support.
 * Tests conversion of string functions like LOWER, UPPER, TRIM, etc.
 */
public class FlinkExpressionConverterCalcTest {

    private RexBuilder rexBuilder;
    private RelDataTypeFactory typeFactory;
    private List<String> fieldNames;

    @BeforeEach
    public void setup() {
        typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
        rexBuilder = new RexBuilder(typeFactory);
        fieldNames = Arrays.asList("id", "name", "description");
    }

    @Test
    public void testLowerFunction() {
        // Create LOWER(name) expression
        RexNode nameCol = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR, 255), 1);
        RexNode lowerCall = rexBuilder.makeCall(SqlStdOperatorTable.LOWER, nameCol);

        // Convert to Auron expression
        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(lowerCall, fieldNames);

        // Verify structure
        assertTrue(exprNode.hasScalarFunction(), "Should have scalar function");
        PhysicalScalarFunctionNode scalarFunc = exprNode.getScalarFunction();

        assertEquals("LOWER", scalarFunc.getName(), "Function name should be LOWER");
        assertEquals(ScalarFunction.Lower, scalarFunc.getFun(), "Should map to Lower enum");
        assertEquals(1, scalarFunc.getArgsCount(), "Should have 1 argument");

        // Verify argument is a column reference
        PhysicalExprNode arg = scalarFunc.getArgs(0);
        assertTrue(arg.hasColumn(), "Argument should be a column");
        assertEquals("name", arg.getColumn().getName(), "Column should be 'name'");
        assertEquals(1, arg.getColumn().getIndex(), "Column index should be 1");
    }

    @Test
    public void testUpperFunction() {
        // Create UPPER(description) expression
        RexNode descCol = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR, 500), 2);
        RexNode upperCall = rexBuilder.makeCall(SqlStdOperatorTable.UPPER, descCol);

        // Convert to Auron expression
        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(upperCall, fieldNames);

        // Verify structure
        assertTrue(exprNode.hasScalarFunction(), "Should have scalar function");
        PhysicalScalarFunctionNode scalarFunc = exprNode.getScalarFunction();

        assertEquals("UPPER", scalarFunc.getName(), "Function name should be UPPER");
        assertEquals(ScalarFunction.Upper, scalarFunc.getFun(), "Should map to Upper enum");
        assertEquals(1, scalarFunc.getArgsCount(), "Should have 1 argument");

        // Verify argument
        PhysicalExprNode arg = scalarFunc.getArgs(0);
        assertTrue(arg.hasColumn(), "Argument should be a column");
        assertEquals("description", arg.getColumn().getName(), "Column should be 'description'");
        assertEquals(2, arg.getColumn().getIndex(), "Column index should be 2");
    }
}
