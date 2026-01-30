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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;

/**
 * Utilities for converting Flink RowData to Arrow format.
 */
public class FlinkArrowConverters {

    /**
     * Converts Flink RowType to Arrow Schema.
     */
    public static Schema toArrowSchema(RowType rowType) {
        List<Field> fields = new ArrayList<>();
        List<String> fieldNames = rowType.getFieldNames();
        List<LogicalType> fieldTypes = rowType.getChildren();

        for (int i = 0; i < fieldNames.size(); i++) {
            String name = fieldNames.get(i);
            LogicalType logicalType = fieldTypes.get(i);
            ArrowType arrowType = toArrowType(logicalType);
            boolean nullable = logicalType.isNullable();
            fields.add(new Field(name, new FieldType(nullable, arrowType, null), null));
        }

        return new Schema(fields);
    }

    /**
     * Converts Flink LogicalType to Arrow ArrowType.
     */
    private static ArrowType toArrowType(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return ArrowType.Bool.INSTANCE;
            case TINYINT:
                return new ArrowType.Int(8, true);
            case SMALLINT:
                return new ArrowType.Int(16, true);
            case INTEGER:
                return new ArrowType.Int(32, true);
            case BIGINT:
                return new ArrowType.Int(64, true);
            case FLOAT:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case VARCHAR:
            case CHAR:
                return ArrowType.Utf8.INSTANCE;
            case BINARY:
            case VARBINARY:
                return ArrowType.Binary.INSTANCE;
            case DATE:
                return new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) logicalType;
                return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                return new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale(), 128);
            default:
                throw new UnsupportedOperationException("Unsupported Flink type: " + logicalType.getTypeRoot());
        }
    }

    /**
     * Converts a list of RowData to Arrow VectorSchemaRoot.
     *
     * @param rows List of rows to convert
     * @param root Pre-allocated VectorSchemaRoot (will be cleared and reused)
     * @param rowType Flink row type schema
     */
    public static void convertToArrow(List<RowData> rows, VectorSchemaRoot root, RowType rowType) {

        root.setRowCount(rows.size());
        List<LogicalType> fieldTypes = rowType.getChildren();
        List<FieldVector> vectors = root.getFieldVectors();

        for (int colIdx = 0; colIdx < vectors.size(); colIdx++) {
            FieldVector vector = vectors.get(colIdx);
            LogicalType logicalType = fieldTypes.get(colIdx);

            for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
                RowData row = rows.get(rowIdx);
                if (row.isNullAt(colIdx)) {
                    vector.setNull(rowIdx);
                } else {
                    setVectorValue(vector, rowIdx, row, colIdx, logicalType);
                }
            }
            vector.setValueCount(rows.size());
        }
    }

    /**
     * Sets a value in an Arrow vector from a RowData field.
     */
    private static void setVectorValue(
            FieldVector vector, int rowIdx, RowData row, int colIdx, LogicalType logicalType) {

        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                ((BitVector) vector).setSafe(rowIdx, row.getBoolean(colIdx) ? 1 : 0);
                break;
            case TINYINT:
                ((TinyIntVector) vector).setSafe(rowIdx, row.getByte(colIdx));
                break;
            case SMALLINT:
                ((SmallIntVector) vector).setSafe(rowIdx, row.getShort(colIdx));
                break;
            case INTEGER:
                ((IntVector) vector).setSafe(rowIdx, row.getInt(colIdx));
                break;
            case BIGINT:
                ((BigIntVector) vector).setSafe(rowIdx, row.getLong(colIdx));
                break;
            case FLOAT:
                ((Float4Vector) vector).setSafe(rowIdx, row.getFloat(colIdx));
                break;
            case DOUBLE:
                ((Float8Vector) vector).setSafe(rowIdx, row.getDouble(colIdx));
                break;
            case VARCHAR:
            case CHAR:
                StringData stringData = row.getString(colIdx);
                byte[] bytes = stringData.toBytes();
                ((VarCharVector) vector).setSafe(rowIdx, bytes);
                break;
            case BINARY:
            case VARBINARY:
                byte[] binaryData = row.getBinary(colIdx);
                ((VarBinaryVector) vector).setSafe(rowIdx, binaryData);
                break;
            case DATE:
                ((DateDayVector) vector).setSafe(rowIdx, row.getInt(colIdx));
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) logicalType;
                long timestamp =
                        row.getTimestamp(colIdx, timestampType.getPrecision()).getMillisecond();
                ((TimeStampMicroVector) vector).setSafe(rowIdx, timestamp * 1000);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                java.math.BigDecimal decimal = row.getDecimal(
                                colIdx, decimalType.getPrecision(), decimalType.getScale())
                        .toBigDecimal();
                ((DecimalVector) vector).setSafe(rowIdx, decimal);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type for conversion: " + logicalType.getTypeRoot());
        }
    }

    /**
     * Creates a VectorSchemaRoot for the given schema and allocator.
     */
    public static VectorSchemaRoot createRoot(Schema schema, BufferAllocator allocator) {
        return VectorSchemaRoot.create(schema, allocator);
    }
}
