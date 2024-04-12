/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polycat.common.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import io.polycat.catalog.common.model.record.ByteWritable;
import io.polycat.catalog.common.model.record.DoubleWritable;
import io.polycat.catalog.common.model.record.FloatWritable;
import io.polycat.catalog.common.model.record.IntWritable;
import io.polycat.catalog.common.model.record.LongWritable;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.model.record.ShortWritable;
import io.polycat.catalog.common.model.record.StringWritable;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.common.VectorBatch;
import io.polycat.common.RowBatch;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowUtil {

    /**
     * 将Enigma Schema转换为Arrow Schema
     */
    public static Schema convertToArrowSchema(io.polycat.catalog.common.model.Schema schema) {
        List<Field> arrowFields = schema.getSchemaFields()
                .stream()
                .map(f -> Field.nullable(f.getName(), ArrowUtil.convertToArrowType(f.getDataType())))
                .collect(Collectors.toList());
        return new Schema(arrowFields);
    }

    /**
     * 将Enigma数据类型转换为Arrow数据类型
     */
    public static ArrowType convertToArrowType(DataType dataType) {
        int sqlType = dataType.getSqlType();
        if (sqlType == DataTypes.STRING_SQL_TYPE) {
            return new ArrowType.Utf8();
        } else if (sqlType == DataTypes.BOOLEAN_SQL_TYPE) {
            return new ArrowType.Bool();
        } else if (sqlType == DataTypes.TINYINT_SQL_TYPE) {
            return new ArrowType.Int(8, true);
        } else if (sqlType == DataTypes.SMALLINT_SQL_TYPE) {
            return new ArrowType.Int(16, true);
        } else if (sqlType == DataTypes.INTEGER_SQL_TYPE) {
            return new ArrowType.Int(32, true);
        } else if (sqlType == DataTypes.BIGINT_SQL_TYPE) {
            return new ArrowType.Int(64, true);
        } else if (sqlType == DataTypes.FLOAT_SQL_TYPE) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        } else if (sqlType == DataTypes.DOUBLE_SQL_TYPE) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (sqlType == DataTypes.TIMESTAMP_SQL_TYPE) {
            return new ArrowType.Timestamp(TimeUnit.NANOSECOND,
                TimeZone.getDefault().getDisplayName());
        } else if (sqlType == DataTypes.DATE_SQL_TYPE) {
            return new ArrowType.Date(DateUnit.DAY);
        }
        throw new UnsupportedOperationException(dataType.getName());
    }

    /**
     * 将左表记录和右表记录加入到vector里，用于join算子
     */
    public static void addToVector(VectorSchemaRoot root, int rowIndex, Record leftRecord, Record rightRecord) {
        // 添加左表的记录
        int fieldIndexOffset = 0;
        addToVector(root, rowIndex, leftRecord, fieldIndexOffset);

        // 添加右表的记录
        fieldIndexOffset = leftRecord.fields.size();
        addToVector(root, rowIndex, rightRecord, fieldIndexOffset);
    }

    /**
     * 将记录加入vector中
     */
    public static void addToVector(VectorSchemaRoot root, int rowIndex, Record record) {
        addToVector(root, rowIndex, record, 0);
    }

    private static void addToVector(VectorSchemaRoot root, int rowIndex, Record record, int fieldIndexOffset) {
        List<Field> arrowFields = root.getSchema().getFields();
        for (int fieldIndex = 0; fieldIndex < record.fields.size(); fieldIndex++) {
            Field field = arrowFields.get(fieldIndexOffset + fieldIndex);
            // Use setSafe: it increases the buffer capacity if needed
            if (field.getFieldType().getType() instanceof Utf8) {
                ((VarCharVector) root.getVector(fieldIndexOffset + fieldIndex)).setSafe(rowIndex,
                        record.getString(fieldIndex).getBytes(StandardCharsets.UTF_8));
            } else if (field.getFieldType().getType() instanceof Int) {
                if (((Int) field.getFieldType().getType()).getBitWidth() == Integer.SIZE) {
                    ((IntVector) root.getVector(fieldIndexOffset + fieldIndex)).setSafe(rowIndex,
                            record.getInteger(fieldIndex));
                } else if (((Int) field.getFieldType().getType()).getBitWidth() == Long.SIZE) {
                    ((BigIntVector) root.getVector(fieldIndexOffset + fieldIndex)).setSafe(rowIndex,
                            record.getLong(fieldIndex));
                } else if (((Int) field.getFieldType().getType()).getBitWidth() == Short.SIZE) {
                    ((SmallIntVector) root.getVector(fieldIndexOffset + fieldIndex)).setSafe(rowIndex,
                            record.getShort(fieldIndex));
                } else if (((Int) field.getFieldType().getType()).getBitWidth() == Short.SIZE) {
                    ((TinyIntVector) root.getVector(fieldIndexOffset + fieldIndex)).setSafe(rowIndex,
                            record.getByte(fieldIndex));
                } else {
                    throw new UnsupportedOperationException();
                }
            } else if (field.getFieldType().getType() instanceof FloatingPoint) {
                if (((FloatingPoint) field.getFieldType().getType()).getPrecision() == FloatingPointPrecision.DOUBLE) {
                    ((Float8Vector) root.getVector(fieldIndexOffset + fieldIndex)).setSafe(rowIndex,
                            record.getDouble(fieldIndex));
                } else {
                    ((Float4Vector) root.getVector(fieldIndexOffset + fieldIndex)).setSafe(rowIndex,
                            record.getFloat(fieldIndex));
                }
            } else {
                throw new UnsupportedOperationException("unsupported data type: " + field.getFieldType().getType());
            }
        }
    }

    public static VectorBatch rowToVector(RowBatch rowBatch) {
        throw new UnsupportedOperationException("unsupported converting RowBatch to VectorBatch");
    }
    /**
     * 将列式数据转为行式数据
     */
    public static RowBatch vectorToRow(VectorBatch vectorBatch) {
        VectorSchemaRoot root = vectorBatch.getRoot();
        int rowCount = root.getRowCount();
        int fieldCount = root.getSchema().getFields().size();
        List<Record> records = new ArrayList<>(rowCount);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            List<io.polycat.catalog.common.model.record.Field> rowData = new ArrayList<>(fieldCount);
            for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
                Field field = root.getSchema().getFields().get(fieldIndex);
                FieldVector vector = root.getVector(fieldIndex);
                ArrowType arrowType = field.getFieldType().getType();
                if (arrowType instanceof Utf8) {
                    String value = new String(((VarCharVector) vector).get(rowIndex), StandardCharsets.UTF_8);
                    rowData.add(fieldIndex, new StringWritable(value));
                } else if (arrowType instanceof Int) {
                    if (((Int) arrowType).getBitWidth() == Integer.SIZE) {
                        int value = ((IntVector) vector).get(rowIndex);
                        rowData.add(fieldIndex, new IntWritable(value));
                    } else if (((Int) arrowType).getBitWidth() == Long.SIZE) {
                        long value = ((BigIntVector) vector).get(rowIndex);
                        rowData.add(fieldIndex, new LongWritable(value));
                    } else if (((Int) arrowType).getBitWidth() == Short.SIZE) {
                        short value = ((SmallIntVector) vector).get(rowIndex);
                        rowData.add(fieldIndex, new ShortWritable(value));
                    } else if (((Int) arrowType).getBitWidth() == Byte.SIZE) {
                        byte value = ((TinyIntVector) vector).get(rowIndex);
                        rowData.add(fieldIndex, new ByteWritable(value));
                    } else {
                        throw new UnsupportedOperationException();
                    }
                } else if (arrowType instanceof FloatingPoint) {
                    if (((FloatingPoint) arrowType).getPrecision() == FloatingPointPrecision.DOUBLE) {
                        double value = ((Float8Vector) vector).get(rowIndex);
                        rowData.add(fieldIndex, new DoubleWritable(value));
                    } else {
                        float value = ((Float4Vector) vector).get(rowIndex);
                        rowData.add(fieldIndex, new FloatWritable(value));
                    }
                } else {
                    throw new UnsupportedOperationException(arrowType + " is not supported");
                }
            }
            records.add(new Record(rowData));
        }
        return new RowBatch(records);
    }

    /**
     * 从Arrow中得到指定列的byte数组
     *
     * @param field  列的数据类型
     * @param vector 列数据
     * @return 列内容
     */
    public static byte[] getBytes(Field field, FieldVector vector) {
        int rowCount = vector.getValueCount();
        ArrowBuf buf = vector.getDataBuffer();
        int bufSize;
        ArrowType arrowType = field.getFieldType().getType();
        if (arrowType instanceof Utf8) {
            bufSize = ((VarCharVector) vector).getByteCapacity();
        } else if (arrowType instanceof Int) {
            bufSize = ((Int) arrowType).getBitWidth() / Byte.SIZE * rowCount;
        } else if (arrowType instanceof FloatingPoint) {
            if (((FloatingPoint) arrowType).getPrecision() == FloatingPointPrecision.DOUBLE) {
                bufSize = Double.BYTES * rowCount;
            } else {
                bufSize = Float.BYTES * rowCount;
            }
        } else if (arrowType instanceof Bool) {
            // TODO: 这里算的size可能有问题，要看arrow对bool内部的bool vector是怎么写入的(内部应该是bits)
            if (rowCount % 4 == 0) {
                bufSize = rowCount / 4;
            } else {
                bufSize = rowCount / 4 + 1;
            }
        } else {
            throw new UnsupportedOperationException(arrowType + " is not supported");
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(bufSize);
        buf.getBytes(0, byteBuffer);
        byteBuffer.flip();
        return byteBuffer.array();
    }
}
