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
package io.polycat.catalog.common.model.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.model.Schema;
import io.polycat.catalog.common.model.SchemaField;
import io.polycat.catalog.common.serialization.Writable;
import io.polycat.catalog.common.types.DataTypes;

/**
 * 代表一行记录，会在SQL执行过程中在Processor之间传递此对象
 */
public class Record implements Writable, Serializable {

    public List<Field> fields;

    public Record() {
    }

    public Record(List<Field> fields) {
        this.fields = fields;
    }

    public Record(Object... fields) {
        this.fields = new ArrayList<>(fields.length);
        for (Object field : fields) {
            // 有可能直接传Filed进来而不是值
            if (field == null) {
                this.fields.add(NullWritable.getInstance());
            } else if (field instanceof String) {
                this.fields.add(new StringWritable((String) field));
            } else if (field instanceof Integer) {
                this.fields.add(new IntWritable((Integer) field));
            } else if (field instanceof Double) {
                this.fields.add(new DoubleWritable((Double) field));
            } else if (field instanceof BigDecimal) {
                this.fields.add(new DecimalWritable((BigDecimal) field));
            } else if (field instanceof Float) {
                this.fields.add(new FloatWritable((Float) field));
            } else if (field instanceof Byte) {
                this.fields.add(new ByteWritable((Byte) field));
            } else if (field instanceof Short) {
                this.fields.add(new ShortWritable((Short) field));
            } else if (field instanceof Long) {
                this.fields.add(new LongWritable((Long) field));
            } else if (field instanceof Date) {
                this.fields.add(new DateWritable((Date) field));
            } else if (field instanceof Timestamp) {
                this.fields.add(new TimestampWritable((Timestamp) field));
            } else if (field instanceof Boolean) {
                this.fields.add(new BooleanWritable((Boolean) field));
            } else if (field instanceof String[]) {
                String result = convertMultString((String[])field);
                if (result != null) {
                    this.fields.add(new StringWritable(result));
                } else {
                    this.fields.add(new StringWritable(""));
                }
            }else {
                // 不支持的都先转string。避免报错
                this.fields.add(new StringWritable(field.toString()));
                // todo 后续需要添加一个通用地解析record类型的field
            }
        }
    }

    public Record(Field... writableFields) {
        this.fields = Arrays.asList(writableFields);
    }

    public Record(int numFields) {
        fields = new ArrayList<>(numFields);
    }

    public Object getFields() {
        return fields;
    }

    public static Record ofStringValue(String[] fieldValues, Schema schema) {
        List<SchemaField> schemaFields = schema.getSchemaFields();
        List<Field> fields = new ArrayList<>(fieldValues.length);
        for (int i = 0; i < fieldValues.length; i++) {
            fields.add(convertStringToField(fieldValues[i], schemaFields.get(i).getDataType()));
        }
        return new Record(fields);
    }

    public static Field convertFieldByType(Field field, DataType dataType) {
        if (dataType.equals(field.getType())) {
            return field;
        }
        return convertStringToField(field.getString(), dataType);
    }

    public static Field convertStringToField(String fieldValue, DataType dataType) {
        if (fieldValue == null) {
            return NullWritable.getInstance();
        }
        int sqlType = dataType.getSqlType();
        if (sqlType == DataTypes.STRING_SQL_TYPE) {
            return new StringWritable(fieldValue);
        } else if (sqlType == DataTypes.DATE_SQL_TYPE) {
            throw new UnsupportedOperationException();
        } else if (sqlType == DataTypes.TIMESTAMP_SQL_TYPE) {
            throw new UnsupportedOperationException();
        } else if (sqlType == DataTypes.BOOLEAN_SQL_TYPE) {
            return new BooleanWritable(Boolean.parseBoolean(fieldValue));
        } else if (sqlType == DataTypes.SMALLINT_SQL_TYPE) {
            return new ShortWritable(Short.parseShort(fieldValue));
        } else if (sqlType == DataTypes.INTEGER_SQL_TYPE) {
            return new IntWritable(Integer.parseInt(fieldValue));
        } else if (sqlType == DataTypes.BIGINT_SQL_TYPE) {
            return new LongWritable(Long.parseLong(fieldValue));
        } else if (sqlType == DataTypes.FLOAT_SQL_TYPE) {
            return new FloatWritable(Float.parseFloat(fieldValue));
        } else if (sqlType == DataTypes.DOUBLE_SQL_TYPE) {
            return new DoubleWritable(Double.parseDouble(fieldValue));
        } else if (sqlType == DataTypes.DECIMAL_SQL_TYPE) {
            throw new UnsupportedOperationException();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public Record add(Field writable) {
        if (fields == null) {
            fields = new ArrayList<>();
        }
        fields.add(writable);
        return this;
    }

    public Record add(Object... fields) {
        for (Object field : fields) {
            // 有可能直接传Filed进来而不是值
            if (field == null) {
                this.fields.add(NullWritable.getInstance());
            } else if (field instanceof String) {
                this.fields.add(new StringWritable((String) field));
            } else if (field instanceof Integer) {
                this.fields.add(new IntWritable((Integer) field));
            } else if (field instanceof Double) {
                this.fields.add(new DoubleWritable((Double) field));
            } else if (field instanceof BigDecimal) {
                this.fields.add(new DecimalWritable((BigDecimal) field));
            } else if (field instanceof Float) {
                this.fields.add(new FloatWritable((Float) field));
            } else if (field instanceof Byte) {
                this.fields.add(new ByteWritable((Byte) field));
            } else if (field instanceof Short) {
                this.fields.add(new ShortWritable((Short) field));
            } else if (field instanceof Long) {
                this.fields.add(new LongWritable((Long) field));
            } else if (field instanceof Date) {
                this.fields.add(new DateWritable((Date) field));
            } else if (field instanceof Timestamp) {
                this.fields.add(new TimestampWritable((Timestamp) field));
            } else if (field instanceof Boolean) {
                this.fields.add(new BooleanWritable((Boolean) field));
            } else if (field instanceof String[]) {
                String result = convertMultString((String[])field);
                if (result != null) {
                    this.fields.add(new StringWritable(result));
                } else {
                    this.fields.add(new StringWritable(""));
                }
            }else {
                // 不支持的都先转string。避免报错
                this.fields.add(new StringWritable(field.toString()));
                // todo 后续需要添加一个通用地解析record类型的field
            }
        }
        return this;
    }

    public Field getField(int index) {
        return fields.get(index);
    }
    
    public void setValue(int index, Field value) {
        fields.set(index, value);
    }
    
    public void setValue(int index, byte value) {
        fields.get(index).setValue(value);
    }

    public void setValue(int index, short value) {
        fields.get(index).setValue(value);
    }

    public void setValue(int index, int value) {
        fields.get(index).setValue(value);
    }

    public void setValue(int index, long value) {
        fields.get(index).setValue(value);
    }

    public void setValue(int index, float value) {
        fields.get(index).setValue(value);
    }

    public void setValue(int index, double value) {
        fields.get(index).setValue(value);
    }

    public void setValue(int index, String value) {
        fields.get(index).setValues(value);
    }

    public boolean getBoolean(int index) {
        return fields.get(index).getBoolean();
    }

    public byte getByte(int index) {
        return fields.get(index).getByte();
    }

    public short getShort(int index) {
        return fields.get(index).getShort();
    }

    public int getInteger(int index) {
        return fields.get(index).getInteger();
    }

    public long getLong(int index) {
        return fields.get(index).getLong();
    }

    public float getFloat(int index) {
        return fields.get(index).getFloat();
    }

    public double getDouble(int index) {
        return fields.get(index).getDouble();
    }

    public String getString(int index) {
        return fields.get(index).getString();
    }
    
    public byte[] getBlob(int index) {
        return fields.get(index).getBlob();
    }

    public Date getDate(int index) {
        return fields.get(index).getDate();
    }

    public Timestamp getTimestamp(int index) {
        return fields.get(index).getTimestamp();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Record record = (Record) o;
        return Objects.equals(fields, record.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }

    @Override
    public String toString() {
        return "Record" + fields;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(fields.size());
        for (Field field : fields) {
            field.serialize(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        fields = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Field field = Field.deserialize(in);
            fields.add(field);
        }
    }

    private String convertMultString(String[] in) {
        String result = null;
        for (int i = 0; i < in.length; i++) {
            if (result == null) {
                result = in[i];
            } else {
                result += "," + in[i];
            }
        }
        return result;
    }
}
