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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

import io.polycat.catalog.common.exception.CarbonSqlException;
import io.polycat.catalog.common.serialization.Writable;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;

/**
 * 代表{@link Record}里的一个字段
 */
public abstract class Field implements Writable, Serializable, Comparable<Field> {

    public abstract DataType getType();

    // 为了避免boxing/unboxing，每个数据类型实现一个方法

    public void setValue(boolean value) {
        throw new CarbonSqlException("unsupport set boolean value, filedType=" + getType());
    }

    public void setValue(byte value) {
        throw new CarbonSqlException("unsupport set byte value, filedType=" + getType());
    }

    public void setValue(short value) {
        throw new CarbonSqlException("unsupport set short value, filedType=" + getType());
    }

    public void setValue(int value) {
        throw new CarbonSqlException("unsupport set int value, filedType=" + getType());
    }

    public void setValue(long value) {
        throw new CarbonSqlException("unsupport set long value, filedType=" + getType());
    }

    public void setValue(float value) {
        throw new CarbonSqlException("unsupport set float value, filedType=" + getType());
    }

    public void setValue(double value) {
        throw new CarbonSqlException("unsupport set double value, filedType=" + getType());
    }

    public void setValue(BigDecimal value) {
        throw new CarbonSqlException("unsupport set double value, filedType=" + getType());
    }

    public void setValues(String values) {
        throw new CarbonSqlException("unsupport set String value, filedType=" + getType());
    }
    
    public void setValue(byte[] value) {
        throw new CarbonSqlException("unsupport set blob value, filedType=" + getType());
    }

    public void setValue(Object value) {
        throw new CarbonSqlException("unsupport set blob value, filedType=" + getType());
    }

    public byte[] getBlob() {
        throw new CarbonSqlException("wrong data type: " + getType());
    }

    public boolean getBoolean() {
        throw new CarbonSqlException("wrong data type: " + getType());
    }

    public byte getByte() {
        throw new CarbonSqlException("wrong data type: " + getType());
    }

    public short getShort() {
        throw new CarbonSqlException("wrong data type: " + getType());
    }

    public int getInteger() {
        throw new CarbonSqlException("wrong data type: " + getType());
    }

    public long getLong() {
        throw new CarbonSqlException("wrong data type: " + getType());
    }

    public float getFloat() {
        throw new CarbonSqlException("wrong data type: " + getType());
    }

    public double getDouble() {
        throw new CarbonSqlException("wrong data type: " + getType());
    }

    public String getString() {
        throw new CarbonSqlException("wrong data type: " + getType());
    }

    public Date getDate() {
        throw new CarbonSqlException("wrong data type: " + getType());
    }
    
    public BigDecimal getDecimal() {
        throw new CarbonSqlException("wrong data type: " + getType());
    }

    public Timestamp getTimestamp() {
        throw new CarbonSqlException("wrong data type: " + getType());
    }

    public Object getObject() {
        throw new CarbonSqlException("wrong data type: " + getType());
    }

    public void negate() {
        throw new CarbonSqlException("unsupport minus, filedType=" + getType());
    }

    public void serialize(DataOutput out) throws IOException {
        out.writeInt(getType().getSqlType());
        write(out);
    }

    public static Field deserialize(DataInput in) throws IOException {
        Field field = DataTypes.createEmptyField(in.readInt());
        field.readFields(in);
        return field;
    }

    public byte[] serialize() {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream outputStream = new ObjectOutputStream(bytes)) {
            outputStream.writeInt(getType().getSqlType());
            write(outputStream);
            outputStream.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new CarbonSqlException("failed to serialize", e);
        }
    }

    public static Field deserialize(byte[] input) {
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(input);
                ObjectInputStream inputStream = new ObjectInputStream(bytes)) {
            Field field = DataTypes.createEmptyField(inputStream.readInt());
            field.readFields(inputStream);
            return field;
        } catch (IOException e) {
            throw new CarbonSqlException("failed to deserialize", e);
        }
    }

    public void setValues(String[] values) {
        throw new CarbonSqlException("unsupport set String[] value, filedType=" + getType());
    }

    public String[] getStringArray() {
        throw new CarbonSqlException("wrong data type: " + getType());
    }
}
