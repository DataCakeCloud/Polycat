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
import java.util.Objects;

import io.polycat.catalog.common.exception.CarbonSqlException;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;

public class ShortWritable extends Field {

    private short value;

    public ShortWritable() {
    }

    public ShortWritable(short value) {
        this.value = value;
    }

    @Override
    public DataType getType() {
        return DataTypes.SMALLINT;
    }

    @Override
    public void setValue(short value) {
        this.value = value;
    }

    @Override
    public short getShort() {
        return value;
    }

    @Override
    public long getLong() {
        return value;
    }

    @Override
    public int getInteger() {
        return value;
    }

    @Override
    public String getString() {
        return String.valueOf(value);
    }
    
    @Override
    public boolean getBoolean() {
        // 解决mysql使用int类型处理布尔值导致的问题
        if (value == 0) {
            return false;
        } else if (value == 1) {
            return true;
        } else {
            throw new CarbonSqlException("ShortWritable can't cast to boolean by value=" + value);
        }
    }

    @Override
    public void negate() {
        value *= -1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeShort(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value = in.readShort();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShortWritable that = (ShortWritable) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    @Override
    public int compareTo(Field o) {
        return Short.compare(value, ((ShortWritable) o).value);
    }
}
