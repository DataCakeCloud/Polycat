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
import java.math.BigDecimal;
import java.util.Objects;

import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;

public class FloatWritable extends Field {

    private float value;

    public FloatWritable() {
    }

    public FloatWritable(float value) {
        this.value = value;
    }

    @Override
    public DataType getType() {
        return DataTypes.FLOAT;
    }

    @Override
    public void setValue(float value) {
        this.value = value;
    }

    @Override
    public BigDecimal getDecimal() {
        // Float转BigDecimal存在精度丢失，使用toString可避免精度损失
        return new BigDecimal(Float.toString(value));
    }
    
    @Override
    public float getFloat() {
        return value;
    }

    @Override
    public double getDouble() {
        return value;
    }

    @Override
    public String getString() {
        return String.valueOf(value);
    }

    @Override
    public void negate() {
        value *= -1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value = in.readFloat();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FloatWritable that = (FloatWritable) o;
        return Float.compare(that.value, value) == 0;
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
        FloatWritable other = (FloatWritable) o;
        return Float.compare(value, other.value);
    }
}
