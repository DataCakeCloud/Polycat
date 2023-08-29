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
import java.math.RoundingMode;
import java.util.Objects;

import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;

/**
 * @singe 2020/12/8
 */
public class DecimalWritable extends Field {
    
    private BigDecimal value;

    public DecimalWritable() {
    }

    public DecimalWritable(BigDecimal value) {
        this.value = value;
    }
    
    @Override
    public DataType getType() {
        return DataTypes.createDecimalType(value.precision(), value.scale());
    }

    @Override
    public void setValue(BigDecimal value) {
        this.value = value;
    }

    @Override
    public BigDecimal getDecimal() {
        return value;
    }

    @Override
    public String getString() {
        return String.valueOf(value);
    }

    @Override
    public void negate() {
        value = value.negate();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(value.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value = new BigDecimal(in.readUTF());
    }
    
    @Override
    public int compareTo(Field o) {
        DecimalWritable other = (DecimalWritable) o;
        return value.compareTo(other.value);    
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DecimalWritable)) {
            return false;
        }
        DecimalWritable that = (DecimalWritable) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
    
    public static DecimalWritable decimalDivide(BigDecimal left, BigDecimal right) {
        // todo:这里遵从h2的除法逻辑便于UT验证
        BigDecimal diviValue = left.divide(right, left.scale() + 25, RoundingMode.HALF_DOWN);
        if (diviValue.signum() == 0) {
            diviValue = BigDecimal.ZERO;
        } else if (diviValue.scale() > 0 && !diviValue.unscaledValue().testBit(0)) {
            diviValue = new BigDecimal(diviValue.stripTrailingZeros().toPlainString());
        }
        return new DecimalWritable(diviValue);
    }
}
