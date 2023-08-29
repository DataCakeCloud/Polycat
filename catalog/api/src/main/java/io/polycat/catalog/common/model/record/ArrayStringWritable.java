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
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;

public class ArrayStringWritable extends Field {

    private String[] values;

    public ArrayStringWritable() {
    }

    public ArrayStringWritable(String[] values) {
        Objects.requireNonNull(values);
        this.values = values;
    }

    @Override
    public DataType getType() {
        return DataTypes.STRING;
    }

    @Override
    public void setValues(String[] values) {
        Objects.requireNonNull(values);
        this.values = values;
    }

    @Override
    public String[] getStringArray() {
        return values;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        if (values == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(values.length);
        for (String value : values) {
            out.writeUTF(value);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int len = in.readInt();
        if (len >= 0) {
            values = new String[len];
            for (int i = 0; i < len; i++) {
                values[i] = in.readUTF();
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ArrayStringWritable that = (ArrayStringWritable) o;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return Arrays.stream(values).collect(Collectors.joining("[", ",", "]"));
    }

    @Override
    public int compareTo(Field o) {
        if (o == null) {
            return -1;
        }
        String[] stringArray = o.getStringArray();
        int length = Math.min(values.length, stringArray.length);
        int tempResult = 0;
        for (int i = 0; i < length; i++) {
            tempResult = values[i].compareTo(stringArray[i]);
            if (tempResult != 0) {
                return tempResult;
            }
        }
        return Integer.compare(values.length,stringArray.length);
    }
}
