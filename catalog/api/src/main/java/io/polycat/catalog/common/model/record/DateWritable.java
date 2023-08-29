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
import java.sql.Date;
import java.util.Objects;

import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;

public class DateWritable extends Field {

    private Date date;

    public DateWritable() {
    }

    public DateWritable(Date date) {
        this.date = date;
    }

    @Override
    public Date getDate() {
        return date;
    }

    @Override
    public DataType getType() {
        return DataTypes.DATE;
    }

    @Override
    public String getString() {
        return date.toString();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(date.getTime());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        date = new Date(in.readLong());
    }

    @Override
    public int compareTo(Field o) {
        return date.compareTo(((DateWritable) o).date);
    }

    @Override
    public String toString() {
        return date.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DateWritable that = (DateWritable) o;
        return Objects.equals(date, that.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(date);
    }
}
