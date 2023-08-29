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
package io.polycat.common;

import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.serialization.Writable;
import io.polycat.common.util.ArrowUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * A batch of row-oriented record
 */
public class RowBatch implements Records {

    private List<Record> records;

    public RowBatch() {
        records = new LinkedList<>();
    }

    public RowBatch(List<Record> records) {
        this.records = records;
    }

    public static RowBatch empty() {
        return new RowBatch(new LinkedList<>());
    }

    public List<Record> getRecords() {
        return records;
    }

    @Override
    public RowBatch asRowBatch() {
        return this;
    }

    @Override
    public VectorBatch asVectorBatch() {
        return ArrowUtil.rowToVector(this);
    }

    @Override
    public int size() {
        return records.size();
    }

    public void add(Record record) {
        records.add(record);
    }

    public void addAll(RowBatch rowBatch) {
        records.addAll(rowBatch.getRecords());
    }

    public RowBatch shadowCopyOf(List<Record> rowBatch) {
        return new RowBatch(rowBatch);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Writable.writeList(out, records);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        records = Writable.readList(in, Record.class);
    }

    @Override
    public Iterator<Record> iterator() {
        return new Iterator<Record>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < records.size();
            }

            @Override
            public Record next() {
                return records.get(index++);
            }
        };
    }

    @Override
    public String toString() {
        return "RowBatch(" + records.size() + " rows" + ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowBatch records1 = (RowBatch) o;
        return Objects.equals(records, records1.records);
    }

    @Override
    public int hashCode() {
        return Objects.hash(records);
    }
}
