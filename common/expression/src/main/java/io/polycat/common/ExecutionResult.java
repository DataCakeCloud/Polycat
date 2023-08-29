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

import io.polycat.catalog.common.model.Schema;
import io.polycat.catalog.common.model.record.Record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Iterator;

public class ExecutionResult implements PrintableRecords {

    private final Schema schema;

    private final Records records;

    private final boolean isSingleResult;

    public ExecutionResult() {
        this(new Schema(Collections.emptyList()));
    }

    public ExecutionResult(Schema schema) {
        this(schema, RowBatch.empty());
    }

    public ExecutionResult(Schema schema, Records records) {
        this(schema, records, false);
    }

    public ExecutionResult(Schema schema, Records records, boolean isSingleResult) {
        this.schema = schema;
        this.records = records;
        this.isSingleResult = isSingleResult;
    }

    @Override
    public RowBatch asRowBatch() {
        return records.asRowBatch();
    }

    @Override
    public VectorBatch asVectorBatch() {
        return records.asVectorBatch();
    }

    @Override
    public int size() {
        return records.size();
    }

    public boolean isEmpty() {
        return records.size() == 0;
    }

    @Override
    public Iterator<Record> iterator() {
        return records.iterator();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedEncodingException();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new UnsupportedEncodingException();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    public Records getRecords() {
        return records;
    }

    public boolean isSingleResult() {
        return isSingleResult;
    }
}
