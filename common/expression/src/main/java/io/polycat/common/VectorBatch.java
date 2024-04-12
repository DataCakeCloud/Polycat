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

import io.polycat.catalog.common.serialization.Writable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.common.util.ArrowUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public class VectorBatch implements Records {

    private VectorSchemaRoot root;

    public VectorBatch() {
    }

    public VectorBatch(VectorSchemaRoot root) {
        this.root = root;
    }

    public static VectorBatch empty() {
        return new VectorBatch(null);
    }

    public VectorSchemaRoot getRoot() {
        return root;
    }

    public FieldVector getFieldVector(int columnIndex) {
        return root.getVector(columnIndex);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(root != null);
        if (root != null) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, output)) {
                writer.start();
                for (int i = 0; i < 1; i++) {
                    writer.writeBatch();
                }
                writer.end();
            }
            Writable.writeByteArray(out, output.toByteArray());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (in.readBoolean()) {
            byte[] bytes = Writable.readByteArray(in);
            ByteArrayInputStream input = new ByteArrayInputStream(bytes);
            ArrowStreamReader reader = new ArrowStreamReader(input, new RootAllocator(Integer.MAX_VALUE));
            // TODO：目前只处理了第一个batch，要处理多于一个batch的情况
            reader.loadNextBatch();
            root = reader.getVectorSchemaRoot();

            // TODO: 当前没有关闭reader，如果关闭reader，root也会被Arrow清除，导致加密失败，要研究一下ArrowReader的使用方式
        }
    }

    @Override
    public Iterator<Record> iterator() {
        return asRowBatch().iterator();
    }

    @Override
    public RowBatch asRowBatch() {
        return ArrowUtil.vectorToRow(this);
    }

    @Override
    public VectorBatch asVectorBatch() {
        return this;
    }

    @Override
    public int size() {
        return root.getRowCount();
    }

    @Override
    public String toString() {
        return "VectorBatch(" + root.getRowCount() + " rows" + ')';
    }
}
