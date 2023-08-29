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
package io.polycat.sql.common.expression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.google.common.collect.Lists;
import io.polycat.catalog.common.model.Schema;
import io.polycat.catalog.common.model.SchemaField;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.common.RowBatch;
import io.polycat.common.util.ArrowUtil;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ArrowTestCase {

    // Test writing RowBatch to arrow vectors and reading from it
    @Test
    public void testReadWrite() throws IOException {
        Schema schema = new Schema(
                Lists.newArrayList(
                        new SchemaField("name", DataTypes.STRING),
                        new SchemaField("age", DataTypes.INTEGER),
                        new SchemaField("salary", DataTypes.DOUBLE)));

        RowBatch rowBatch = new RowBatch(
                Lists.newArrayList(
                        new Record("amy", 3, 83.9),
                        new Record("bob", 5, 93.63),
                        new Record("cat", 6, 938.3),
                        new Record("dog", 9, 129.4)));

        org.apache.arrow.vector.types.pojo.Schema arrowSchema = ArrowUtil.convertToArrowSchema(schema);
        VectorSchemaRoot root = fillVector(schema, arrowSchema, rowBatch);

        // Write the stream.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
            writer.start();
            for (int i = 0; i < 1; i++) {
                writer.writeBatch();
            }
            writer.end();
        }

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        try (ArrowStreamReader reader = new ArrowStreamReader(in, new RootAllocator())) {
            // Empty should return false
            reader.loadNextBatch();
            VectorSchemaRoot newRoot = reader.getVectorSchemaRoot();
            org.apache.arrow.vector.types.pojo.Schema s = newRoot.getSchema();
            Assertions.assertEquals(arrowSchema, s);
            assertVector(newRoot);
        }

    }

    private VectorSchemaRoot fillVector(Schema schema, org.apache.arrow.vector.types.pojo.Schema arrowSchema,
            RowBatch rowBatch) {
        List<SchemaField> schemaFields = schema.getSchemaFields();
        List<Record> records = rowBatch.getRecords();
        VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(arrowSchema, new RootAllocator());
        schemaRoot.setRowCount(records.size());
        for (int rowIndex = 0; rowIndex < records.size(); rowIndex++) {
            for (int fieldIndex = 0; fieldIndex < schemaFields.size(); fieldIndex++) {
                SchemaField schemaField = schemaFields.get(fieldIndex);
                // Use setSafe: it increases the buffer capacity if needed
                int sqlType = schemaField.getDataType().getSqlType();
                if (sqlType == DataTypes.STRING_SQL_TYPE) {
                    ((VarCharVector) schemaRoot.getVector(schemaField.getName())).setSafe(rowIndex,
                        records.get(rowIndex).getField(fieldIndex).getString()
                            .getBytes(StandardCharsets.UTF_8));
                } else if (sqlType == DataTypes.INTEGER_SQL_TYPE) {
                    ((IntVector) schemaRoot.getVector(schemaField.getName())).setSafe(rowIndex,
                        records.get(rowIndex).getField(fieldIndex).getInteger());
                } else if (sqlType == DataTypes.BIGINT_SQL_TYPE) {
                    ((BigIntVector) schemaRoot.getVector(schemaField.getName())).setSafe(rowIndex,
                        records.get(rowIndex).getField(fieldIndex).getLong());
                } else if (sqlType == DataTypes.DOUBLE_SQL_TYPE) {
                    ((Float8Vector) schemaRoot.getVector(schemaField.getName())).setSafe(rowIndex,
                        records.get(rowIndex).getField(fieldIndex).getDouble());
                } else if (sqlType == DataTypes.FLOAT_SQL_TYPE) {
                    ((Float4Vector) schemaRoot.getVector(schemaField.getName())).setSafe(rowIndex,
                        records.get(rowIndex).getField(fieldIndex).getFloat());
                } else if (sqlType == DataTypes.TINYINT_SQL_TYPE) {
                    ((TinyIntVector) schemaRoot.getVector(schemaField.getName())).setSafe(rowIndex,
                        records.get(rowIndex).getField(fieldIndex).getByte());
                } else if (sqlType == DataTypes.SMALLINT_SQL_TYPE) {
                    ((SmallIntVector) schemaRoot.getVector(schemaField.getName())).setSafe(rowIndex,
                        records.get(rowIndex).getField(fieldIndex).getShort());
                } else if (sqlType == DataTypes.BOOLEAN_SQL_TYPE) {
                    ((BitVector) schemaRoot.getVector(schemaField.getName())).setSafe(rowIndex,
                        records.get(rowIndex).getField(fieldIndex).getBoolean() ? 1 : 0);
                } else {
                    throw new UnsupportedOperationException(
                        "unsupported data type: " + schemaField.getDataType());
                }
            }
        }
        for (SchemaField schemaField : schemaFields) {
            schemaRoot.getVector(schemaField.getName()).setValueCount(records.size());
        }
        return schemaRoot;
    }

    private static void assertVector(VectorSchemaRoot root) {
        VarCharVector v1 = (VarCharVector) root.getVector("name");
        Assertions.assertEquals(4, root.getRowCount());
        Assertions.assertEquals("amy", new String(v1.get(0), StandardCharsets.UTF_8));
        Assertions.assertEquals("bob", new String(v1.get(1), StandardCharsets.UTF_8));
        Assertions.assertEquals("cat", new String(v1.get(2), StandardCharsets.UTF_8));
        Assertions.assertEquals("dog", new String(v1.get(3), StandardCharsets.UTF_8));

        IntVector v2 = (IntVector) root.getVector("age");
        Assertions.assertEquals(3, v2.get(0));
        Assertions.assertEquals(5, v2.get(1));
        Assertions.assertEquals(6, v2.get(2));
        Assertions.assertEquals(9, v2.get(3));

        Float8Vector v3 = (Float8Vector) root.getVector("salary");
        Assertions.assertEquals(83.9, v3.get(0), 0);
        Assertions.assertEquals(93.63, v3.get(1), 0);
        Assertions.assertEquals(938.3, v3.get(2), 0);
        Assertions.assertEquals(129.4, v3.get(3), 0);
    }
}
