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
package io.polycat.catalog.common.model;

import com.google.protobuf.ByteString;
import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.NullWritable;
import io.polycat.catalog.common.types.DataType;
import io.polycat.common.expression.Expression;
import io.polycat.common.expression.FieldExpression;
import io.polycat.common.expression.LiteralExpression;
import io.polycat.common.expression.NullExpression;
import io.polycat.common.expression.rewrite.PartitionMinMaxFilterRewriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionMinMaxFilter {
    private List<ColumnObject> tableSchema;
    private int numberOfColumn;
    private boolean[] presentMinMax;
    private boolean[] presentFieldInFilter;
    private boolean[] presentFieldInPartition;
    private boolean[] absentFieldInPartition;
    private int[] fieldIndexInPartition;
    private Expression partitionExpression;

    public PartitionMinMaxFilter(List<ColumnObject> tableSchema, List<ColumnObject> partitionSchema, Expression minMaxFilter) {
        this.tableSchema = tableSchema;
        numberOfColumn = tableSchema.size();
        collectPresentMinMax(minMaxFilter);
        collectPresentFieldInFilter(minMaxFilter);
        collectPresentFieldInPartition(tableSchema, partitionSchema);
        rewriteFilterIfNeeded(tableSchema, minMaxFilter);
    }

    private void collectPresentMinMax(Expression minMaxFilter) {
        presentMinMax = new boolean[numberOfColumn * 2];
        minMaxFilter.transformUp(expr -> {
            if (expr instanceof FieldExpression) {
                int index = expr.getFieldIndexes().get(0);
                presentMinMax[index] = true;
            }
            return expr;
        });
    }

    private void collectPresentFieldInFilter(Expression minMaxFilter) {
        presentFieldInFilter = new boolean[numberOfColumn];
        for (int i = 0; i < numberOfColumn; i++) {
            presentFieldInFilter[i] = presentMinMax[i * 2] || presentMinMax[i * 2 + 1];
        }
    }

    private void collectPresentFieldInPartition(List<ColumnObject> tableSchema, List<ColumnObject> segmentSchema) {
        Map<String, Integer> segmentColumnMap = new HashMap<>(segmentSchema.size());
        for (int i = 0; i < segmentSchema.size(); i++) {
            segmentColumnMap.put(segmentSchema.get(i).getName(), i);
        }
        presentFieldInPartition = new boolean[numberOfColumn];
        absentFieldInPartition = new boolean[numberOfColumn];
        fieldIndexInPartition = new int[numberOfColumn];
        for (int i = 0; i < numberOfColumn; i++) {
            presentFieldInPartition[i] = false;
            absentFieldInPartition[i] = false;
            fieldIndexInPartition[i] = -1;
            if (presentFieldInFilter[i]) {
                Integer index = segmentColumnMap.get(tableSchema.get(i).getName());
                if (index == null) {
                    absentFieldInPartition[i] = true;
                } else {
                    presentFieldInPartition[i] = true;
                    fieldIndexInPartition[i] = index;
                }
            }
        }
    }

    private void rewriteFilterIfNeeded(List<ColumnObject> tableSchema, Expression minMaxFilter) {
        if (isRewriteNeeded()) {
            Map<Integer, Expression> absentFieldMap = collectAbsentFieldMap(tableSchema, absentFieldInPartition);
            partitionExpression = new PartitionMinMaxFilterRewriter(absentFieldMap).execute(minMaxFilter);
        } else {
            partitionExpression = minMaxFilter;
        }
    }

    private static Map<Integer, Expression> collectAbsentFieldMap(List<ColumnObject> tableSchema,
            boolean[] absentFieldInSegment) {
        Map<Integer, Expression> absentExpressionMap = new HashMap<>(absentFieldInSegment.length);
        for (int i = 0; i < absentFieldInSegment.length; i++) {
            if (absentFieldInSegment[i]) {
                ColumnObject column = tableSchema.get(i);
                byte[] value = null;
                ByteString defaultValue = ByteString.copyFrom(value);
                Expression fieldExpression;
                if (defaultValue.isEmpty()) {
                    Field field = NullWritable.getInstance();
                    fieldExpression = new NullExpression(field.getType());
                } else {
                    Field field = DataTypeUtils.toField(column.getDataType());
                    DataTypeUtils.createTransform(field).transform(defaultValue);
                    fieldExpression = new LiteralExpression(field);
                }
                absentExpressionMap.put(i * 2, fieldExpression);
                absentExpressionMap.put(i * 2 + 1, fieldExpression);
            }
        }
        return absentExpressionMap;
    }

    private boolean isRewriteNeeded() {
        for (boolean absent : absentFieldInPartition) {
            if (absent) {
                return true;
            }
        }
        return false;
    }

    public DataType dataTypeOfColumn(int index) {
        return tableSchema.get(index).getDataType();
    }

    public int getNumberOfColumn() {
        return numberOfColumn;
    }

    public int getNumberOfMinMaxColumn() {
        return numberOfColumn * 2;
    }

    public Expression getPartitionExpression() {
        return partitionExpression;
    }

    public boolean isFieldPresentInPartition(int index) {
        return presentFieldInPartition[index];
    }

    public boolean isMinMaxPresent(int index) {
        return presentMinMax[index];
    }

    public int fieldIndexInPartition(int i) {
        return fieldIndexInPartition[i];
    }
}
