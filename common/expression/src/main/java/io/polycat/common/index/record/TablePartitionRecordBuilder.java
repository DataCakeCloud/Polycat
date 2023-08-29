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
package io.polycat.common.index.record;

import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.common.expression.Attribute;
import io.polycat.common.expression.Expression;
import io.polycat.common.expression.FieldExpression;
import io.polycat.common.expression.rule.ResolveExpressionVisitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TablePartitionRecordBuilder {

    private List<String> colFilterNames;
    private Expression filterExpression;

    public TablePartitionRecordBuilder(Expression expression, HashMap<String, String> schemaMap,
        List<Column> partitionColumns) {
        colFilterNames = makeFilterColumnNames(expression, partitionColumns);
        List<Attribute> attributeList = new ArrayList<>(colFilterNames.size());
        for (int i = 0; i < colFilterNames.size(); i++) {
            attributeList.add(new Attribute(colFilterNames.get(i), DataTypes.valueOf(schemaMap.get(colFilterNames.get(i)))));
        }
        ResolveExpressionVisitor expressionVisitor = new ResolveExpressionVisitor(attributeList);
        filterExpression = expression.transformUp(x -> x.accept(expressionVisitor));
    }

    private List<String> makeFilterColumnNames(Expression expression, List<Column> partitionColumns) {
        List<String> columnFilterNames = new ArrayList<>(partitionColumns.size());
        List<String> partitionNames = new ArrayList<>(partitionColumns.size());
        for (int i = 0; i < partitionColumns.size(); i++) {
            columnFilterNames.add(partitionColumns.get(i).getColumnName());
            partitionNames.add(partitionColumns.get(i).getColumnName());
        }

        expression.transformUp(expr -> {
            if (expr instanceof FieldExpression) {
                partitionNames.remove(((FieldExpression) expr).getName());
            }
            return expr;
        });

        partitionNames.forEach(name -> {
            columnFilterNames.remove(name);
        });
        return columnFilterNames;
    }

    public boolean evaluateToBool(Record record) {
        return filterExpression.evaluateToBool(record);
    }

    public Record build(HashMap<String, String> columnMap, HashMap<String, String> schemaMap) {
        Record record = new Record();
        for (int i = 0; i < colFilterNames.size(); i++) {
            if (!columnMap.containsKey(colFilterNames.get(i)) ||
            !schemaMap.containsKey(colFilterNames.get(i))) {
                throw new RuntimeException(String.format("Partition column(%s) does not exist", colFilterNames.get(i)));
            }

            Field field = Record.convertStringToField(columnMap.get(colFilterNames.get(i)),
                    DataTypes.valueOf(schemaMap.get(colFilterNames.get(i))));
            record.add(field);
        }
        return record;
    }
}
