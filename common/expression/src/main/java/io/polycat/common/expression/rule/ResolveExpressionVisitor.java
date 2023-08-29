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
package io.polycat.common.expression.rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.polycat.catalog.common.exception.CarbonSqlException;
import io.polycat.common.expression.Attribute;
import io.polycat.common.expression.Expression;
import io.polycat.common.expression.FieldExpression;
import io.polycat.common.expression.comparison.EqualExpression;

public class ResolveExpressionVisitor implements ToExpressionVisitor {

    private Map<String, List<String>> nameMaps = new HashMap<>();
    private Map<String, Integer> qualifierNameMapIndex = new HashMap<>();
    private List<Attribute> input;
    private boolean isGroupBy = false;

    public ResolveExpressionVisitor(List<Attribute> input, boolean isGroupBy) {
        this.input = input;
        for (int i = 0; i < input.size(); i++) {
            Attribute attribute = input.get(i);
            String name = attribute.getFieldNames().get(0);
            List<String> names = nameMaps.computeIfAbsent(name, k -> new ArrayList<>(input.size()));
            String nameWithQualifier = attribute.getNameWithQualifier();
            names.add(nameWithQualifier);
            if (qualifierNameMapIndex.get(nameWithQualifier) != null) {
                throw new CarbonSqlException("Found multiple field " + nameWithQualifier);
            }
            qualifierNameMapIndex.put(nameWithQualifier, i);
        }
        this.isGroupBy = isGroupBy;
    }

    public ResolveExpressionVisitor(List<Attribute> input) {
        this(input, false);
    }

    @Override
    public Expression visit(EqualExpression expression) {
        List<Expression> operands = expression.getOperands();
        return new EqualExpression(operands.get(0), operands.get(1));
    }

    @Override
    public Expression visit(FieldExpression expression) {
        String name = expression.getName();
        if (name == null) {
            return expression;
        }
        List<String> findNames = nameMaps.get(name);
        if (findNames == null) {
            if (isGroupBy) {
                throw new CarbonSqlException("Field " + name + " not in group by");
            } else {
                throw new CarbonSqlException("Field " + name + " not found");
            }
        }
        if (expression.getQualifier().isEmpty()) {
            if (findNames.size() == 1) {
                int index = qualifierNameMapIndex.get(findNames.get(0));
                return new FieldExpression(input.get(index), index);
            } else {
                String fields = findNames.stream().collect(Collectors.joining(","));
                throw new CarbonSqlException("Field " + name + " is ambiguous, it should be " + fields);
            }
        } else {
            String nameWithQualifier = expression.getNameWithQualifier();
            List<String> filteredNames = findNames.stream()
                    .filter(x -> x.equals(nameWithQualifier))
                    .collect(Collectors.toList());
            if (filteredNames.isEmpty()) {
                String fields = findNames.stream().collect(Collectors.joining(","));
                throw new CarbonSqlException("Field " + nameWithQualifier + " not found, it may be " + fields);
            } else if (filteredNames.size() >= 2) {
                String fields = String.join(",", filteredNames);
                throw new CarbonSqlException("Field " + nameWithQualifier + " is ambiguous, it should be " + fields);
            } else {
                int index = qualifierNameMapIndex.get(filteredNames.get(0));
                return new FieldExpression(input.get(index), index);
            }
        }
    }
}
