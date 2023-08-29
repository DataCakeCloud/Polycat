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
package io.polycat.common.expression.bool;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexNode;
import io.polycat.catalog.common.types.DataType;
import io.polycat.common.expression.Expression;
import io.polycat.common.expression.ExpressionBase;
import io.polycat.common.expression.rule.ToExpressionVisitor;
import io.polycat.common.expression.rule.ToRexCallVisitor;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.common.expression.Attribute;

public class AndExpression extends ExpressionBase {

    public AndExpression(List<Expression> expressionList, DataType dataType) {
        super(expressionList, dataType);
        if (expressionList.isEmpty()) {
            throw new IllegalArgumentException("expressionList should not be empty");
        }
    }

    @Override
    public Expression accept(ToExpressionVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    public RexNode accept(ToRexCallVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    protected Expression copyWithNewChildren(List<Expression> newChildren) {
        return new AndExpression(newChildren, dataType);
    }

    @Override
    public boolean evaluateToBool(Record record) {
        boolean result = true;
        for (Expression expression : operands) {
            result = result && expression.evaluateToBool(record);
        }
        return result;
    }

    @Override
    public String simpleString(List<Attribute> inputAttributes) {
        return operands.stream()
                .map(expression -> expression.simpleString(inputAttributes))
                .collect(Collectors.joining(" AND "));
    }

    @Override
    public String templateString() {
        return operands.stream()
                .map(Expression::templateString)
                .collect(Collectors.joining("(", " AND ", ")"));
    }

    @Override
    public int templateHash() {
        Object[] operandHash = operands.stream()
                .map(Expression::templateHash)
                .toArray();
        Object[] objects = new Object[operandHash.length + 1];
        System.arraycopy(operandHash, 0, objects, 1, operandHash.length);
        objects[0] = "AND";
        return Objects.hash(objects);
    }
}
