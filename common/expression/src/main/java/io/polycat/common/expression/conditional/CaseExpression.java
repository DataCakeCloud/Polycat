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
package io.polycat.common.expression.conditional;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import io.polycat.common.expression.Expression;
import io.polycat.common.expression.ExpressionBase;
import io.polycat.common.expression.rule.ToExpressionVisitor;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.common.expression.Attribute;

public class CaseExpression extends ExpressionBase {

    public CaseExpression(List<Expression> operands) {
        super(null, null);
        this.operands = operands;
        this.dataType = operands.get(operands.size() - 1).getDataType();
    }

    public CaseExpression(List<Expression> operands, DataType dataType) {
        super(operands, dataType);
    }

    @Override
    public Field evaluate(Record record) {
        // operands里存的是N对When-Then表达式，加一个Else表达式
        int numOfWhen = (operands.size() - 1) / 2;
        for (int i = 0; i < numOfWhen; i++) {
            if (operands.get(2 * i).evaluateToBool(record)) {
                return operands.get(2 * i + 1).evaluate(record);
            }
        }
        return operands.get(operands.size() - 1).evaluate(record);
    }

    // 只输出then里的结果
    @Override
    public List<Integer> getFieldIndexes() {
        Set<Integer> output = new LinkedHashSet<>();
        int numOfWhen = (operands.size() - 1) / 2;
        for (int i = 0; i < numOfWhen; i++) {
            output.addAll(operands.get(2 * i + 1).getFieldIndexes());
        }
        output.addAll(operands.get(operands.size() - 1).getFieldIndexes());

        return new ArrayList<>(output);
    }

    @Override
    public Expression accept(ToExpressionVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    protected Expression copyWithNewChildren(List<Expression> children) {
        return new CaseExpression(children, dataType);
    }

    // 获取条件表达式字段
    public List<Integer> getFilterFieldIndexes() {
        Set<Integer> output = new LinkedHashSet<>();
        int numOfWhen = (operands.size() - 1) / 2;
        for (int i = 0; i < numOfWhen; i++) {
            output.addAll(operands.get(2 * i).getFieldIndexes());
        }
        return new ArrayList<>(output);
    }

    @Override
    public String simpleString(List<Attribute> inputAttributes) {
        return String.join("CASE WHEN ",
                operands.stream().map(expression -> expression.simpleString(inputAttributes)).toArray(String[]::new));
    }
}
