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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.polycat.common.expression.Expression;
import io.polycat.common.expression.bool.AndExpression;
import io.polycat.common.expression.bool.OrExpression;

public class CombineBinary implements ToExpressionVisitor {

    @Override
    public Expression visit(AndExpression expression) {
        long count = expression.getOperands().stream().filter(operand -> operand instanceof AndExpression).count();
        if (count == 0) {
            return expression;
        }

        List<Expression> expressionList = expression.getOperands().stream().flatMap(operand -> {
            if (operand instanceof AndExpression) {
                return ((AndExpression) operand).getOperands().stream();
            } else {
                return Stream.of(operand);
            }
        }).collect(Collectors.toList());

        return new AndExpression(expressionList, expression.getDataType());
    }



    @Override
    public Expression visit(OrExpression expression) {
        long count = expression.getOperands().stream().filter(operand -> operand instanceof OrExpression).count();
        if (count == 0) {
            return expression;
        }

        List<Expression> expressionList = expression.getOperands().stream().flatMap(operand -> {
            if (operand instanceof OrExpression) {
                return ((OrExpression) operand).getOperands().stream();
            } else {
                return Stream.of(operand);
            }
        }).collect(Collectors.toList());

        return new OrExpression(expressionList, expression.getDataType());
    }
}
