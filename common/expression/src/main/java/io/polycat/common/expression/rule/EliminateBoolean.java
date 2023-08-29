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

import io.polycat.common.expression.Expression;
import io.polycat.common.expression.bool.AndExpression;
import io.polycat.common.expression.bool.FalseExpression;
import io.polycat.common.expression.bool.OrExpression;
import io.polycat.common.expression.bool.TrueExpression;

public class EliminateBoolean implements ToExpressionVisitor {

    @Override
    public Expression visit(AndExpression expression) {
        // if exists FalseExpression, return FalseExpression
        long countFalse = expression.getOperands().stream().filter(expr -> (expr instanceof FalseExpression)).count();
        if (countFalse >= 1) {
            return FalseExpression.getInstance();
        }
        // remove TureExpression from expression
        List<Expression> expressionList = expression.getOperands().stream()
                .filter(expr -> !(expr instanceof TrueExpression)).collect(Collectors.toList());
        int size = expressionList.size();
        if (size == expression.getOperands().size()) {
            return expression;
        }
        if (size == 0) {
            return TrueExpression.getInstance();
        } else if (size == 1) {
            return expressionList.get(0);
        } else {
            return new AndExpression(expressionList, expression.getDataType());
        }
    }

    @Override
    public Expression visit(OrExpression expression) {
        // if exists TrueExpression, return TrueExpression
        long countTrue = expression.getOperands().stream().filter(expr -> expr instanceof TrueExpression).count();
        if (countTrue >= 1) {
            return TrueExpression.getInstance();
        }
        // remove TureExpression from expression
        List<Expression> expressionList = expression.getOperands().stream()
                .filter(expr -> !(expr instanceof FalseExpression)).collect(Collectors.toList());
        int size = expressionList.size();
        if (size == expression.getOperands().size()) {
            return expression;
        }
        if (size == 0) {
            return TrueExpression.getInstance();
        } else if (size == 1) {
            return expressionList.get(0);
        } else {
            return new OrExpression(expressionList, expression.getDataType());
        }
    }
}
