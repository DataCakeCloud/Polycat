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

import io.polycat.common.expression.Expression;
import io.polycat.common.expression.ExpressionBase;
import io.polycat.common.expression.LiteralExpression;
import io.polycat.common.expression.NullExpression;
import io.polycat.common.expression.bool.FalseExpression;
import io.polycat.common.expression.bool.TrueExpression;
import io.polycat.common.expression.comparison.EqualExpression;
import io.polycat.common.expression.comparison.GreaterThanExpression;
import io.polycat.common.expression.comparison.GreaterThanOrEqualExpression;
import io.polycat.common.expression.comparison.LessThanExpression;
import io.polycat.common.expression.comparison.LessThanOrEqualExpression;
import io.polycat.catalog.common.model.record.Record;

public class PreCompute implements ToExpressionVisitor {

    @Override
    public Expression visit(EqualExpression expression) {
        return preCompute(expression);
    }

    @Override
    public Expression visit(GreaterThanExpression expression) {
        return preCompute(expression);
    }

    @Override
    public Expression visit(GreaterThanOrEqualExpression expression) {
        return preCompute(expression);
    }

    @Override
    public Expression visit(LessThanExpression expression) {
        return preCompute(expression);
    }

    @Override
    public Expression visit(LessThanOrEqualExpression expression) {
        return preCompute(expression);
    }

    private ExpressionBase preCompute(ExpressionBase expression) {
        if (isSupported(expression.getOperands())) {
            if (expression.evaluateToBool((Record) null)) {
                return TrueExpression.getInstance();
            } else {
                return FalseExpression.getInstance();
            }
        }
        return expression;
    }

    private boolean isSupported(List<Expression> operands) {
        return operands.stream().allMatch(expr -> expr instanceof LiteralExpression || expr instanceof NullExpression);
    }
}
