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

import io.polycat.common.expression.conditional.CaseExpression;
import io.polycat.common.expression.Expression;
import io.polycat.common.expression.ExpressionBase;
import io.polycat.common.expression.FieldExpression;
import io.polycat.common.expression.LiteralExpression;
import io.polycat.common.expression.UnSupportExpression;
import io.polycat.common.expression.bool.IsNullExpression;
import io.polycat.common.expression.bool.NotExpression;
import io.polycat.common.expression.bool.TrueExpression;
import io.polycat.common.expression.comparison.EqualExpression;
import io.polycat.common.expression.comparison.GreaterThanExpression;
import io.polycat.common.expression.comparison.GreaterThanOrEqualExpression;
import io.polycat.common.expression.comparison.LessThanExpression;
import io.polycat.common.expression.comparison.LessThanOrEqualExpression;
import io.polycat.common.expression.comparison.LikeExpression;
import io.polycat.common.expression.comparison.NotEqualExpression;

/**
 * pruning expression
 * 1. only keep few supported expression
 * 2. using TrueExpression instead of unsupported expression
 */
public class MinMaxPruning implements ToExpressionVisitor {

    @Override
    public Expression visit(EqualExpression expression) {
        return visitExpressionBase(expression);
    }

    @Override
    public Expression visit(NotEqualExpression expression) {
        return TrueExpression.getInstance();
    }

    @Override
    public Expression visit(GreaterThanExpression expression) {
        return visitExpressionBase(expression);
    }

    @Override
    public Expression visit(GreaterThanOrEqualExpression expression) {
        return visitExpressionBase(expression);
    }

    @Override
    public Expression visit(LessThanExpression expression) {
        return visitExpressionBase(expression);
    }

    @Override
    public Expression visit(LessThanOrEqualExpression expression) {
        return visitExpressionBase(expression);
    }

    @Override
    public Expression visit(IsNullExpression expression) {
        return TrueExpression.getInstance();
    }

    @Override
    public Expression visit(NotExpression expression) {
        return TrueExpression.getInstance();
    }

    @Override
    public Expression visit(LikeExpression expression) {
        return TrueExpression.getInstance();
    }

    @Override
    public Expression visit(CaseExpression expression) {
        return TrueExpression.getInstance();
    }

    @Override
    public Expression visit(UnSupportExpression expression) {
        return TrueExpression.getInstance();
    }

    private ExpressionBase visitExpressionBase(ExpressionBase expression) {
        List<Expression> operands = expression.getOperands();
        if (isSupported(operands.get(0), operands.get(1))) {
            return expression;
        } else {
            return TrueExpression.getInstance();
        }
    }

    private boolean isSupported(Expression left, Expression right) {
        return (left instanceof FieldExpression && right instanceof LiteralExpression) ||
                (right instanceof FieldExpression && left instanceof LiteralExpression);
    }
}
