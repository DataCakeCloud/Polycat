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

import io.polycat.common.expression.Expression;
import io.polycat.common.expression.ExpressionBase;
import io.polycat.common.expression.FieldExpression;
import io.polycat.common.expression.LiteralExpression;
import io.polycat.common.expression.comparison.EqualExpression;
import io.polycat.common.expression.comparison.GreaterThanExpression;
import io.polycat.common.expression.comparison.GreaterThanOrEqualExpression;
import io.polycat.common.expression.comparison.LessThanExpression;
import io.polycat.common.expression.comparison.LessThanOrEqualExpression;

import java.util.List;
import java.util.function.Function;

public class FieldBeforeLiteral implements ToExpressionVisitor {

    @Override
    public Expression visit(EqualExpression expression) {
        return visitExpressionBase(expression,
            descriptor -> new EqualExpression(descriptor.field, descriptor.literal));
    }

    @Override
    public Expression visit(GreaterThanExpression expression) {
        return visitExpressionBase(expression,
            descriptor -> new LessThanExpression(descriptor.field, descriptor.literal));
    }

    @Override
    public Expression visit(GreaterThanOrEqualExpression expression) {
        return visitExpressionBase(expression,
            descriptor -> new LessThanOrEqualExpression(descriptor.field, descriptor.literal));
    }

    @Override
    public Expression visit(LessThanExpression expression) {
        return visitExpressionBase(expression,
            descriptor -> new GreaterThanExpression(descriptor.field, descriptor.literal));
    }

    @Override
    public Expression visit(LessThanOrEqualExpression expression) {
        return visitExpressionBase(expression,
            descriptor -> new GreaterThanOrEqualExpression(descriptor.field, descriptor.literal));
    }

    public Expression visitExpressionBase(ExpressionBase expression, Function<Descriptor, Expression> newExpression) {
        Descriptor descriptor = Descriptor.build().addAllOperands(expression.getOperands());
        if (descriptor.isFieldFirst) {
            return expression;
        } else {
            return newExpression.apply(descriptor);
        }
    }

    private static class Descriptor {
        FieldExpression field;
        LiteralExpression literal;
        boolean isFieldFirst;

        public static Descriptor build() {
            return new Descriptor();
        }

        public Descriptor addAllOperands(List<Expression> operands) {
            isFieldFirst = operands.get(0) instanceof FieldExpression;
            if (!isFieldFirst) {
                setField(operands.get(1));
                setLiteral(operands.get(0));
            }
            return this;
        }

        private void setField(Expression expression) {
            this.field = (FieldExpression) expression;
        }

        private void setLiteral(Expression expression) {
            this.literal = (LiteralExpression) expression;
        }
    }
}
