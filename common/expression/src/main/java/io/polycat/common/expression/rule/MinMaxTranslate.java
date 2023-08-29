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
import java.util.List;

import io.polycat.common.expression.Expression;
import io.polycat.common.expression.FieldExpression;
import io.polycat.common.expression.LiteralExpression;
import io.polycat.common.expression.bool.AndExpression;
import io.polycat.common.expression.comparison.EqualExpression;
import io.polycat.common.expression.comparison.GreaterThanExpression;
import io.polycat.common.expression.comparison.GreaterThanOrEqualExpression;
import io.polycat.common.expression.comparison.LessThanExpression;
import io.polycat.common.expression.comparison.LessThanOrEqualExpression;

/**
 * translate expression on original field to new expression on min/max field
 */
public class MinMaxTranslate implements ToExpressionVisitor {

    @Override
    public Expression visit(EqualExpression expression) {
        Descriptor descriptor  = Descriptor.build().addAllOperands(expression.getOperands());
        List<Expression> minMaxOperands = new ArrayList<>(2);
        minMaxOperands.add(new LessThanOrEqualExpression(descriptor.minField, descriptor.literal));
        minMaxOperands.add(new GreaterThanOrEqualExpression(descriptor.maxField, descriptor.literal));
        return new AndExpression(minMaxOperands, expression.getDataType());
    }

    @Override
    public Expression visit(GreaterThanExpression expression) {
        Descriptor descriptor  = Descriptor.build().addAllOperands(expression.getOperands());
        return new GreaterThanExpression(descriptor.maxField, descriptor.literal);
    }

    @Override
    public Expression visit(GreaterThanOrEqualExpression expression) {
        Descriptor descriptor  = Descriptor.build().addAllOperands(expression.getOperands());
        return new GreaterThanOrEqualExpression(descriptor.maxField, descriptor.literal);
    }

    @Override
    public Expression visit(LessThanExpression expression) {
        Descriptor descriptor  = Descriptor.build().addAllOperands(expression.getOperands());
        return new LessThanExpression(descriptor.minField, descriptor.literal);
    }

    @Override
    public Expression visit(LessThanOrEqualExpression expression) {
        Descriptor descriptor  = Descriptor.build().addAllOperands(expression.getOperands());
        return new LessThanOrEqualExpression(descriptor.minField, descriptor.literal);
    }

    private static class Descriptor {
        FieldExpression minField;
        FieldExpression maxField;
        LiteralExpression literal;

        public static Descriptor build() {
            return new Descriptor();
        }

        public Descriptor addAllOperands(List<Expression> operands) {
            setField(operands.get(0));
            setLiteral(operands.get(1));
            return this;
        }

        private void setField(Expression expression) {
            FieldExpression field = (FieldExpression) expression;
            int fieldIndex = field.getFieldIndexes().get(0);
            int newFieldIndex = fieldIndex * 2;
            this.minField = field.copyWithNewIndex(newFieldIndex);
            this.maxField = field.copyWithNewIndex(newFieldIndex + 1);
        }

        private void setLiteral(Expression expression) {
            this.literal = (LiteralExpression) expression;
        }
    }
}
