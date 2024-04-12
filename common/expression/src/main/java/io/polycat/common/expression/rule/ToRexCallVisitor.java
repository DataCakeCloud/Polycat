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

import io.polycat.common.expression.arithmetic.DivideExpression;
import io.polycat.common.expression.arithmetic.MinusExpression;
import io.polycat.common.expression.bool.AndExpression;
import io.polycat.common.expression.bool.FalseExpression;
import io.polycat.common.expression.bool.IsNullExpression;
import io.polycat.common.expression.bool.NotExpression;
import io.polycat.common.expression.bool.OrExpression;
import io.polycat.common.expression.bool.TrueExpression;
import io.polycat.common.expression.function.SubstringExpression;
import org.apache.calcite.rex.RexNode;
import io.polycat.common.expression.FieldExpression;
import io.polycat.common.expression.LiteralExpression;
import io.polycat.common.expression.UnSupportExpression;
import io.polycat.common.expression.arithmetic.PlusExpression;
import io.polycat.common.expression.arithmetic.TimesExpression;
import io.polycat.common.expression.conditional.CaseExpression;
import io.polycat.common.expression.AsteriskExpression;
import io.polycat.common.expression.CastExpression;
import io.polycat.common.expression.ExtractExpression;
import io.polycat.common.expression.NullExpression;
import io.polycat.common.expression.comparison.EqualExpression;
import io.polycat.common.expression.comparison.GreaterThanExpression;
import io.polycat.common.expression.comparison.GreaterThanOrEqualExpression;
import io.polycat.common.expression.comparison.LessThanExpression;
import io.polycat.common.expression.comparison.LessThanOrEqualExpression;
import io.polycat.common.expression.comparison.LikeExpression;
import io.polycat.common.expression.comparison.NotEqualExpression;

public interface ToRexCallVisitor extends ExpressionVisitor {
    // arithmetic
    default RexNode visit(DivideExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(MinusExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(PlusExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(TimesExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    // bool
    default RexNode visit(AndExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(IsNullExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(NotExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(OrExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(TrueExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(FalseExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    // comparison
    default RexNode visit(EqualExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(GreaterThanExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(GreaterThanOrEqualExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(LessThanExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(LessThanOrEqualExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(LikeExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(NotEqualExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    // Conditional
    default RexNode visit(CaseExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    // function
    default RexNode visit(SubstringExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    // others
    default RexNode visit(CastExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(ExtractExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(FieldExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(LiteralExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(NullExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(UnSupportExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode visit(AsteriskExpression expression) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }
}
