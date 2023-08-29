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
import io.polycat.common.expression.FieldExpression;
import io.polycat.common.expression.LiteralExpression;
import io.polycat.common.expression.UnSupportExpression;
import io.polycat.common.expression.arithmetic.DivideExpression;
import io.polycat.common.expression.arithmetic.MinusExpression;
import io.polycat.common.expression.arithmetic.ModExpression;
import io.polycat.common.expression.arithmetic.PlusExpression;
import io.polycat.common.expression.arithmetic.TimesExpression;
import io.polycat.common.expression.conditional.CaseExpression;
import io.polycat.common.expression.function.SubstringExpression;
import io.polycat.common.expression.CastExpression;
import io.polycat.common.expression.ExtractExpression;
import io.polycat.common.expression.NullExpression;
import io.polycat.common.expression.AsteriskExpression;
import io.polycat.common.expression.bool.AndExpression;
import io.polycat.common.expression.bool.FalseExpression;
import io.polycat.common.expression.bool.IsNullExpression;
import io.polycat.common.expression.bool.NotExpression;
import io.polycat.common.expression.bool.OrExpression;
import io.polycat.common.expression.bool.TrueExpression;
import io.polycat.common.expression.comparison.EqualExpression;
import io.polycat.common.expression.comparison.GreaterThanExpression;
import io.polycat.common.expression.comparison.GreaterThanOrEqualExpression;
import io.polycat.common.expression.comparison.LessThanExpression;
import io.polycat.common.expression.comparison.LessThanOrEqualExpression;
import io.polycat.common.expression.comparison.LikeExpression;
import io.polycat.common.expression.comparison.NotEqualExpression;

public interface ToExpressionVisitor extends ExpressionVisitor {

    // arithmetic
    default Expression visit(DivideExpression expression) {
        return expression;
    }

    default Expression visit(MinusExpression expression) {
        return expression;
    }

    default Expression visit(ModExpression expression) {
        return expression;
    }

    default Expression visit(PlusExpression expression) {
        return expression;
    }

    default Expression visit(TimesExpression expression) {
        return expression;
    }

    // bool
    default Expression visit(AndExpression expression) {
        return expression;
    }

    default Expression visit(IsNullExpression expression) {
        return expression;
    }

    default Expression visit(NotExpression expression) {
        return expression;
    }

    default Expression visit(OrExpression expression) {
        return expression;
    }

    default Expression visit(TrueExpression expression) {
        return expression;
    }

    default Expression visit(FalseExpression expression) {
        return expression;
    }

    // comparison
    default Expression visit(EqualExpression expression) {
        return expression;
    }

    default Expression visit(GreaterThanExpression expression) {
        return expression;
    }

    default Expression visit(GreaterThanOrEqualExpression expression) {
        return expression;
    }

    default Expression visit(LessThanExpression expression) {
        return expression;
    }

    default Expression visit(LessThanOrEqualExpression expression) {
        return expression;
    }

    default Expression visit(LikeExpression expression) {
        return expression;
    }

    default Expression visit(NotEqualExpression expression) {
        return expression;
    }

    // Conditional
    default Expression visit(CaseExpression expression) {
        return expression;
    }

    // function
    default Expression visit(SubstringExpression expression) {
        return expression;
    }

    // others
    default Expression visit(CastExpression expression) {
        return expression;
    }

    default Expression visit(ExtractExpression expression) {
        return expression;
    }

    default Expression visit(FieldExpression expression) {
        return expression;
    }

    default Expression visit(LiteralExpression expression) {
        return expression;
    }

    default Expression visit(NullExpression expression) {
        return expression;
    }

    default Expression visit(UnSupportExpression expression) {
        return expression;
    }

    default Expression visit(AsteriskExpression expression) {
        return expression;
    }
}
