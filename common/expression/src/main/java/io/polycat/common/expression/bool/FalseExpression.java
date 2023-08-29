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

import java.util.Collections;
import java.util.List;

import io.polycat.catalog.common.types.DataTypes;
import io.polycat.common.expression.Attribute;
import io.polycat.common.expression.Expression;
import io.polycat.common.expression.ExpressionBase;
import io.polycat.common.expression.rule.ToExpressionVisitor;

public class FalseExpression extends ExpressionBase {

    static class FalseExpressionHandler {
        private final static FalseExpression INSTANCE = new FalseExpression();
    }

    private FalseExpression() {
        super(Collections.emptyList(), DataTypes.BOOLEAN);
    }

    @Override
    public String simpleString(List<Attribute> inputAttributes) {
        return "1<>1";
    }

    @Override
    public Expression accept(ToExpressionVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    protected Expression copyWithNewChildren(List<Expression> children) {
        return this;
    }

    public static FalseExpression getInstance() {
        return FalseExpressionHandler.INSTANCE;
    }
}
