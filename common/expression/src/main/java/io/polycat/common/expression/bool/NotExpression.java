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

import java.util.List;

import io.polycat.common.expression.Expression;
import io.polycat.common.expression.ExpressionBase;
import io.polycat.common.expression.rule.ToExpressionVisitor;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.common.expression.Attribute;

import com.google.common.collect.Lists;

public class NotExpression extends ExpressionBase {

    public NotExpression(Expression expression) {
        super(Lists.newArrayList(expression), expression.getDataType());
    }

    @Override
    public boolean evaluateToBool(Record record) {
        return !(operands.get(0).evaluateToBool(record));
    }

    @Override
    public String simpleString(List<Attribute> inputAttributes) {
        return "NOT(" + operands.get(0).simpleString(inputAttributes) + ")";
    }

    @Override
    public Expression accept(ToExpressionVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    protected Expression copyWithNewChildren(List<Expression> children) {
        return new NotExpression(children.get(0));
    }
}
