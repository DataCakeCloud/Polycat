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
package io.polycat.common.expression.comparison;

import java.util.List;

import com.google.common.collect.Lists;
import io.polycat.common.expression.rule.ToExpressionVisitor;

import io.polycat.catalog.common.exception.CarbonSqlException;
import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.model.record.StringWritable;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.common.expression.Attribute;
import io.polycat.common.expression.Expression;
import io.polycat.common.expression.ExpressionBase;

public class LikeExpression extends ExpressionBase {

    public LikeExpression(Expression left, Expression right) {
        super(Lists.newArrayList(left, right), left.getDataType());
    }

    @Override
    public boolean evaluateToBool(Record record) {
        Field left = operands.get(0).evaluate(record);
        Field right = operands.get(1).evaluate(record);
        dataType = operands.get(0).getDataType();
        if (dataType != DataTypes.STRING
                || !(left instanceof StringWritable)
                || !(right instanceof StringWritable)) {
            throw new CarbonSqlException("likeExpression need evaluate by string, type=" + dataType);
        }      
        
        // todo:待实现like的其他语义
        // 大小写不敏感
        String likeRex = right.getString().toLowerCase();
        likeRex = likeRex.replaceAll("%", ".*");
        
        return left.getString().toLowerCase().matches(likeRex);
    }

    @Override
    public String simpleString(List<Attribute> inputAttributes) {
        return operands.get(0).simpleString(inputAttributes) + " like " + operands.get(1).simpleString(inputAttributes);
    }

    @Override
    public Expression accept(ToExpressionVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    protected Expression copyWithNewChildren(List<Expression> children) {
        return new LikeExpression(children.get(0), children.get(1));
    }
}
