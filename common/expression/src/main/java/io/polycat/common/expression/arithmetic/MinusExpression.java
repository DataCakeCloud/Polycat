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
package io.polycat.common.expression.arithmetic;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import io.polycat.catalog.common.model.record.DateWritable;
import io.polycat.catalog.common.model.record.DecimalWritable;
import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.LongWritable;
import io.polycat.catalog.common.model.record.NullWritable;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.model.record.TimestampWritable;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.common.expression.Attribute;
import io.polycat.common.expression.Expression;
import io.polycat.common.expression.ExpressionUtil;
import io.polycat.common.expression.NamedExpression;
import io.polycat.common.expression.interval.IntervalUtils;
import io.polycat.common.expression.interval.TimeInterval;
import io.polycat.common.expression.rule.ToExpressionVisitor;

public class MinusExpression extends NamedExpression {

    public MinusExpression(Expression left, Expression right) {
        this(left, right, ExpressionUtil.name(left, right, " - "), Collections.emptyList());
    }

    public MinusExpression(Expression left, Expression right, String name, List<String> qualifier) {
        super(Lists.newArrayList(left, right), left.getDataType(), name, qualifier);
    }

    @Override
    public Field evaluate(Record record) {
        Field left = operands.get(0).evaluate(record);
        Field right = operands.get(1).evaluate(record);
        if (left.getType() == DataTypes.NULL || right.getType() == DataTypes.NULL) {
            return NullWritable.getInstance();
        }

        // 右边的类型可能和左边不相等，需要转换
        if (left.getType() != right.getType()) {
            dataType = DataTypes.getHighType(left.getType(), right.getType());
        }

        if (DataTypes.isIntegerType(dataType)) {
            return new LongWritable(left.getLong() - right.getLong());
        } else if (DataTypes.isFloatingPointType(dataType)) {
            return new DecimalWritable(left.getDecimal().subtract(right.getDecimal()));
        } else if (DataTypes.DATE == dataType) {
            TimeInterval interval = (TimeInterval) right.getObject();
            return new DateWritable(IntervalUtils.minusDate(left.getDate(), interval));
        } else if (DataTypes.TIMESTAMP == dataType) {
            TimeInterval interval = (TimeInterval) right.getObject();
            return new TimestampWritable(IntervalUtils.minusTimestamp(left.getTimestamp(), interval));
        } else {
            throw new UnsupportedOperationException(dataType.toString());
        }
    }

    @Override
    public String simpleString(List<Attribute> inputAttributes) {
        return operands.get(0).simpleString(inputAttributes) + " - " + operands.get(1).simpleString(inputAttributes);
    }

    @Override
    public Expression accept(ToExpressionVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    protected Expression copyWithNewChildren(List<Expression> children) {
        return new MinusExpression(children.get(0), children.get(1));
    }
}
