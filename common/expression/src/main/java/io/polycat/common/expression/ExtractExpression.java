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
package io.polycat.common.expression;

import java.text.SimpleDateFormat;
import java.util.List;

import com.google.common.collect.Lists;
import io.polycat.common.expression.rule.ToExpressionVisitor;

import io.polycat.catalog.common.exception.CarbonSqlException;
import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.LongWritable;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;

public class ExtractExpression extends ExpressionBase {

    public ExtractExpression(Expression left, Expression right) {
        super(Lists.newArrayList(left, right), left.getDataType());
    }

    @Override
    public Field evaluate(Record record) {
        Field left = operands.get(0).evaluate(record);
        Field right = operands.get(1).evaluate(record);
        DataType leftDataType = operands.get(0).getDataType();
        DataType rightDataType = operands.get(1).getDataType();

        if (leftDataType != DataTypes.STRING || (rightDataType != DataTypes.DATE
                && rightDataType != DataTypes.TIMESTAMP)) {
            throw new CarbonSqlException(
                    "ExtractExpression dataType error, leftDataType=" + leftDataType + ", rightDataType="
                            + rightDataType);
        }

        // todo:待实现extract的其他语义
        // 大小写不敏感
        String unit = left.getString().toLowerCase();
        String[] strDate;
        String[] strTime;
        if (rightDataType == DataTypes.DATE) {
            strDate = new SimpleDateFormat("yyyy-MM-dd").format(right.getDate()).split("-");
            strTime = new String[] {"0", "0", "0"};
        } else {
            strDate = new SimpleDateFormat("yyyy-MM-dd").format(right.getTimestamp()).split("-");
            strTime = new SimpleDateFormat("HH:mm:ss").format(right.getTimestamp()).split(":");
        }

        switch (unit) {
            case "year":
                return new LongWritable(Long.parseLong(strDate[0]));
            case "month":
                return new LongWritable(Long.parseLong(strDate[1]));
            case "day":
                return new LongWritable(Long.parseLong(strDate[2]));
            case "hour":
                return new LongWritable(Long.parseLong(strTime[0]));
            case "minute":
                return new LongWritable(Long.parseLong(strTime[1]));
            case "second":
                return new LongWritable(Long.parseLong(strTime[2]));
            default:
                throw new CarbonSqlException("ExtractExpression does not support the operation, unit=" + unit);
        }
    }

    @Override
    public String simpleString(List<Attribute> inputAttributes) {
        return "extract(" + operands.get(0).simpleString(inputAttributes) + " from " + operands.get(1)
                .simpleString(inputAttributes) + ")";
    }

    @Override
    public Expression accept(ToExpressionVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    protected Expression copyWithNewChildren(List<Expression> children) {
        return new ExtractExpression(children.get(0), children.get(1));
    }
}
