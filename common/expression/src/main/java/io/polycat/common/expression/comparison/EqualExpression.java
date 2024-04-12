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
import java.util.Objects;

import com.google.common.collect.Lists;
import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.common.expression.Attribute;
import io.polycat.common.expression.Expression;
import io.polycat.common.expression.ExpressionBase;
import io.polycat.common.expression.rule.ToExpressionVisitor;
import io.polycat.common.expression.rule.ToRexCallVisitor;
import org.apache.calcite.rex.RexNode;

public class EqualExpression extends ExpressionBase {

    public EqualExpression(Expression left, Expression right) {
        super(Lists.newArrayList(left, right), left.getDataType());
    }

    @Override
    public boolean evaluateToBool(Record record) {
        Field left = operands.get(0).evaluate(record);
        Field right = operands.get(1).evaluate(record);
        dataType = operands.get(0).getDataType();

        if (left.getType() == DataTypes.NULL || right.getType() == DataTypes.NULL) {
            return false;
        }

        // 右边的类型可能和左边不相等，需要转换
        if (left.getType() != right.getType()) {
            dataType = DataTypes.getHighType(left.getType(), right.getType());
        }
        
        if (dataType == DataTypes.STRING) {
            return left.getString().equals(right.getString());
        } else if (DataTypes.isIntegerType(dataType)) {
            return left.getLong() == right.getLong();
        } else if (DataTypes.isFloatingPointType(dataType)) {
            return left.getDecimal().compareTo(right.getDecimal()) == 0;
        } else if (dataType == DataTypes.DATE) {
            return left.equals(right);
        } else if (dataType == DataTypes.TIMESTAMP) {
            return left.equals(right);
        } else {
            throw new UnsupportedOperationException(dataType.toString());
        }
    }

    @Override
    public String simpleString(List<Attribute> inputAttributes) {
        return operands.get(0).simpleString(inputAttributes) + "=" + operands.get(1).simpleString(inputAttributes);
    }

    @Override
    public String templateString() {
        return "(" + operands.get(0).templateString() + "=" + operands.get(1).templateString() + ")";
    }

    @Override
    public int templateHash() {
        return Objects.hash("=", operands.get(0).templateHash(), operands.get(1).templateHash());
    }

    @Override
    public Expression accept(ToExpressionVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    public RexNode accept(ToRexCallVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    protected Expression copyWithNewChildren(List<Expression> children) {
        return new EqualExpression(children.get(0), children.get(1));
    }
}
