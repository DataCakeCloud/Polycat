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

import java.util.List;

import io.polycat.catalog.common.types.DataType;
import io.polycat.common.expression.rule.ToExpressionVisitor;
import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.Record;

public class CastExpression extends ExpressionBase {

    /**
     * 构造函数
     * @param operands 操作数
     * @param dataType 要转换的目标类型
     */
    public CastExpression(List<Expression> operands, DataType dataType) {
        super(operands, dataType);
        if (operands.size() != 1) {
            throw new UnsupportedOperationException("cast expression size " + operands.size());
        }
    }

    @Override
    public Expression accept(ToExpressionVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    protected Expression copyWithNewChildren(List<Expression> children) {
        return new CastExpression(children, dataType);
    }

    @Override
    public Field evaluate(Record record) {
        Field field = operands.get(0).evaluate(record);
        // 转换为目标数据类型
        if (field.getType().equals(dataType)) {
            return field;
        } else {
            // TODO: 实现类型转换
            throw new UnsupportedOperationException(String.format("cast from %s to %s", field.getType(), dataType));
        }
    }

    @Override
    public String simpleString(List<Attribute> inputAttributes) {
        return null;
    }
}
