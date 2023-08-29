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
package io.polycat.common.expression.function;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import io.polycat.catalog.common.exception.CarbonSqlException;
import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.model.record.StringWritable;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.common.expression.Attribute;
import io.polycat.common.expression.Expression;
import io.polycat.common.expression.NamedExpression;
import io.polycat.common.expression.rule.ToExpressionVisitor;

public class SubstringExpression extends NamedExpression {
    public SubstringExpression(List<Expression> expressions) {
        this(expressions, getName(expressions.get(0)), Collections.emptyList());
    }

    public SubstringExpression(List<Expression> expressions, String name, List<String> qualifier) {
        super(expressions, expressions.get(0).getDataType(), name, qualifier);
    }

    @Override
    public Field evaluate(Record record) {
        List<Field> fields = new LinkedList<>();
        List<DataType> dataTypes = new LinkedList<>();
        for (int i = 0; i < operands.size(); i++) {
            fields.add(operands.get(i).evaluate(record));
            dataTypes.add(operands.get(i).getDataType());
        }

        if (fields.size() != 3 || dataTypes.size() != 3) {
            throw new CarbonSqlException(
                    "SubstringExpression need 3 operands, operands size=" + operands.size() + ", fields size="
                            + fields.size() + ", dataTypes size=" + dataTypes.size());
        }

        if (dataTypes.get(0) != DataTypes.STRING || dataTypes.get(1) != DataTypes.INTEGER
                || dataTypes.get(2) != DataTypes.INTEGER) {
            throw new CarbonSqlException("SubstringExpression dataType error, dataTypes=" + dataTypes);
        }

        // todo:待实现substring的其他语义
        // 大小写不敏感
        String str = fields.get(0).getString().toLowerCase();
        Integer beginIndex = fields.get(1).getInteger() - 1;
        Integer endIndex = beginIndex + fields.get(2).getInteger();

        return new StringWritable(str.substring(beginIndex, endIndex));
    }

    @Override
    public String simpleString(List<Attribute> inputAttributes) {
        return "substring(" + operands.get(0).simpleString(inputAttributes) + ", " + operands.get(1)
                .simpleString(inputAttributes) + ", " + operands.get(2).simpleString(inputAttributes) + ")";
    }

    @Override
    public Expression accept(ToExpressionVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    protected Expression copyWithNewChildren(List<Expression> children) {
        return new SubstringExpression(children);
    }

    private static String getName(Expression expression) {
        return "substring(" + ((NamedExpression) expression).getName() + ")";
    }
}
