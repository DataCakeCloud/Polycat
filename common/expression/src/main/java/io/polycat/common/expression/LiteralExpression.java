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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.common.expression.rule.ToExpressionVisitor;
import io.polycat.common.expression.rule.ToRexCallVisitor;
import org.apache.calcite.rex.RexNode;

public class LiteralExpression extends NamedExpression {

    private final Field value;

    /**
     * 数值型Literal，value应该传入BigDecimal对象。
     * 字符串Literal，value应该传入String对象。
     * Boolean Literal, value应该传入Boolean对象。
     */
    public LiteralExpression(Field field, DataType dataType) {
        super(Collections.emptyList(), dataType, field.getString(), Collections.emptyList());
        value = field;
    }

    public LiteralExpression(Field field) {
        super(Collections.emptyList(), field.getType(), field.getString(), Collections.emptyList());
        this.value = field;
    }

    public Field getValue() {
        return value;
    }

    @Override
    public Field evaluate(Record record) {
        return value;
    }

    @Override
    public boolean evaluateToBool(Record record) {
        if (this.value.getType() == DataTypes.BOOLEAN) {
            return this.value.getBoolean();
        }

        throw new IllegalArgumentException(
            "LiteralExpression value is not boolean ,can't evaluete to bool:" + this.value.getType());
    }

    @Override
    public String simpleString(List<Attribute> inputAttributes) {
        return value.toString();
    }

    @Override
    public String templateString() {
        return "?";
    }

    @Override
    public int templateHash() {
        return "?".hashCode();
    }

    @Override
    public RexNode accept(ToRexCallVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return "LiteralExpression{" + "value=" + value + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LiteralExpression that = (LiteralExpression) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }

    @Override
    public Expression accept(ToExpressionVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    protected Expression copyWithNewChildren(List<Expression> children) {
        return this;
    }
}
