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

import com.google.common.collect.Lists;

import io.polycat.common.VectorBatch;
import io.polycat.common.expression.rule.ToExpressionVisitor;
import io.polycat.common.expression.rule.ToRexCallVisitor;
import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.rex.RexNode;
import io.polycat.catalog.common.types.DataType;
import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.Record;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import lombok.Getter;

public class FieldExpression extends NamedExpression {

    @Getter
    private int fieldIndex = -1;

    public FieldExpression(String name, List<String> qualifier) {
        super(Collections.emptyList(), null, name, qualifier);
    }

    public FieldExpression(String name) {
        this(name, Collections.emptyList());
    }

    public FieldExpression(int fieldIndex, DataType dataType) {
        super(Collections.emptyList(), dataType, null, Collections.emptyList());
        this.fieldIndex = fieldIndex;
    }

    public FieldExpression(Attribute attribute, int fieldIndex) {
        super(Collections.emptyList(), attribute.getDataType(), attribute.getName(), attribute.getQualifier());
        this.fieldIndex = fieldIndex;
    }

    public FieldExpression(String name, int fieldIndex, DataType dataType) {
        this(fieldIndex, dataType);
        this.name = name;
    }

    @Override
    public List<Integer> getFieldIndexes() {
        return Lists.newArrayList(fieldIndex);
    }

    @Override
    public Field evaluate(Record record) {
        return record.getField(fieldIndex);
    }

    @Override
    public boolean evaluateToBool(Record record) {
        return record.getBoolean(fieldIndex);
    }

    @Override
    public FieldVector evaluate(VectorBatch batch) {
        return batch.getFieldVector(fieldIndex);
    }

    @Override
    public String simpleString(List<Attribute> inputAttributes) {
        if (name != null) {
            return name;
        } else {
            return "$" + fieldIndex;
        }
    }

    /**
     *
     */
    @Override
    public String templateString() {
        return "$" + fieldIndex;
    }

    /**
     * hash code of template sql
     * the query will use it to find the same template sql
     */
    @Override
    public int templateHash() {
        return fieldIndex;
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
        FieldExpression that = (FieldExpression) o;
        return fieldIndex == that.fieldIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fieldIndex);
    }

    @Override
    public RexNode accept(ToRexCallVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    public Expression accept(ToExpressionVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    protected Expression copyWithNewChildren(List<Expression> children) {
        return this;
    }

    public FieldExpression copyWithNewIndex(int fieldIndex) {
        return new FieldExpression(fieldIndex, dataType);
    }

    @Override
    public String toString() {
        return "FieldExpression{" + "fieldIndex=" + fieldIndex + ", name='" + name + '\'' + '}';
    }
}
