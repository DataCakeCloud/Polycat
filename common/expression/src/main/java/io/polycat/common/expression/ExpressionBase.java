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

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexNode;
import io.polycat.catalog.common.types.DataType;

public abstract class ExpressionBase implements Expression, Serializable {

    // 运算数
    protected List<Expression> operands;

    protected DataType dataType;

    public ExpressionBase(List<Expression> operands, DataType dataType) {
        this.operands = operands;
        this.dataType = dataType;
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public List<Integer> getFieldIndexes() {
        List<Integer> output = new LinkedList<>();
        operands.forEach(it -> output.addAll(it.getFieldIndexes()));
        return output;
    }

    public List<Expression> getOperands() {
        return operands;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExpressionBase that = (ExpressionBase) o;
        return Objects.equals(operands, that.operands) && dataType == that.dataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(operands, dataType);
    }

    @Override
    public Expression transformUp(Function<Expression, Expression> rule) {
        if (operands.isEmpty()) {
            return rule.apply(this);
        }
        List<Expression> newChildren = operands.stream()
            .map(expr -> expr.transformUp(rule)).collect(Collectors.toList());
        assert (newChildren.size() == operands.size());
        if (newChildren.hashCode() == operands.hashCode()) {
            return rule.apply(this);
        } else {
            return rule.apply(copyWithNewChildren(newChildren));
        }
    }

    @Override
    public RexNode trans2RexNode(Function<Expression, RexNode> rule) {
        return rule.apply(this);
    }

    @Override
    public List<Expression> collect(Function<Expression, Boolean> rule) {
        List<Expression> expressions = new LinkedList<>();
        if (rule.apply(this)) {
            expressions.add(this);
        }
        operands.forEach(expr -> expressions.addAll(expr.collect(rule)));
        return expressions;
    }

    protected abstract Expression copyWithNewChildren(List<Expression> children);
}
