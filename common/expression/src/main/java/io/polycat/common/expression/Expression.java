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
import java.util.List;
import java.util.function.Function;

import org.apache.calcite.rex.RexNode;
import io.polycat.catalog.common.types.DataType;
import io.polycat.common.expression.rule.ToExpressionVisitor;
import io.polycat.catalog.common.model.record.Field;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.common.VectorBatch;

import org.apache.arrow.vector.FieldVector;
import io.polycat.common.expression.rule.ToRexCallVisitor;

public interface Expression extends Serializable {

    DataType getDataType();

    // 获取仅输出作为结果的字段索引
    List<Integer> getFieldIndexes();

    /**
     * 根据表达式对输入数据求值
     *
     * @param record 一行数据
     * @return 结果值
     */
    default Field evaluate(Record record) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    /**
     * 根据表达式求bool值
     *
     * @param record 一行数据
     * @return bool值
     */
    default boolean evaluateToBool(Record record) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    /**
     * 根据表达式对输入数据求值，向量化版本
     *
     * @param batch 向量化数据
     * @return 结果也是向量化数据
     */
    default FieldVector evaluate(VectorBatch batch) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    /**
     * 根据表达式求bool值，向量化版本
     *
     * @param batch 向量化数据
     * @return 结果是bool vector
     */
    default FieldVector evaluateToBool(VectorBatch batch) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    /**
     * 返回表达式字符串
     *
     * @param inputAttributes 子节点的输出字段
     */
    String simpleString(List<Attribute> inputAttributes);

    default String templateString() {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default int templateHash() {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default Expression transformUp(Function<Expression, Expression> rule) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode trans2RexNode(Function<Expression, RexNode> rule) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default Expression accept(ToExpressionVisitor visitor) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default RexNode accept(ToRexCallVisitor visitor) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }

    default List<Expression> collect(Function<Expression, Boolean> rule) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName());
    }
}
