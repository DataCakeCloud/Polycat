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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.joining;

public class AggregateFunction implements Serializable {
    private final AggregationType aggregationType;

    private List<Integer> aggregateColumnIndexes;

    private List<NamedExpression> expressions;

    public AggregateFunction(AggregationType aggregationType, List<Integer> aggregateColumnIndexes) {
        this.aggregationType = aggregationType;
        this.aggregateColumnIndexes = aggregateColumnIndexes;
    }

    public AggregateFunction(List<NamedExpression> expressions, AggregationType aggregationType) {
        this.aggregationType = aggregationType;
        this.expressions = expressions;
        this.aggregateColumnIndexes = new ArrayList<>(expressions.size());
    }

    public AggregationType getAggregationType() {
        return aggregationType;
    }

    public List<Integer> getAggregateColumnIndexes() {
        return aggregateColumnIndexes;
    }

    public List<NamedExpression> getExpressions() {
        return expressions;
    }

    public String desc(List<Attribute> inputAttributes) {
        return aggregationType + "(" + expressions.stream()
                .map(it -> it.simpleString(null))
                .collect(joining(",")) + ")";
    }

    public String getName() {
        return aggregationType + "(" + expressions.stream().map(NamedExpression::getName).collect(joining(",")) + ")";
    }

    @Override
    public String toString() {
        return aggregationType + "(" + aggregateColumnIndexes.stream().map(it -> "#" + it).collect(joining(",")) + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AggregateFunction that = (AggregateFunction) o;
        return aggregationType == that.aggregationType && Objects.equals(aggregateColumnIndexes,
                that.aggregateColumnIndexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregationType, aggregateColumnIndexes);
    }
}
