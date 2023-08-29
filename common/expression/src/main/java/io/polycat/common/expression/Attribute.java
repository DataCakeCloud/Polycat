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
import io.polycat.catalog.common.types.DataType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * 代表逻辑算子的输出字段
 */
public class Attribute implements Serializable {

    // 保存该字段在SQL语句中经历过的所有汇聚操作，如果没有做过汇聚，此链表为空
    private List<AggregationType> aggregationTypes;

    /**
     * 保存的是原始的列名字
     */
    private List<String> fieldNames;

    private List<DataType> dataTypes;

    protected List<String> qualifier;

    protected String nameWithQualifier;

    public Attribute(String fieldName) {
        this.qualifier = Collections.emptyList();
        this.fieldNames = Lists.newArrayList(fieldName);
        this.nameWithQualifier = ExpressionUtil.nameWithQualifier(fieldName, qualifier);
        this.dataTypes = new ArrayList<>();
        this.aggregationTypes = Collections.emptyList();
    }

    public Attribute(String fieldName, DataType dataType) {
        this(fieldName, Collections.emptyList(), dataType);
    }

    public Attribute(NamedExpression expression) {
        this(expression.getName(), expression.getQualifier(), expression.getDataType());
    }

    public Attribute(String fieldName, List<String> qualifier, DataType dataType) {
        this.qualifier = qualifier;
        this.fieldNames = Lists.newArrayList(fieldName);
        this.nameWithQualifier = ExpressionUtil.nameWithQualifier(fieldName, qualifier);
        this.dataTypes = dataType == null ? new ArrayList<>() : Lists.newArrayList(dataType);
        this.aggregationTypes = Collections.emptyList();
    }

    public Attribute(String fieldName, String nameWithQualifier, List<String> qualifier, DataType dataType) {
        this.qualifier = qualifier;
        this.fieldNames = Lists.newArrayList(fieldName);
        this.nameWithQualifier = nameWithQualifier;
        this.dataTypes = dataType == null ? new ArrayList<>() : Lists.newArrayList(dataType);
        this.aggregationTypes = Collections.emptyList();
    }

    public Attribute(String fieldName, AggregationType aggregationType) {
        this.qualifier = Collections.emptyList();
        this.fieldNames = Lists.newArrayList(fieldName);
        this.nameWithQualifier = ExpressionUtil.nameWithQualifier(fieldName, qualifier);
        this.dataTypes = new ArrayList<>();
        this.aggregationTypes = Lists.newArrayList(aggregationType);
    }

    public Attribute(List<String> fieldNames) {
        this.qualifier = Collections.emptyList();
        this.fieldNames = fieldNames;
        this.nameWithQualifier = ExpressionUtil.nameWithQualifier(fieldNames.get(0), qualifier);
        this.dataTypes = new ArrayList<>();
        this.aggregationTypes = Collections.emptyList();
    }

    public Attribute(Attribute inputAttribute, AggregationType aggregationType) {
        this.qualifier = inputAttribute.qualifier;
        this.fieldNames = inputAttribute.getFieldNames();
        this.nameWithQualifier = ExpressionUtil.nameWithQualifier(inputAttribute.getNameWithQualifier(), qualifier);
        this.dataTypes = new ArrayList<>();
        this.aggregationTypes = new LinkedList<>();
        // 先加入子节点的汇聚，这样plan树种越往上的汇聚在链表的越后面
        this.aggregationTypes.addAll(inputAttribute.getAggregationTypes());
        this.aggregationTypes.add(aggregationType);
    }

    // 为Count Aggregation设置的单例
    public static final Attribute COUNT_ATTRIBUTE =
        new Attribute(Lists.newArrayList("count(1)"));

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public String getName() {
        return fieldNames.get(0);
    }

    public List<String> getQualifier() {
        return qualifier;
    }

    public String getNameWithQualifier() {
        return nameWithQualifier;
    }

    public List<DataType> getDataTypes() {
        return dataTypes;
    }

    public DataType getDataType() {
        return dataTypes.get(0);
    }

    public void setFieldNames(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    public void setDataTypes(List<DataType> dataTypes) {
        this.dataTypes = dataTypes;
    }

    public List<AggregationType> getAggregationTypes() {
        return aggregationTypes;
    }

    @Override
    public String toString() {
        String attrName;
        if (fieldNames.size() == 1) {
            attrName = fieldNames.get(0);
        } else {
            attrName = fieldNames.toString();
        }
        return toAggregatedName(attrName);
    }

    private String toAggregatedName(String attributeName) {
        String name = attributeName;
        for (AggregationType aggregationType : aggregationTypes) {
            name = String.format("%s(%s)", aggregationType.name(), name);
        }
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Attribute attribute = (Attribute) o;
        return Objects.equals(aggregationTypes, attribute.aggregationTypes) &&
            Objects.equals(fieldNames, attribute.fieldNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregationTypes, fieldNames);
    }
}
