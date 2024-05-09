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
package io.polycat.catalog.server.wrapper;

import io.polycat.catalog.common.utils.SQLUtil;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.FilterBuilder;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LogicalOperator;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.Operator;

public class PartitionFilterStatesWrapper {

    public static enum FilterOptimizerType {
        EQUALS,
        DEFAULT,
    }

    public List<String> keys = new ArrayList<>();
    public List<String> partitionKeys = new ArrayList<>();
    public List<String> alignPartitionValues = new ArrayList<>();
    public List<Operator> operators = new ArrayList<>();
    public List<Object> values = new ArrayList<>();
    public List<LogicalOperator> logicalOperators = new ArrayList<>();
    public String filterSql = null;
    public FilterOptimizerType filterOptimizerType;

    public void addOperator(Operator operator) {
        operators.add(operator);
    }

    public void setFilterSql(String filterSql) {
        this.filterSql = filterSql;
    }

    public void addColumnKey(String key) {
        keys.add(key);
    }

    public void addLogicalOperators(LogicalOperator logicalOperator) {
        logicalOperators.add(logicalOperator);
    }

    public void addColumnValue(Object value) {
        if (value instanceof String) {
            values.add(SQLUtil.parseSqlQuotesValue((String) value));
            return;
        }
        values.add(value);
    }

    public void setFilterOptimizerType() {
        this.filterOptimizerType = initFilterOptimizerType();
        alignPartitionKv();
    }

    /**
     * Align the partition kv, by default fill '.*'
     */
    private void alignPartitionKv() {
        if (alignPartitionValues.size() == 0) {
            for (int i = 0; i < partitionKeys.size(); i++) {
                if (keys.contains(partitionKeys.get(i))) {
                    alignPartitionValues.add(values.get(keys.indexOf(partitionKeys.get(i))).toString());
                } else {
                    alignPartitionValues.add(SQLUtil.HIVE_WILDCARD_IDENTIFIER);
                }
            }
        }
    }

    private FilterOptimizerType initFilterOptimizerType() {
        if (keys.size() == values.size() && values.size() == operators.size()) {
            Set<Operator> distinctOperators = new HashSet<>(operators);
            if (distinctOperators.size() == 0) {
                return FilterOptimizerType.EQUALS;
            } else if (distinctOperators.size() == 1) {
                switch (operators.get(0)) {
                    case EQUALS:
                    case LIKE:
                        if (logicalOperators.contains(LogicalOperator.OR)) {
                            return FilterOptimizerType.DEFAULT;
                        }
                        return FilterOptimizerType.EQUALS;
                    default:
                        return FilterOptimizerType.DEFAULT;
                }
            } else if (distinctOperators.size() == 2 &&
                    distinctOperators.contains(Operator.EQUALS) && distinctOperators.contains(Operator.LIKE)) {
                return FilterOptimizerType.EQUALS;
            }
        }
        return FilterOptimizerType.DEFAULT;
    }

    public static class FilterBuilderList extends FilterBuilder {
        public Map<Integer, String> map = new LinkedHashMap<>();
        private Integer partIndex = 0;

        public FilterBuilderList(boolean expectNoErrors) {
            super(expectNoErrors);
        }

        @Override
        public ExpressionTree.FilterBuilder append(String filterPart) {
            super.append(filterPart);
            map.put(partIndex++, filterPart);
            return this;
        }
    }
}
