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
package io.polycat.catalog.server.hive;

import io.polycat.catalog.server.wrapper.PartitionFilterStatesWrapper;
import io.polycat.catalog.common.model.ColumnObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableSchemaObject;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.catalog.common.utils.SQLUtil;

import java.util.ArrayList;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.*;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author liangyouze
 * @date 2022/12/3
 */
@Slf4j
public class PartitionFilterGenerator extends TreeVisitor {
    private final TableSchemaObject tableSchema;
    private final TableIdent tableIdent;
    private final FilterBuilder filterBuffer;
    private final List<Object> params;
    private final Map<String, String> joins;
    private final String defaultPartName;
    private final PartitionFilterStatesWrapper partitionFilterStatesWrapper;
    private List<String> partitionKeys = new ArrayList<>();

    public static final ThreadLocal<DateFormat> PARTITION_DATE_FORMAT =
        ThreadLocal.withInitial(() -> {
            DateFormat val = new SimpleDateFormat("yyyy-MM-dd");
            val.setLenient(false); // Without this, 2020-20-20 becomes 2021-08-20.
            return val;
        });

    private PartitionFilterGenerator(TableSchemaObject tableSchema,
            TableIdent tableIdent, List<Object> params, Map<String, String> joins,
            String defaultPartName, PartitionFilterStatesWrapper partitionFilterStatesWrapper) {
        this.tableSchema = tableSchema;
        if (tableSchema.getPartitionKeys() != null) {
            partitionKeys = tableSchema.getPartitionKeys().stream().map(ColumnObject::getName).collect(Collectors.toList());
        }
        this.tableIdent = tableIdent;
        this.params = params;
        this.joins = joins;
        this.filterBuffer = new FilterBuilder(false);
        this.defaultPartName = defaultPartName;
        this.partitionFilterStatesWrapper = partitionFilterStatesWrapper;
        this.partitionFilterStatesWrapper.partitionKeys = partitionKeys;
    }

    /**
     * Generate the ANSI SQL92 filter for the given expression tree
     * @param tableSchema the table being queried
     * @param params the ordered parameters for the resulting expression
     * @param joins the joins necessary for the resulting expression
     * @return the string representation of the expression tree
     */
    public static String generateSqlFilter(TableIdent tableIdent, TableSchemaObject tableSchema, ExpressionTree tree, List<Object> params,
        Map<String, String> joins, String defaultPartName, PartitionFilterStatesWrapper partitionFilterStatesWrapper) throws MetaException {
        assert tableSchema != null;
        if (tree == null) {
            // consistent with other APIs like makeExpressionTree, null is returned to indicate that
            // the filter could not pushed down due to parsing issue etc
            return null;
        }
        if (tree.getRoot() == null) {
            return "";
        }
        PartitionFilterGenerator visitor = new PartitionFilterGenerator(
            tableSchema, tableIdent, params, joins, defaultPartName, partitionFilterStatesWrapper);
        tree.accept(visitor);
        if (visitor.filterBuffer.hasError()) {
            log.warn("Unable to push down SQL filter: " + visitor.filterBuffer.getErrorMessage());
            return null;
        }

        // Some joins might be null (see processNode for LeafNode), clean them up.
        String generatorFilterSql = StringUtils.join(joins.values(), " ") + " where " + visitor.filterBuffer.getFilter();
        partitionFilterStatesWrapper.setFilterSql(generatorFilterSql);
        partitionFilterStatesWrapper.setFilterOptimizerType();
        return generatorFilterSql;
    }

    @Override
    protected void beginTreeNode(TreeNode node) throws MetaException {
        filterBuffer.append(" (");
    }

    @Override
    protected void midTreeNode(TreeNode node) throws MetaException {
        String logicalOperator = (node.getAndOr() == LogicalOperator.AND) ? " and " : " or ";
        filterBuffer.append(logicalOperator);
        partitionFilterStatesWrapper.addLogicalOperators(node.getAndOr());
    }

    @Override
    protected void endTreeNode(TreeNode node) throws MetaException {
        filterBuffer.append(") ");
    }

    @Override
    protected boolean shouldStop() {
        return filterBuffer.hasError();
    }

    private static enum FilterType {
        Integral,
        String,
        Date,
        Invalid;

        static PartitionFilterGenerator.FilterType fromType(String colTypeStr) {
            if (colTypeStr.equals(DataTypes.STRING.getName())) {
                return PartitionFilterGenerator.FilterType.String;
            } else if (colTypeStr.equals(DataTypes.DATE.getName())) {
                return PartitionFilterGenerator.FilterType.Date;
            } else if (DataTypes.isIntegerType(DataTypes.valueOf(colTypeStr))) {
                return PartitionFilterGenerator.FilterType.Integral;
            }
            return PartitionFilterGenerator.FilterType.Invalid;
        }

        public static PartitionFilterGenerator.FilterType fromClass(Object value) {
            if (value instanceof String) {
                return PartitionFilterGenerator.FilterType.String;
            } else if (value instanceof Long) {
                return PartitionFilterGenerator.FilterType.Integral;
            } else if (value instanceof java.sql.Date) {
                return PartitionFilterGenerator.FilterType.Date;
            }
            return PartitionFilterGenerator.FilterType.Invalid;
        }
    }

    @Override
    public void visit(LeafNode node) throws MetaException {
//        if (node.operator == Operator.LIKE) {
//            filterBuffer.setError("LIKE is not supported for SQL filter pushdown");
//            return;
//        }

        final String keyName = node.keyName;
        if (!partitionKeys.contains(keyName)) {
            throw new MetaException("Filter on invalid key <" + keyName + "> is not a partitioning key for the table");
        }
        final List<ColumnObject> partitionKeys = tableSchema.getPartitionKeys();
        String colTypeStr = null;
        for (ColumnObject partitionKey : partitionKeys) {
            if (partitionKey.getName().equals(keyName)) {
                colTypeStr = partitionKey.getDataType().getName();
                break;
            }
        }
        if (colTypeStr == null) {
            filterBuffer.setError("Specified key <" + keyName +
                "> is not a partitioning key for the table");
            return;
        }

        // We skipped 'like', other ops should all work as long as the types are right.
        PartitionFilterGenerator.FilterType colType = PartitionFilterGenerator.FilterType
            .fromType(colTypeStr);
        if (colType == PartitionFilterGenerator.FilterType.Invalid) {
            filterBuffer.setError("Filter pushdown not supported for type " + colTypeStr);
            return;
        }
        PartitionFilterGenerator.FilterType valType = PartitionFilterGenerator.FilterType
            .fromClass(node.value);
        Object nodeValue = node.value;
        if (valType == PartitionFilterGenerator.FilterType.Invalid) {
            filterBuffer.setError("Filter pushdown not supported for value " + node.value.getClass());
            return;
        }

        // if Filter.g does date parsing for quoted strings, we'd need to verify there's no
        // type mismatch when string col is filtered by a string that looks like date.
        if (colType == PartitionFilterGenerator.FilterType.Date && valType == PartitionFilterGenerator.FilterType.String) {
            // Filter.g cannot parse a quoted date; try to parse date here too.
            try {
                nodeValue = new java.sql.Date(
                    PARTITION_DATE_FORMAT.get().parse((String)nodeValue).getTime());
                valType = PartitionFilterGenerator.FilterType.Date;
            } catch (ParseException pe) { // do nothing, handled below - types will mismatch

            }
        }

       /* if (colType != valType) {
            // It's not clear how filtering for e.g. "stringCol > 5" should work (which side is
            // to be coerced?). Let the expression evaluation sort this one out, not metastore.
            filterBuffer.setError("Cannot push down filter for "
                + colTypeStr + " column and value " + nodeValue.getClass());
            return;
        }*/

        if (joins.get(keyName) == null) {
            joins.put(keyName, "inner join schema_" + tableIdent.getProjectId() + ".table_partition_column_info_" +
                tableIdent.getTableId() + " filter_" + keyName
                + " on filter_"  + keyName + ".partition_id = p.partition_id and filter_" + keyName + ".name = '"
                + keyName + "'");
        }

        /*filterBuffer.append("INNER JOIN schema_" + tableIdent.getProjectId() + ".table_partition_column_info_"
            + tableIdent.getTableId() + " filter_" + keyName
            + " ON filter_"  + keyName + ".partition_id = p.partition_id AND filter_" + keyName + ".name = '"
            + keyName + "'");*/

        // Build the filter and add parameters linearly; we are traversing leaf nodes LTR.
        String tableValue = "filter_" + keyName + ".value";

        if (node.isReverseOrder) {
            params.add(nodeValue);
        }
        String tableColumn = tableValue;
        if (colType != PartitionFilterGenerator.FilterType.String) {
            if (colType != valType) {
                throw new MetaException("Filtering is supported only on partition keys of type string");
            }
            // The underlying database field is varchar, we need to compare numbers.
            if (colType == PartitionFilterGenerator.FilterType.Integral) {
                tableValue = "cast(" + tableValue + " as decimal(21,0))";
            } else if (colType == PartitionFilterGenerator.FilterType.Date) {
                tableValue = "cast(" + tableValue + " as date)";
                nodeValue = "'" + nodeValue + "'";
            }

            // Workaround for HIVE_DEFAULT_PARTITION - ignore it like JDO does, for now.
            String tableValue0 = tableValue;
            tableValue = "(case when " + tableColumn + " <> '" + defaultPartName + "'";
            params.add(defaultPartName);

            tableValue += " then " + tableValue0 + " else null end)";
        } else {
            nodeValue = "'" + nodeValue + "'";
        }

        partitionFilterStatesWrapper.addColumnKey(keyName);
        partitionFilterStatesWrapper.addOperator(node.operator);
        partitionFilterStatesWrapper.addColumnValue(nodeValue);

        if (!node.isReverseOrder) {
            params.add(nodeValue);
        }

        if (node.operator == Operator.LIKE) {
            nodeValue = SQLUtil.likeEscapeForHiveQuotesValue(nodeValue.toString());
        } else {
            nodeValue = SQLUtil.escapeForHiveQuotesValue(nodeValue.toString());
        }
        filterBuffer.append(node.isReverseOrder
                ? "( " + nodeValue + " " + node.operator.getSqlOp() + " " + tableValue + ")"
                : "( " + tableValue + " " + node.operator.getSqlOp() + " " + nodeValue + " )");
    }
}
