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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import io.polycat.catalog.server.wrapper.PartitionFilterStatesWrapper;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.ColumnObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableSchemaObject;

import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * @author liangyouze
 * @date 2023/7/17
 */
public class HiveUtil {

    private static PartitionExpressionProxy expressionProxy = new PartitionExpressionForMetastore();

    private static ExpressionTree makeExpressionTree(String filter) {
        try {
            return (filter != null && !filter.isEmpty())
                    ? PartFilterExprUtil.getFilterParser(filter).tree : ExpressionTree.EMPTY_TREE;
        } catch (MetaException metaException) {
            throw new CatalogServerException(ErrorCode.PARTITION_FILTER_ILLEGAL, metaException);
        }
    }

    private static ExpressionTree makeExpressionTree(byte[] expr) {
        try {
            return PartFilterExprUtil
                    .makeExpressionTree(expressionProxy, expr);
        } catch (MetaException metaException) {
            throw new CatalogServerException(ErrorCode.PARTITION_FILTER_ILLEGAL, metaException);
        }
    }

    public static String generatePartitionSqlFilter(TableIdent tableIdent, TableSchemaObject tableSchemaObject, String filter, PartitionFilterStatesWrapper partitionFilterStatesWrapper) {
        try {
            return PartitionFilterGenerator
                    .generateSqlFilter(tableIdent, tableSchemaObject, makeExpressionTree(filter), new ArrayList<>(), new HashMap<>(),
                            "__DEFAULT_PARTITION__", partitionFilterStatesWrapper);
        } catch (MetaException metaException) {
            throw new CatalogServerException(ErrorCode.PARTITION_FILTER_ILLEGAL, metaException);
        }
    }

    public static String generatePartitionSqlFilter(TableIdent tableIdent, TableSchemaObject tableSchemaObject, byte[] expr) {
        try {
            final ExpressionTree expressionTree = makeExpressionTree(expr);
            if (expressionTree == null) {
                return null;
            }
            return PartitionFilterGenerator
                    .generateSqlFilter(tableIdent, tableSchemaObject, makeExpressionTree(expr), new ArrayList<>(), new HashMap<>(),
                            "__DEFAULT_PARTITION__", new PartitionFilterStatesWrapper());
        } catch (MetaException metaException) {
            return null;
        }
    }

    public static boolean filterPartitionsByExpr(List<ColumnObject> partitionKeys, byte[] expr, String defaultPartitionName,
                                                 List<String> partitionNames) {
        final List<String> columnNames = partitionKeys.stream().map(ColumnObject::getName)
                .collect(Collectors.toList());
        final List<PrimitiveTypeInfo> typeInfos = partitionKeys.stream()
                .map(partitionKey -> TypeInfoFactory.getPrimitiveTypeInfo(partitionKey.getDataType().getName().toLowerCase()))
                .collect(Collectors.toList());
        try {
            return expressionProxy
                    .filterPartitionsByExpr(columnNames, typeInfos, expr, defaultPartitionName,
                            partitionNames);
        } catch (MetaException e) {
            throw new CatalogServerException(ErrorCode.PARTITION_FILTER_ILLEGAL, e);
        } catch (RuntimeException e) {
            throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR, e);
        }

    }
}
