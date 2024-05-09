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
package io.polycat.probe.hive.lineage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import com.esotericsoftware.minlog.Log;
import org.apache.commons.collections.SetUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reference org.apache.hadoop.hive.ql.hooks.LineageLogger
 *
 * @author renjianxu
 * @date 2024/1/3
 */
public class HiveLineageHelper {
    private static Logger LOG = LoggerFactory.getLogger(HiveLineageHelper.class.getName());

    public List<Edge> getEdges(QueryPlan plan, LineageCtx.Index index) {
        LinkedHashMap<String, ObjectPair<SelectOperator, org.apache.hadoop.hive.ql.metadata.Table>>
                finalSelOps = index.getFinalSelectOps();

        Map<String, Vertex> vertexCache = new LinkedHashMap<>();
        List<Edge> edges = new ArrayList<>();
        boolean processedTargetTable = false;

        for (ObjectPair<SelectOperator, Table> pair : finalSelOps.values()) {
            List<FieldSchema> fieldSchemas = plan.getResultSchema().getFieldSchemas();
            SelectOperator SelOp = pair.getFirst();
            Table t = pair.getSecond();
            String destPureDbName = null;
            String destPureTableName = null;
            String destTableName = null;
            List<String> colNames = null;
            if (t != null) {
                destPureDbName = t.getDbName();
                destPureTableName = t.getTableName();
                destTableName = t.getDbName() + "." + t.getTableName();
                fieldSchemas = t.getCols();
            } else {
                for (WriteEntity output : plan.getOutputs()) {
                    Entity.Type entityType = output.getType();
                    if (entityType == Entity.Type.TABLE || entityType == Entity.Type.PARTITION) {
                        t = output.getTable();
                        destPureDbName = t.getDbName();
                        destPureTableName = t.getTableName();
                        destTableName = t.getDbName() + "." + t.getTableName();
                        List<FieldSchema> cols = t.getCols();
                        if (cols != null && !cols.isEmpty()) {
                            colNames = Utilities.getColumnNamesFromFieldSchema(cols);
                            break;
                        }
                        break;
                    }
                }
            }
            if (t == null || processedTargetTable) {
                LOG.info("no target table or target table processed skip lineage parse!");
                continue;
            }
            Map<ColumnInfo, LineageInfo.Dependency> colMap =
                    (Map<ColumnInfo, LineageInfo.Dependency>)
                            index.getDependencies((Operator) SelOp);
            List<LineageInfo.Dependency> dependencies =
                    (colMap != null) ? new ArrayList<>(colMap.values()) : null;
            int fields = fieldSchemas.size();
            if (t != null && colMap != null && fields < colMap.size()) {
                List<FieldSchema> partitionKeys = t.getPartitionKeys();
                int dynamicKeyCount = colMap.size() - fields;
                int keyOffset = partitionKeys.size() - dynamicKeyCount;
                if (keyOffset >= 0) {
                    fields += dynamicKeyCount;
                    for (int i = 0; i < dynamicKeyCount; ++i) {
                        FieldSchema field = partitionKeys.get(keyOffset + i);
                        fieldSchemas.add(field);
                        if (colNames != null) {
                            colNames.add(field.getName());
                        }
                    }
                }
            }
            if (dependencies == null || dependencies.size() != fields) {
                Log.info(
                        "Result schema has "
                                + fields
                                + " fields, but we don't get as many dependencies");
            } else {
                Set<Vertex> targets = new LinkedHashSet<>();
                for (int j = 0; j < fields; ++j) {
                    Vertex target =
                            this.getOrCreateVertex(
                                    vertexCache,
                                    this.getTargetFieldName(
                                            j, destTableName, colNames, fieldSchemas),
                                    Vertex.Type.COLUMN,
                                    destPureDbName,
                                    destPureTableName,
                                    this.getTargetPureFieldName(j, colNames, fieldSchemas));
                    targets.add(target);
                    LineageInfo.Dependency dep = dependencies.get(j);
                    this.addEdge(
                            vertexCache,
                            edges,
                            dep.getBaseCols(),
                            target,
                            dep.getExpr(),
                            Edge.Type.PROJECTION);
                }
                // not deal PREDICATE
            }
            processedTargetTable = true;
        }
        return edges;
    }

    private String getTargetFieldName(
            int fieldIndex,
            String destTableName,
            List<String> colNames,
            List<FieldSchema> fieldSchemas) {
        String fieldName = fieldSchemas.get(fieldIndex).getName();
        String[] parts = fieldName.split("\\.");
        if (destTableName != null) {
            String colName = parts[parts.length - 1];
            if (colNames != null && !colNames.contains(colName) && colNames.size() > fieldIndex) {
                colName = colNames.get(fieldIndex);
            }
            return destTableName + "." + colName;
        }
        if (parts.length == 2 && parts[0].startsWith("_u")) {
            return parts[1];
        }
        return fieldName;
    }

    private String getTargetPureFieldName(
            int fieldIndex, List<String> colNames, List<FieldSchema> fieldSchemas) {
        String fieldName = fieldSchemas.get(fieldIndex).getName();
        String[] parts = fieldName.split("\\.");
        String colName = parts[parts.length - 1];
        if (org.apache.commons.lang.StringUtils.isNotEmpty(colName)) {
            return colName;
        }
        if (parts.length == 2 && parts[0].startsWith("_u")) {
            return parts[1];
        }
        return fieldName;
    }

    private Set<Vertex> createSourceVertices(
            Map<String, Vertex> vertexCache, Collection<LineageInfo.BaseColumnInfo> baseCols) {
        Set<Vertex> sources = new LinkedHashSet<>();
        if (baseCols != null && !baseCols.isEmpty()) {
            for (LineageInfo.BaseColumnInfo col : baseCols) {
                org.apache.hadoop.hive.metastore.api.Table table = col.getTabAlias().getTable();
                if (table.isTemporary()) {
                    continue;
                }
                Vertex.Type type = Vertex.Type.TABLE;
                String fullTableName = table.getDbName() + "." + table.getTableName();
                FieldSchema fieldSchema = col.getColumn();
                String label = fullTableName;
                String dbName = table.getDbName();
                String tableName = table.getTableName();
                String columnName = null;
                if (fieldSchema != null) {
                    type = Vertex.Type.COLUMN;
                    label = fullTableName + "." + fieldSchema.getName();
                    columnName = fieldSchema.getName();
                }
                sources.add(
                        this.getOrCreateVertex(
                                vertexCache, label, type, dbName, tableName, columnName));
            }
        }
        return sources;
    }

    private Edge findSimilarEdgeBySources(
            List<Edge> edges, Set<Vertex> sources, String expr, Edge.Type type) {
        for (Edge edge : edges) {
            if (edge.type == type
                    && org.apache.commons.lang.StringUtils.equals(edge.expr, expr)
                    && SetUtils.isEqualSet((Collection) edge.sources, (Collection) sources)) {
                return edge;
            }
        }
        return null;
    }

    private void addEdge(
            Map<String, Vertex> vertexCache,
            List<Edge> edges,
            Set<LineageInfo.BaseColumnInfo> srcCols,
            Vertex target,
            String expr,
            Edge.Type type) {
        Set<Vertex> targets = new LinkedHashSet<>();
        targets.add(target);
        this.addEdge(vertexCache, edges, srcCols, targets, expr, type);
    }

    private void addEdge(
            Map<String, Vertex> vertexCache,
            List<Edge> edges,
            Set<LineageInfo.BaseColumnInfo> srcCols,
            Set<Vertex> targets,
            String expr,
            Edge.Type type) {
        Set<Vertex> sources = this.createSourceVertices(vertexCache, srcCols);
        Edge edge = this.findSimilarEdgeBySources(edges, sources, expr, type);
        if (edge == null) {
            edges.add(new Edge(sources, targets, expr, type));
        } else {
            edge.targets.addAll(targets);
        }
    }

    private Vertex getOrCreateVertex(
            Map<String, Vertex> vertices,
            String label,
            Vertex.Type type,
            String dbName,
            String tableName,
            String columnName) {
        Vertex vertex = vertices.get(label);
        if (vertex == null) {
            vertex = new Vertex(label, type, dbName, tableName, columnName);
            vertices.put(label, vertex);
        }
        return vertex;
    }
}
