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

import io.polycat.catalog.common.lineage.EDbType;
import io.polycat.catalog.common.lineage.EDependWay;
import io.polycat.catalog.common.lineage.EJobStatus;
import io.polycat.catalog.common.lineage.ELineageObjectType;
import io.polycat.catalog.common.lineage.ELineageSourceType;
import io.polycat.catalog.common.lineage.ELineageType;
import io.polycat.catalog.common.lineage.LineageConstants;
import io.polycat.catalog.common.lineage.LineageFact;
import io.polycat.catalog.common.lineage.LineageNode;
import io.polycat.catalog.common.plugin.request.input.LineageInfoInput;
import io.polycat.catalog.common.utils.LineageUtils;
import io.polycat.probe.EventProcessor;
import io.polycat.probe.ProbeConstants;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHook;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookContext;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author renjianxu
 * @date 2024/1/2
 */
public class HiveLineageHook implements QueryLifeTimeHook {
    private static final Logger LOG = LoggerFactory.getLogger(HiveLineageHook.class.getName());

    private static final HashSet<String> OPERATION_NAMES = new HashSet<String>();

    static {
        OPERATION_NAMES.add(HiveOperation.QUERY.getOperationName());
        OPERATION_NAMES.add(HiveOperation.CREATETABLE_AS_SELECT.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERVIEW_AS.getOperationName());
        OPERATION_NAMES.add(HiveOperation.CREATEVIEW.getOperationName());
    }

    @Override
    public void beforeCompile(QueryLifeTimeHookContext ctx) {}

    @Override
    public void afterCompile(QueryLifeTimeHookContext ctx, boolean hasError) {}

    @Override
    public void beforeExecution(QueryLifeTimeHookContext ctx) {}

    @Override
    public void afterExecution(QueryLifeTimeHookContext ctx, boolean hasError) {
        HookContext hookContext = ctx.getHookContext();
        HiveConf hiveConf = (HiveConf) hookContext.getConf();
        QueryPlan plan = hookContext.getQueryPlan();
        LineageCtx.Index index = hookContext.getIndex();
        SessionState ss = SessionState.get();
        String catalog = hiveConf.get(ProbeConstants.HIVE_HMS_BRIDGE_CATALOG_NAME);
        if (ss != null
                && index != null
                && OPERATION_NAMES.contains(plan.getOperationName())
                && !plan.isExplain()) {
            try {
                LineageInfoInput lineageInfo = new LineageInfoInput();
                String sql = plan.getQueryStr().trim();
                LineageFact lineageFact = new LineageFact();
                String errorMessage = ctx.getHookContext().getErrorMessage();
                Throwable exception = ctx.getHookContext().getException();
                hasError = StringUtils.isNotEmpty(errorMessage) || Objects.nonNull(exception);
                if (hasError) {
                    lineageFact.setJobStatus(EJobStatus.FAILED);
                    lineageFact.setErrorMsg(errorMessage);
                    Optional.ofNullable(exception)
                            .ifPresent(
                                    throwable ->
                                            lineageFact.setErrorMsg(stringifyException(throwable)));
                } else {
                    lineageFact.setJobStatus(EJobStatus.SUCCESS);
                }
                lineageFact.setSql(sql);
                long queryTime =
                        plan.getQueryStartTime() == 0
                                ? System.currentTimeMillis()
                                : plan.getQueryStartTime();
                lineageFact.setStartTime(queryTime);
                lineageFact.setEndTime(System.currentTimeMillis());
                Map<String, Object> lineageConfig = getLineageConfig(hiveConf);
                lineageConfig.put(
                        LineageConstants.LINEAGE_JOB_FACT_PREFIX
                                + ProbeConstants.CATALOG_PROBE_SOURCE,
                        hiveConf.get(ProbeConstants.CATALOG_PROBE_SOURCE));
                lineageFact.setParams(lineageConfig);
                lineageFact.setJobType(EDbType.HIVE.name());
                Map<String, String> customConfig = getCustomConfig(hiveConf);
                lineageFact.setCluster(customConfig.get(ProbeConstants.CATALOG_PROBE_CLUSTER_NAME));
                lineageFact.setExecuteUser(customConfig.get(ProbeConstants.CATALOG_PROBE_USER_ID));
                lineageFact.setJobId(customConfig.get(ProbeConstants.CATALOG_PROBE_TASK_ID));
                lineageFact.setJobName(customConfig.get(ProbeConstants.CATALOG_PROBE_TASK_NAME));
                lineageInfo.setJobFact(lineageFact);
                // fill lineage table and column relation
                List<Edge> edges = new HiveLineageHelper().getEdges(plan, index);
                buildTableLineages(catalog, lineageInfo, edges);
                buildColumnLineages(catalog, lineageInfo, edges);
                LOG.info("HiveLineageHook:{}", lineageInfo.toString());
                if (null != lineageInfo.getNodeMap() && !lineageInfo.getNodeMap().isEmpty()) {
                    EventProcessor eventProcessor = new EventProcessor(hiveConf);
                    eventProcessor.pushSqlLineage(lineageInfo);
                }
            } catch (Exception e) {
                LOG.error("exec HiveLineageHook error! ", e);
            }
        }
    }

    private Map<String, Object> getLineageConfig(HiveConf hiveConf) {
        Map<String, Object> lineageConfig = new HashMap<>();
        Iterator<Map.Entry<String, String>> iterator = hiveConf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> kv = iterator.next();
            if (kv.getKey().startsWith(LineageConstants.LINEAGE_JOB_FACT_PREFIX)) {
                lineageConfig.put(kv.getKey(), kv.getValue());
            }
        }
        return lineageConfig;
    }

    private Map<String, String> getCustomConfig(HiveConf conf) {
        Map<String, String> customConfig = new HashMap<>();
        String paraString = HiveConf.getVar(conf, HiveConf.ConfVars.NEWTABLEDEFAULTPARA);
        if (paraString != null && !paraString.isEmpty()) {
            for (String keyValuePair : paraString.split(",")) {
                String[] keyValue = keyValuePair.split("=", 2);
                if (keyValue.length != 2) {
                    continue;
                }
                if (!customConfig.containsKey(keyValue[0])) {
                    customConfig.put(keyValue[0], keyValue[1]);
                }
            }
        }
        return customConfig;
    }

    private void buildColumnLineages(
            String catalog, LineageInfoInput lineageInfo, List<Edge> edges) {
        for (final Edge edge : edges) {
            String expression = null;
            Set<Vertex> vertices = new HashSet<>();
            vertices.addAll(edge.sources);
            vertices.addAll(edge.targets);
            for (final Vertex vertex : vertices) {
                String tableQualifiedName =
                        LineageUtils.getTableQualifiedName(
                                catalog,
                                normalizeIdentifier(vertex.dbName),
                                normalizeIdentifier(vertex.tableName));
                lineageInfo.addNode(
                        new LineageNode(
                                LineageUtils.getColumnQualifiedName(
                                        tableQualifiedName, vertex.columnName),
                                EDbType.HIVE,
                                ELineageObjectType.COLUMN,
                                ELineageSourceType.LINEAGE,
                                false));
            }
            if (edge.expr != null) {
                expression = edge.expr;
            }
            for (final Vertex source : edge.sources) {
                for (final Vertex target : edge.targets) {
                    String upstreamNode =
                            LineageUtils.getTableQualifiedName(
                                    catalog,
                                    normalizeIdentifier(source.dbName),
                                    normalizeIdentifier(source.tableName));
                    String downstreamNode =
                            LineageUtils.getTableQualifiedName(
                                    catalog,
                                    normalizeIdentifier(target.dbName),
                                    normalizeIdentifier(target.tableName));
                    LineageInfoInput.LineageRsInput lineageRs =
                            new LineageInfoInput.LineageRsInput(
                                    ELineageType.FIELD_DEPEND_FIELD,
                                    EDependWay.EXPRESSION,
                                    expression);
                    lineageRs.addUpstreamNode(
                            EDbType.HIVE,
                            ELineageObjectType.COLUMN,
                            LineageUtils.getColumnQualifiedName(upstreamNode, source.columnName));
                    lineageRs.setDownstreamNode(
                            EDbType.HIVE,
                            ELineageObjectType.COLUMN,
                            LineageUtils.getColumnQualifiedName(downstreamNode, target.columnName));
                    lineageInfo.addRelationShipMap(lineageRs);
                }
            }
        }
    }

    private void buildTableLineages(
            String catalog, LineageInfoInput lineageInfo, List<Edge> edges) {
        for (final Edge edge : edges) {
            Set<Vertex> vertices = new HashSet<>();
            vertices.addAll(edge.sources);
            vertices.addAll(edge.targets);
            for (final Vertex vertex : vertices) {
                String lineageQualifiedName =
                        LineageUtils.getTableQualifiedName(
                                catalog,
                                normalizeIdentifier(vertex.dbName),
                                normalizeIdentifier(vertex.tableName));
                lineageInfo.addNode(
                        new LineageNode(
                                lineageQualifiedName,
                                EDbType.HIVE,
                                ELineageObjectType.TABLE,
                                ELineageSourceType.LINEAGE,
                                false));
            }
            for (final Vertex source : edge.sources) {
                for (final Vertex target : edge.targets) {
                    String upstreamNode =
                            LineageUtils.getTableQualifiedName(
                                    catalog,
                                    normalizeIdentifier(source.dbName),
                                    normalizeIdentifier(source.tableName));
                    String downstreamNode =
                            LineageUtils.getTableQualifiedName(
                                    catalog,
                                    normalizeIdentifier(target.dbName),
                                    normalizeIdentifier(target.tableName));

                    LineageInfoInput.LineageRsInput lineageRs =
                            new LineageInfoInput.LineageRsInput(ELineageType.TABLE_DEPEND_TABLE);
                    lineageRs.addUpstreamNode(EDbType.HIVE, ELineageObjectType.TABLE, upstreamNode);
                    lineageRs.setDownstreamNode(
                            EDbType.HIVE, ELineageObjectType.TABLE, downstreamNode);
                    lineageInfo.addRelationShipMap(lineageRs);
                }
            }
        }
    }

    public static String normalizeIdentifier(String identifier) {
        return StringUtils.isBlank(identifier)
                ? identifier
                : HiveStringUtils.normalizeIdentifier(identifier);
    }

    public static String stringifyException(Throwable e) {
        StringWriter stm = new StringWriter();
        PrintWriter wrt = new PrintWriter(stm);
        e.printStackTrace(wrt);
        wrt.close();
        return stm.toString();
    }
}
