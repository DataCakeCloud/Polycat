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
package io.polycat.catalog.server.service.impl;

import io.polycat.catalog.server.util.TransactionRunnerUtil;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.DataLineageType;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.lineage.*;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.plugin.request.input.DataLineageInput;
import io.polycat.catalog.common.plugin.request.input.LineageInfoInput;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.service.api.DataLineageService;
import io.polycat.catalog.store.api.DataLineageStore;
import io.polycat.catalog.store.api.Transaction;
import io.polycat.catalog.store.common.StoreConvertor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.*;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class DataLineageServiceImpl implements DataLineageService {

    private final static int maxLineageDepth = 10;

    @Autowired
    private Transaction storeTransaction;
    @Autowired
    private DataLineageStore dataLineageStore;

    @Override
    public void recordDataLineage(DataLineageInput dataLineageInput) {
        if (Objects.isNull(dataLineageInput.getDataLineage().getDataOutput())) {
            throw new CatalogServerException(ErrorCode.DATA_LINEAGE_OUTPUT_ERROR);
        }

        TableSource tableSource = dataLineageInput.getDataLineage().getDataOutput().getTableSource();
        TableObject table = TableObjectHelper.getTableObject(StoreConvertor.tableName(tableSource.getProjectId(),
                tableSource.getCatalogName(), tableSource.getDatabaseName(), tableSource.getTableName()));
        if (Objects.isNull(table)) {
            throw new CatalogServerException(ErrorCode.DATA_LINEAGE_SOURCE_ERROR, tableSource);
        }
        TableIdent targetTableIdent = StoreConvertor.tableIdent(table.getProjectId(),
                table.getCatalogId(), table.getDatabaseId(), table.getTableId(), null);
        DataLineageObject dataLineageObject = new DataLineageObject(targetTableIdent, DataSourceType.SOURCE_TABLE,
                "", dataLineageInput.getDataLineage().getOperation().toString());
        try (TransactionContext ctx = storeTransaction.openTransaction()) {
            for (DataSource dataSource : dataLineageInput.getDataLineage().getDataInput()) {
                dataLineageObject.setDataSourceType(dataSource.getSourceType());
                switch (dataSource.getSourceType()) {
                    case SOURCE_TABLE:
                        TableObject dataSourceTable = TableObjectHelper.getTableObject(
                                StoreConvertor.tableName(dataSource.getTableSource().getProjectId(),
                                        dataSource.getTableSource().getCatalogName(),
                                        dataSource.getTableSource().getDatabaseName(),
                                        dataSource.getTableSource().getTableName()));
                        if (dataSourceTable == null) {
                            continue;
                        }
                        String strSourceTable = dataSourceTable.getProjectId() + ":"
                                + dataSourceTable.getCatalogId() + ":"
                                + dataSourceTable.getDatabaseId() + ":"
                                + dataSourceTable.getTableId();
                        dataLineageObject.setDataSourceContent(strSourceTable);
                        break;
                    case SOURCE_STREAM:
                        dataLineageObject.setDataSourceContent(dataSource.getStreamerSource().getStreamer());
                        break;
                    case SOURCE_FILE:
                        dataLineageObject.setDataSourceContent(dataSource.getFileSource().getPath());
                        break;
                    case SOURCE_APP:
                        dataLineageObject.setDataSourceContent(dataSource.getAppSource().getAppName());
                        break;
                    default:
                        throw new CatalogServerException(ErrorCode.DATA_LINEAGE_SOURCE_TYPE_ILLEGAL,
                                dataSource.getSourceType());
                }

                dataLineageStore.upsertDataLineage(ctx, dataLineageObject);
                ctx.commit();
            }
        }
    }

    @Override
    public List<DataLineage> getDataLineageByTable(String projectId, String catalogName, String databaseName,
                                                   String tableName, DataLineageType dataLineageType) {
        TableName name = new TableName(projectId, catalogName, databaseName, tableName);
        TableObject table = TableObjectHelper.getTableObject(name);
        if (table == null) {
            return Collections.emptyList();
        }

        List<DataLineageObject> dataLineageObjects;
        try (TransactionContext ctx = storeTransaction.openTransaction()) {
            if (dataLineageType == DataLineageType.UPSTREAM) {
                dataLineageObjects = dataLineageStore.listDataLineageByTableId(ctx, projectId,
                        table.getCatalogId(), table.getDatabaseId(), table.getTableId());
            } else {
                String dataSourceContent = table.getProjectId() + ":" + table.getCatalogId() + ":"
                        + table.getDatabaseId() + ":" + table.getTableId();
                dataLineageObjects = dataLineageStore.listDataLineageByDataSource(ctx, table.getProjectId(),
                        DataSourceType.SOURCE_TABLE, dataSourceContent);
            }
        }


        List<DataLineage> dataLineageList = new ArrayList<>();
        for (DataLineageObject value : dataLineageObjects) {
            DataSource dataSource = null;
            switch (value.getDataSourceType()) {
                case SOURCE_TABLE:
                    String[] strSourceTable = value.getDataSourceContent().split(":");
                    if (strSourceTable.length < 4) {
                        break;
                    }
                    TableObject sourceTableRecord = TableObjectHelper.getTableObject(new TableIdent(strSourceTable[0],
                            strSourceTable[1], strSourceTable[2], strSourceTable[3]));
                    if (sourceTableRecord == null) {
                        break;
                    }
                    TableSource tableSource = new TableSource(projectId,
                            TableObjectConvertHelper.toTableModel(sourceTableRecord));
                    dataSource = new DataSource(tableSource);
                    break;
                case SOURCE_STREAM:
                    StreamerSource streamSource = new StreamerSource(value.getDataSourceContent());
                    dataSource = new DataSource(streamSource);
                    break;
                case SOURCE_FILE:
                    FileSource fileSource = new FileSource(value.getDataSourceContent());
                    dataSource = new DataSource(fileSource);
                    break;
                case SOURCE_APP:
                    AppSource appSource = new AppSource(value.getDataSourceContent());
                    dataSource = new DataSource(appSource);
                    break;
                default:
                    throw new CatalogServerException(ErrorCode.DATA_LINEAGE_SOURCE_TYPE_ILLEGAL,
                            value.getDataSourceType().toString());
            }
            if (dataSource != null) {
                List<DataSource> dataInputs = new ArrayList<>();
                dataInputs.add(dataSource);
                TableObject sourceTableRecord = TableObjectHelper
                        .getTableObject(new TableIdent(value.getProjectId(),
                                value.getCatalogId(), value.getDatabaseId(), value.getTableId()));
                TableSource tableSource = new TableSource(projectId,
                        TableObjectConvertHelper.toTableModel(sourceTableRecord));
                DataSource dataOutput = new DataSource();
                dataOutput.setTableSource(tableSource);
                dataOutput.setSourceType(value.getDataSourceType());
                DataLineage dataLineage = new DataLineage();
                dataLineage.setOperation(Operation.valueOf(value.getOperation()));
                dataLineage.setDataOutput(dataOutput);
                dataLineage.setDataInput(dataInputs);
                dataLineageList.add(dataLineage);
            }
        }
        return dataLineageList;
    }

    @Override
    public void updateDataLineage(String projectId, LineageInfoInput lineageInput) {
        Map<String, LineageNode> nodeMap = lineageInput.getNodeMap();
        long createTime = lineageInput.getCreateTime();
        LineageFact jobFact = lineageInput.getJobFact();
        insertLineageJobFact(projectId, jobFact, createTime);
        upsertAndSetNodeId(projectId, nodeMap, createTime, jobFact.getId());
        upsertLineageEdge(projectId, nodeMap, createTime, lineageInput.getRsMap(), jobFact);
    }

    private void upsertLineageEdge(String projectId, Map<String, LineageNode> nodeMap, long createTime,
                                   Map<String, LineageInfoInput.LineageRsInput> rsMap, LineageFact jobFact) {
        if (MapUtils.isNotEmpty(rsMap)) {
            Collection<LineageInfoInput.LineageRsInput> values = rsMap.values();
            List<LineageEdge> list = new ArrayList<>(values.size());
            for (LineageInfoInput.LineageRsInput rsInput : values) {
                if (CollectionUtils.isNotEmpty(rsInput.getUns()) && rsInput.getDn() != null) {
                    for (String upstreamNodeUniqueName : rsInput.getUns()) {
                        list.add(constructLineageEdge(rsInput, nodeMap, upstreamNodeUniqueName, createTime, jobFact));
                    }
                }
            }
            if (list.size() > 0) {
                TransactionRunnerUtil.transactionRun(context -> {
                    dataLineageStore.upsertLineageEdge(context, projectId, list);
                    return null;
                }).getResult();
            }
        }
    }

    private LineageEdge constructLineageEdge(LineageInfoInput.LineageRsInput rsInput, Map<String, LineageNode> nodeMap, String upstreamNodeUniqueName, long createTime, LineageFact jobFact) {
        LineageNode downstreamNode = nodeMap.get(rsInput.getDn());
        LineageNode upstreamNode = nodeMap.get(upstreamNodeUniqueName);
        LineageEdge edge = new LineageEdge();
        edge.setDownstreamId(downstreamNode.getId());
        edge.setUpstreamId(upstreamNode.getId());
        edge.setLineageType(rsInput.getLineageType());
        LineageEdge.EdgeInfo edgeInfo = new LineageEdge.EdgeInfo();
        edgeInfo.setDn(downstreamNode.getQualifiedName());
        edgeInfo.setDot(downstreamNode.getObjectType().shortValue());
        edgeInfo.setDdt(downstreamNode.getDbType().shortValue());
        edgeInfo.setDtf(downstreamNode.getTmpFlag());
        edgeInfo.setUn(upstreamNode.getQualifiedName());
        edgeInfo.setUot(upstreamNode.getObjectType().shortValue());
        edgeInfo.setUdt(upstreamNode.getDbType().shortValue());
        edgeInfo.setUtf(upstreamNode.getTmpFlag());
        edgeInfo.setCt(createTime);
        edgeInfo.setEfi(jobFact.getId());
        edgeInfo.setJs(jobFact.getJobStatus().shortValue());
        edgeInfo.setEu(jobFact.getExecuteUser());
        edgeInfo.setTt(jobFact.getJobType());
        if (rsInput.getParams() != null) {
            edgeInfo.setParams(rsInput.getParams());
        }
        edge.setEdgeInfo(edgeInfo);
        return edge;
    }

    private void insertLineageJobFact(String projectId, LineageFact jobFact, long createTime) {
        if (jobFact == null) {
            throw new CatalogServerException(ErrorCode.LINEAGE_REQ_PARAM_ERROR);
        }
        LineageEdgeFact edgeFact = fact2EdgeFact(jobFact, createTime);
        TransactionRunnerUtil.transactionRun(context -> {
            dataLineageStore.insertLineageEdgeFact(context, projectId, edgeFact);
            return null;
        });
    }

    private LineageEdgeFact fact2EdgeFact(LineageFact jobFact, long createTime) {
        jobFact.setId(UuidUtil.generateUUID32());
        LineageEdgeFact edgeFact = new LineageEdgeFact();
        edgeFact.setId(jobFact.getId());
        edgeFact.setExecuteUser(jobFact.getExecuteUser());
        edgeFact.setJobStatus(jobFact.getJobStatus().shortValue());
        edgeFact.setCluster(jobFact.getCluster());
        edgeFact.setJobId(jobFact.getJobId());
        edgeFact.setJobType(jobFact.getJobType());
        edgeFact.setJobName(jobFact.getJobName());
        edgeFact.setProcessType(jobFact.getProcessType());
        edgeFact.setSql(jobFact.getSql());
        edgeFact.setStartTime(jobFact.getStartTime());
        edgeFact.setCreateTime(createTime);
        edgeFact.setEndTime(jobFact.getEndTime());
        edgeFact.setErrorMsg(jobFact.getErrorMsg());
        if (jobFact.getParams() != null) {
            edgeFact.setParams(jobFact.getParams());
        }
        return edgeFact;
    }

    /**
     * This transaction can be done separately.
     *
     * @param nodeMap
     * @param createTime
     * @param factId
     */
    private void upsertAndSetNodeId(String projectId, Map<String, LineageNode> nodeMap, long createTime, String factId) {
        if (MapUtils.isEmpty(nodeMap)) {
            throw new CatalogServerException(ErrorCode.LINEAGE_REQ_PARAM_ERROR);
        }
        List<LineageNode> nodes = new ArrayList<>(nodeMap.values());
        List<Integer> nodeIdList = getNodeIdList(projectId, nodes, createTime, factId);
        for (int i = 0; i < nodeIdList.size(); i++) {
            nodes.get(i).setId(nodeIdList.get(i));
        }
    }

    private List<Integer> getNodeIdList(String projectId, List<LineageNode> nodes, long createTime, String factId) {
        List<LineageVertex> vertex = new ArrayList<LineageVertex>(nodes.size());
        nodes.forEach(node -> {
            vertex.add(node2Vertex(node, createTime, factId));
        });
        return TransactionRunnerUtil.transactionRun(context -> {
            return dataLineageStore.upsertLineageVertexAndGet(context, projectId, vertex);
        }).getResult();
    }

    private LineageVertex node2Vertex(LineageNode node, long createTime, String factId) {
        LineageVertex vertex = new LineageVertex();
        vertex.setObjectType(node.getObjectType().shortValue());
        vertex.setDbType(node.getDbType().shortValue());
        vertex.setQualifiedName(node.getQualifiedName());
        vertex.setSourceType(node.getSourceType().shortValue());
        vertex.setTmpFlag(node.getTmpFlag());
        vertex.setCreateTime(createTime);
        vertex.setFactId(factId);
        if (node.getParams() != null) {
            vertex.setParams(node.getParams());
        }
        return vertex;
    }

    @Override
    public LineageFact getLineageJobFact(String projectId, String jobFactId) {
        if (jobFactId == null || jobFactId.length() != 32) {
            throw new CatalogServerException(ErrorCode.INVALID_OBJECT, jobFactId);
        }
        LineageEdgeFact lineageEdgeFact = TransactionRunnerUtil.transactionReadRunThrow(context -> {
            return dataLineageStore.getLineageEdgeFact(context, projectId, jobFactId);
        }).getResult();
        return transLineageFact(lineageEdgeFact);
    }

    private LineageFact transLineageFact(LineageEdgeFact lineageEdgeFact) {
        if (lineageEdgeFact == null) {
            return null;
        }
        LineageFact lineageFact = new LineageFact();
        lineageFact.setId(lineageEdgeFact.getId());
        lineageFact.setJobStatus(EJobStatus.forNum(lineageEdgeFact.getJobStatus()));
        lineageFact.setStartTime(lineageEdgeFact.getStartTime());
        lineageFact.setEndTime(lineageEdgeFact.getEndTime());
        lineageFact.setCreateTime(lineageEdgeFact.getCreateTime());
        lineageFact.setExecuteUser(lineageEdgeFact.getExecuteUser());
        lineageFact.setJobType(lineageEdgeFact.getJobType());
        lineageFact.setJobId(lineageEdgeFact.getJobId());
        lineageFact.setJobName(lineageEdgeFact.getJobName());
        lineageFact.setProcessType(lineageEdgeFact.getProcessType());
        lineageFact.setErrorMsg(lineageEdgeFact.getErrorMsg());
        lineageFact.setSql(lineageEdgeFact.getSql());
        lineageFact.setCluster(lineageEdgeFact.getCluster());
        lineageFact.setParams(lineageEdgeFact.getParamsBean());
        return lineageFact;
    }

    @Override
    public LineageInfo getLineageGraph(String projectId, EDbType dbType, ELineageObjectType objectType, String qualifiedName, int depth,
                                       ELineageDirection lineageDirection, ELineageType lineageType, Long startTime) {
        if (depth > maxLineageDepth) {
            throw new CatalogServerException(ErrorCode.INVALID_PARAMS_EXCEED_LIMIT, "depth", maxLineageDepth);
        }
        LineageInfo lineageInfo = new LineageInfo();
        List<LineageEdge> edgeList = TransactionRunnerUtil.transactionReadRunThrow(context -> {
            LineageVertex vertex = dataLineageStore.getLineageVertex(context, projectId, dbType.getNum(), objectType.getNum(), qualifiedName);
            if (vertex == null) {
                throw new CatalogServerException(ErrorCode.INVALID_OBJECT, String.format("Lineage search base node: DBType=%s, ObjectType=%s, qualifiedName=%s", dbType, objectType, qualifiedName));
            }
            lineageInfo.setBaseNode(vertex2LineageNode(vertex));
            return dataLineageStore.getLineageGraph(context, projectId, vertex.getId(), depth, lineageDirection, lineageType, startTime);
        }).getResult();
        setLineageInfoNodesRs(edgeList, lineageInfo);
        return lineageInfo;
    }

    private void setLineageInfoNodesRs(List<LineageEdge> edgeList, LineageInfo lineageInfo) {
        if (CollectionUtils.isNotEmpty(edgeList)) {
            Set<Integer> set = new HashSet<>(edgeList.size() * 2);
            for (LineageEdge edge : edgeList) {
                if (!set.contains(edge.getDownstreamId())) {
                    lineageInfo.addNode(edge2DownstreamNode(edge));
                    set.add(edge.getDownstreamId());
                }
                if (!set.contains(edge.getUpstreamId())) {
                    lineageInfo.addNode(edge2UpstreamNode(edge));
                    set.add(edge.getUpstreamId());
                }
                lineageInfo.addRelation(edge2Relationship(edge));
            }
        }
    }

    private LineageInfo.LineageRs edge2Relationship(LineageEdge edge) {
        LineageInfo.LineageRs rs = new LineageInfo.LineageRs();
        rs.setUpId(edge.getUpstreamId());
        rs.setDownId(edge.getDownstreamId());
        rs.setExecuteUser(edge.getEdgeInfoBean().getEu());
        rs.setJobFactId(edge.getEdgeInfoBean().getEfi());
        rs.setLineageType(edge.getLineageType());
        rs.setJobStatus(Integer.valueOf(edge.getEdgeInfoBean().getJs()));
        if (edge.getEdgeInfoBean().getParams() != null) {
            rs.setParams(edge.getEdgeInfoBean().getParams());
        }
        return rs;
    }

    private LineageNode edge2UpstreamNode(LineageEdge edge) {
        LineageEdge.EdgeInfo edgeInfo = edge.getEdgeInfoBean();
        LineageNode node = new LineageNode(edgeInfo.getUn(), EDbType.forNum(edgeInfo.getUdt()), ELineageObjectType.forNum(edgeInfo.getUot()), edgeInfo.getUtf());
        node.setId(edge.getUpstreamId());
        return node;
    }

    private LineageNode edge2DownstreamNode(LineageEdge edge) {
        LineageEdge.EdgeInfo edgeInfo = edge.getEdgeInfoBean();
        LineageNode node = new LineageNode(edgeInfo.getDn(), EDbType.forNum(edgeInfo.getDdt()), ELineageObjectType.forNum(edgeInfo.getDot()), edgeInfo.getDtf());
        node.setId(edge.getDownstreamId());
        return node;
    }

    private LineageNode vertex2LineageNode(LineageVertex vertex) {
        LineageNode baseNode = new LineageNode(vertex.getQualifiedName(), EDbType.forNum(vertex.getDbType()),
                ELineageObjectType.forNum(vertex.getObjectType()), ELineageSourceType.forNum(vertex.getSourceType()),
                vertex.isTmpFlag());
        baseNode.setId(vertex.getId());
        baseNode.setParams(vertex.getParamsBean());
        return baseNode;
    }
}
