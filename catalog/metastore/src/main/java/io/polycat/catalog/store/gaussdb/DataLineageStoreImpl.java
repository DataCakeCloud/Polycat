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
package io.polycat.catalog.store.gaussdb;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.store.ResourceStoreHelper;
import io.polycat.catalog.store.api.DataLineageStore;
import io.polycat.catalog.store.common.StoreSqlConvertor;
import io.polycat.catalog.store.gaussdb.common.MetaTableConsts;
import io.polycat.catalog.store.mapper.DataLineageMapper;
import io.polycat.catalog.util.ResourceUtil;
import io.polycat.catalog.common.lineage.ELineageDirection;
import io.polycat.catalog.common.lineage.ELineageType;
import io.polycat.catalog.common.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.stream.Collectors;


@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class DataLineageStoreImpl implements DataLineageStore {

    private static final int LINEAGE_RELATION_SEARCH_MAX_NUM = 500;
    @Autowired
    private DataLineageMapper dataLineageMapper;

    @Override
    public void upsertDataLineage(TransactionContext ctx, DataLineageObject dataLineageObject) throws MetaStoreException {

    }

    @Override
    public List<DataLineageObject> listDataLineageByTableId(TransactionContext ctx, String projectId, String catalogId, String databaseId, String tableId) throws MetaStoreException {
        return null;
    }

    @Override
    public List<DataLineageObject> listDataLineageByDataSource(TransactionContext ctx, String projectId, DataSourceType dataSourceType, String dataSourceContent) throws MetaStoreException {
        return null;
    }

    @Override
    public List<Integer> upsertLineageVertexAndGet(TransactionContext context, String projectId, List<LineageVertex> vertexs) {
        dataLineageMapper.upsertVertexAndGetId(projectId, vertexs);
        return vertexs.stream().map(LineageVertex::getId).collect(Collectors.toList());
    }

    @Override
    public void insertLineageEdgeFact(TransactionContext context, String projectId, LineageEdgeFact edgeFact) {
        dataLineageMapper.insertLineageEdgeFact(projectId, edgeFact);
    }

    @Override
    public void upsertLineageEdge(TransactionContext context, String projectId, List<LineageEdge> list) {
        dataLineageMapper.upsertLineageEdge(projectId, list);
    }

    @Override
    public LineageEdgeFact getLineageEdgeFact(TransactionContext context, String projectId, String jobFactId) {
        return dataLineageMapper.getLineageEdgeFact(projectId, jobFactId);
    }

    @Override
    public LineageVertex getLineageVertex(TransactionContext context, String projectId, int dbType, int objectType, String qualifiedName) {
        String filterSql = StoreSqlConvertor.get().equals(LineageVertex.Fields.dbType, dbType).AND()
                .equals(LineageVertex.Fields.objectType, objectType).AND()
                .equals(LineageVertex.Fields.qualifiedName, qualifiedName).getFilterSql();
        return dataLineageMapper.getLineageVertex(projectId, filterSql);
    }

    @Override
    public List<LineageEdge> getLineageGraph(TransactionContext context, String projectId, Integer nodeId, int depth, ELineageDirection lineageDirection, ELineageType lineageType, Long startTime) {
        boolean lineageTypeFlag = false;
        boolean startTimeFlag = false;
        int lineageTypeNum = -1;
        if (lineageDirection == null) {
            lineageDirection = ELineageDirection.BOTH;
        }
        if (lineageType == null) {
            lineageTypeFlag = true;
        } else {
            lineageTypeNum = lineageType.getNum();
        }
        if (startTime == null || startTime <= 0) {
            startTime = 0L;
            startTimeFlag = true;
        }
        return dataLineageMapper.getLineageGraph(
                ResourceUtil.getTableQName(projectId, MetaTableConsts.LINEAGE_EDGE), nodeId, depth,
                lineageDirection.getNum(), lineageTypeNum, startTime,
                lineageTypeFlag, startTimeFlag, LINEAGE_RELATION_SEARCH_MAX_NUM);
    }

    @Override
    public void createSubspace(TransactionContext context, String projectId) {
        if (!ResourceStoreHelper.doesExistsTable(context, projectId, MetaTableConsts.LINEAGE_EDGE)) {
            dataLineageMapper.createSubspace(projectId);
        }
        if (!ResourceStoreHelper.doesExistsFunction(context, MetaTableConsts.FUNC_LINEAGE_GRAPH_SEARCH)) {
            dataLineageMapper.createOrReplaceLineageSearchFunction();
        }
    }
}
