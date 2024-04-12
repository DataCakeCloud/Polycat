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
package io.polycat.catalog.store.api;

import java.util.List;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.lineage.ELineageDirection;
import io.polycat.catalog.common.lineage.ELineageType;
import io.polycat.catalog.common.model.*;

public interface DataLineageStore extends SubspaceStore  {

    void upsertDataLineage(TransactionContext ctx, DataLineageObject dataLineageObject) throws MetaStoreException;

    List<DataLineageObject> listDataLineageByTableId(TransactionContext ctx, String projectId, String catalogId,
        String databaseId, String tableId) throws MetaStoreException;

    List<DataLineageObject> listDataLineageByDataSource(TransactionContext ctx, String projectId,
        DataSourceType dataSourceType, String dataSourceContent) throws MetaStoreException;

    List<Integer> upsertLineageVertexAndGet(TransactionContext context, String projectId, List<LineageVertex> vertex);

    void insertLineageEdgeFact(TransactionContext context, String projectId, LineageEdgeFact edgeFact);

    void upsertLineageEdge(TransactionContext context, String projectId, List<LineageEdge> list);

    LineageEdgeFact getLineageEdgeFact(TransactionContext context, String projectId, String jobFactId);

    LineageVertex getLineageVertex(TransactionContext context, String projectId, int dbType, int objectType, String qualifiedName);

    List<LineageEdge> getLineageGraph(TransactionContext context, String projectId, Integer nodeId, int depth, ELineageDirection lineageDirection, ELineageType lineageType, Long startTime);
}
