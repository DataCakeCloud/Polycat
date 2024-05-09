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
package io.polycat.catalog.store.mapper;

import io.polycat.catalog.common.model.LineageEdge;
import io.polycat.catalog.common.model.LineageEdgeFact;
import io.polycat.catalog.common.model.LineageVertex;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface DataLineageMapper {
    /**
     * lineage subspace
     * @param projectId projectId
     */
    void createSubspace(@Param("projectId") String projectId);

    /**
     * lineage search function
     */
    void createOrReplaceLineageSearchFunction();

    /**
     * insert or update vertex
     *
     * @param projectId projectId
     * @param list data
     * @return
     */
    int upsertVertexAndGetId(@Param("projectId") String projectId, @Param("list") List<LineageVertex> list);

    /**
     * save lineage edge fact
     * @param projectId projectId
     * @param edgeFact edgeFact
     */
    void insertLineageEdgeFact(@Param("projectId") String projectId, @Param("data") LineageEdgeFact edgeFact);

    /**
     * insert or update edge
     *
     * @param projectId projectId
     * @param list list
     */
    void upsertLineageEdge(@Param("projectId") String projectId, @Param("list") List<LineageEdge> list);

    /**
     * get lineage edge fact
     *
     * @param projectId projectId
     * @param jobFactId jobFactId
     * @return
     */
    LineageEdgeFact getLineageEdgeFact(@Param("projectId") String projectId, @Param("factId") String jobFactId);

    LineageVertex getLineageVertex(@Param("projectId") String projectId, @Param("filter") String filter);

    List<LineageEdge> getLineageGraph(@Param("tableName") String tableName, @Param("nodeId") Integer nodeId, @Param("depth") int depth,
                                      @Param("lineageDirection") int lineageDirection, @Param("lineageType") int lineageType,
                                      @Param("startTime") Long startTime, @Param("lineageTypeFlag") boolean lineageTypeFlag,
                                      @Param("startTimeFlag") boolean startTimeFlag, @Param("limit") int limit);
}
