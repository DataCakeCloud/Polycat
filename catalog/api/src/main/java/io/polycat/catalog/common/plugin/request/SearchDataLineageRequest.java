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
package io.polycat.catalog.common.plugin.request;

import io.polycat.catalog.common.plugin.request.base.ProjectRequestBase;
import io.polycat.catalog.common.lineage.EDbType;
import io.polycat.catalog.common.lineage.ELineageDirection;
import io.polycat.catalog.common.lineage.ELineageObjectType;
import io.polycat.catalog.common.lineage.ELineageType;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class SearchDataLineageRequest extends ProjectRequestBase<Void> {

    private EDbType dbType;
    private ELineageObjectType objectType;
    private String qualifiedName;
    private int depth;
    private ELineageDirection direction;
    private ELineageType lineageType;
    private Long startTime;

    public SearchDataLineageRequest() {
        super();
    }

    /**
     * search lineage graph
     * @param projectId projectId
     * @param dbType Lineage DB vendor type {@link EDbType}
     * @param objectType Lineage object type {@link ELineageObjectType}
     * @param qualifiedName qualified name TABLE: catalogName.databaseName.tableName
     * @param depth number of hops for lineage
     * @param direction UPSTREAM, DOWNSTREAM or BOTH {@link ELineageDirection}
     * @param lineageType lineage type {@link ELineageType}
     * @param startTime lineage start time position.
     */
    public SearchDataLineageRequest(String projectId, EDbType dbType, ELineageObjectType objectType, String qualifiedName,
                                    int depth, ELineageDirection direction, ELineageType lineageType, Long startTime) {
        this.projectId = projectId;
        this.dbType = dbType;
        this.objectType = objectType;
        this.qualifiedName = qualifiedName;
        this.depth = depth;
        this.direction = direction;
        this.lineageType = lineageType;
        this.startTime = startTime;
    }

    public SearchDataLineageRequest(String projectId, EDbType dbType, ELineageObjectType objectType, String qualifiedName,
                                    int depth, ELineageDirection direction, ELineageType lineageType) {
        this.projectId = projectId;
        this.dbType = dbType;
        this.objectType = objectType;
        this.qualifiedName = qualifiedName;
        this.depth = depth;
        this.direction = direction;
        this.lineageType = lineageType;
    }

}
