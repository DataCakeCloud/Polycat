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
package io.polycat.catalog.common.model;

import io.polycat.catalog.common.model.TableIdent;

import lombok.Data;

@Data
public class DataLineageObject {
    private String projectId;
    private String catalogId;
    private String databaseId;
    private String tableId;
    private DataSourceType dataSourceType;
    private String dataSourceContent;
    private String operation;

    public DataLineageObject(TableIdent tableIdent, DataSourceType dataSourceType, String dataSourceContent,
        String operation) {
        this(tableIdent.getProjectId(), tableIdent.getCatalogId(), tableIdent.getDatabaseId(),
            tableIdent.getTableId(), dataSourceType, dataSourceContent, operation);
    }

    public DataLineageObject(String projectId, String catalogId, String databaseId, String tableId,
        DataSourceType dataSourceType, String dataSourceContent, String operation) {
        this.projectId = projectId;
        this.catalogId = catalogId;
        this.databaseId = databaseId;
        this.tableId = tableId;
        this.dataSourceType = dataSourceType;
        this.dataSourceContent = dataSourceContent;
        this.operation = operation;
    }
}

