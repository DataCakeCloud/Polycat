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

import lombok.Data;


@Data
public class TableObject {
    private String projectId = "";
    private String catalogId = "";
    private String databaseId = "";
    private String tableId = "";
    private String catalogName = "";
    private String databaseName = "";
    private String name = "";
    private int historySubspaceFlag = 0;
    private TableBaseObject tableBaseObject;
    private TableStorageObject tableStorageObject;
    private TableSchemaObject tableSchemaObject;
    private long droppedTime = 0;

    public TableObject(TableIdent tableIdent, TableName tableName, int historySubspaceFlag,
        TableBaseObject tableBaseObject, TableSchemaObject tableSchemaObject,
        TableStorageObject tableStorageObject, long droppedTime) {
        this.projectId = tableIdent.getProjectId();
        this.catalogId = tableIdent.getCatalogId();
        this.databaseId = tableIdent.getDatabaseId();
        this.tableId = tableIdent.getTableId();
        this.catalogName = tableName.getCatalogName();
        this.databaseName = tableName.getDatabaseName();
        this.name = tableName.getTableName();
        this.historySubspaceFlag = historySubspaceFlag;
        this.tableBaseObject = tableBaseObject;
        this.tableStorageObject = tableStorageObject;
        this.tableSchemaObject = tableSchemaObject;
        this.droppedTime = droppedTime;
    }

    public TableObject(TableObject src) {
        this.projectId = src.getProjectId();
        this.catalogId = src.getCatalogId();
        this.databaseId = src.getDatabaseId();
        this.tableId = src.getTableId();
        this.catalogName = src.getCatalogName();
        this.databaseName = src.getDatabaseName();
        this.name = src.getName();
        this.historySubspaceFlag = src.historySubspaceFlag;
        this.tableStorageObject = src.getTableStorageObject();
        this.tableSchemaObject = src.getTableSchemaObject();
        this.tableStorageObject = src.getTableStorageObject();
        this.droppedTime = src.getDroppedTime();
    }

    public TableObject(String name) {
        this.name = name;
    }

}
