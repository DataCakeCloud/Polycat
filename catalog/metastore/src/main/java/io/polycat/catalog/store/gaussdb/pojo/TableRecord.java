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
package io.polycat.catalog.store.gaussdb.pojo;

import io.polycat.catalog.common.model.TableIdent;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableRecord {
    private String projectId;
    private String catalogId;
    private String databaseId;
    private String tableId;
    private String tableName;
    private int historySubspaceFlag;
    private byte[] base;
    private byte[] schema;
    private byte[] storage;

    public TableRecord(TableIdent tableIdent, String tableName, int historySubspaceFlag,
        byte[] base, byte[] schema, byte[] storage) {
        this.projectId = tableIdent.getProjectId();
        this.catalogId = tableIdent.getCatalogId();
        this.databaseId = tableIdent.getDatabaseId();
        this.tableId = tableIdent.getTableId();
        this.tableName = tableName;
        this.historySubspaceFlag = historySubspaceFlag;
        this.base = base;
        this.schema = schema;
        this.storage = storage;
    }

}
