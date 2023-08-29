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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import io.polycat.catalog.store.protos.common.DatabaseInfo;
import lombok.Data;

@Data
public class DatabaseObject {
    private String name = "";

    private String projectId = "";
    private String catalogId = "";
    private String databaseId = "";
    private Map<String, String> properties = Collections.emptyMap();
    private String location = "";
    private long createTime;
    private long droppedTime;
    private String description = "";
    private String userId = "";
    private String ownerType = "";

    public DatabaseObject() {
        this.properties = new LinkedHashMap<>();
    }

    public DatabaseObject(DatabaseObject src) {
        this.name = src.getName();
        this.projectId = src.getProjectId();
        this.catalogId = src.getCatalogId();
        this.databaseId = src.getDatabaseId();
        this.properties = new LinkedHashMap<>();
        this.properties.putAll(src.getProperties());
        this.location = src.getLocation();
        this.createTime = src.getCreateTime();
        this.droppedTime = src.getDroppedTime();
        this.description = src.getDescription();
        this.userId = src.getUserId();
        this.ownerType = src.getOwnerType();
    }

    public DatabaseObject(DatabaseIdent databaseIdent, DatabaseHistoryObject databaseHistory) {
        this.name = databaseHistory.getName();
        this.projectId = databaseIdent.getProjectId();
        this.catalogId = databaseIdent.getCatalogId();
        this.databaseId = databaseIdent.getDatabaseId();
        this.properties.putAll(databaseHistory.getProperties());
        this.location = databaseHistory.getLocation();
        this.createTime = databaseHistory.getCreateTime();
        this.droppedTime = databaseHistory.getDroppedTime();
        this.description = databaseHistory.getDescription();
        this.userId = databaseHistory.getUserId();
    }

    public DatabaseObject(DatabaseIdent databaseIdent, String databaseName, DatabaseInfo databaseInfo) {
        this.name = databaseName;
        this.projectId = databaseIdent.getProjectId();
        this.catalogId = databaseIdent.getCatalogId();
        this.databaseId = databaseIdent.getDatabaseId();
        this.properties = new LinkedHashMap<>();
        this.properties.putAll(databaseInfo.getPropertiesMap());
        this.location = databaseInfo.getLocation();
        this.createTime = databaseInfo.getCreateTime();
        this.description = databaseInfo.getDescription();
        this.userId = databaseInfo.getUserId();
        this.ownerType = databaseInfo.getOwnerType();
    }
}
