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
package io.polycat.catalog.common.plugin;

import io.polycat.catalog.common.model.User;

import lombok.Data;

@Data
public class CatalogContext {

    String projectId;

    String currentCatalogName = "default";

    String currentDatabaseName = "default";

    String defaultCatalogName;

    String userName;

    String tenantName;

    String authSourceType;

    String token;

    public CatalogContext() {
        defaultCatalogName = "default_catalog";
    }

    public CatalogContext(String projectId, String userName, String tenantName, String token,
        String defaultCatalogName) {
        this(projectId, "default", "default", defaultCatalogName, userName, tenantName, token);
    }

    public CatalogContext(String projectId, String userName, String tenantName, String token) {
        this(projectId, "default", "default", "default_catalog", userName, tenantName, token);
    }

    public CatalogContext(String projectId, String currentCatalogName, String currentDatabaseName,
        String defaultCatalogName, String userName, String tenantName, String token) {
        this.projectId = projectId;
        this.currentCatalogName = currentCatalogName;
        this.currentDatabaseName = currentDatabaseName;
        this.defaultCatalogName = defaultCatalogName;
        this.userName = userName;
        this.tenantName = tenantName;
        this.token = token;
    }

    public User getUser() {
        return new User(tenantName, userName);
    }

    public String projectId(String projectId) {
        return projectId == null ? this.projectId : projectId;
    }

    public String currentCatalogName(String catalogName) {
        return catalogName == null ? currentCatalogName : catalogName;
    }

    public String currentDatabaseName(String databaseName) {
        return databaseName == null ? currentDatabaseName : databaseName;
    }

    public String getTenantName() {
        return tenantName;
    }

    public String getAuthSourceType() {
        return authSourceType;
    }
}
