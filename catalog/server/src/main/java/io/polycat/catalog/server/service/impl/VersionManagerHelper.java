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

import io.polycat.catalog.server.util.TransactionFrameRunner;
import io.polycat.catalog.server.util.TransactionRunner;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.store.api.CatalogStore;
import io.polycat.catalog.store.api.VersionManager;
import io.polycat.catalog.store.common.StoreConvertor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class VersionManagerHelper {
    private static VersionManager versionManager;
    private static CatalogStore catalogStore;

    @Autowired
    public void setVersionManager(VersionManager versionManager) {
        this.versionManager = versionManager;
    }

    @Autowired
    public void setCatalogStore(CatalogStore catalogStore) {
        this.catalogStore = catalogStore;
    }

    public static String getNextVersion(TransactionContext context, String projectId, String rootCatalogId) {
        return getLatestVersion(context, projectId, rootCatalogId);
    }

    public static String getLatestVersion(TransactionContext context, String projectId, String rootCatalogId) {
        return versionManager.getLatestVersion(context, projectId, rootCatalogId);
    }

    public static String getLatestVersionByName(TransactionContext context, String projectId, String name) {
        CatalogName catalogName = StoreConvertor.catalogName(projectId, name);
        CatalogObject catalogObject = catalogStore.getCatalogByName(context, catalogName);
        return versionManager.getLatestVersion(context, projectId, catalogObject.getRootCatalogId());
    }

    // only for test
    public static String getLatestVersionByName(String projectId, String catalogName) {
        String lastVersion;
        try (TransactionRunner runner = new TransactionRunner()) {
            runner.setMaxAttempts(16);
            lastVersion = runner.run(context -> getLatestVersionByName(context, projectId, catalogName));
        }
        return lastVersion;
    }

    // only for read
    public static String getLatestVersion(TableIdent tableIdent) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(16);
        return runner.run(context -> getLatestVersion(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId())).getResult();
    }

    public static String getLatestVersion(TransactionContext context, TableIdent tableIdent) {
        return getLatestVersion(context, tableIdent.getProjectId(),
                tableIdent.getCatalogId());
    }

}
