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
import io.polycat.catalog.server.util.TransactionRunnerUtil;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.service.api.CatalogResourceService;
import io.polycat.catalog.store.api.*;
import io.polycat.catalog.util.CheckUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class CatalogResourceServiceImpl implements CatalogResourceService {

    private static final Logger log = Logger.getLogger(CatalogResourceServiceImpl.class);

    @Autowired
    private ResourceStore resourceStore;

    @Autowired
    private CatalogStore catalogStore;

    @Autowired
    private DatabaseStore databaseStore;

    @Autowired
    private TableDataStore tableDataStore;

    @Autowired
    private FunctionStore functionStore;

    @Autowired
    private List<SubspaceStore> createSubSpace;

    @Autowired
    private TableMetaStore tableMetaStore;

    @Autowired
    private UserPrivilegeStore userPrivilegeStore;

    @Autowired
    private RoleStore roleStore;

    @Autowired
    private ObjectNameMapStore objectNameMapStore;

    private static Set<String> projectIds =  new HashSet<String>();

    @Override
    public void init() {
        // init projectIds
        CatalogResourceServiceImpl.projectIds.addAll(listProjects());
    }

    /**
     * TODO Resource operation, in the future, it needs to go out separately for separate management
     * @param context
     * @param projectId
     */
    private void createResourceInner(TransactionContext context, String projectId) {
        resourceStore.createResource(context, projectId);

        // catalog subspace create
        catalogStore.createCatalogSubspace(context, projectId);
        catalogStore.createCatalogCommitSubspace(context, projectId);
        catalogStore.createCatalogHistorySubspace(context, projectId);
        catalogStore.createBranchSubspace(context, projectId);

        // database subspace create
        databaseStore.createDatabaseSubspace(context, projectId);
        databaseStore.createDroppedDatabaseNameSubspace(context, projectId);
        databaseStore.createDatabaseHistorySubspace(context, projectId);

        functionStore.createFunctionSubspace(context, projectId);

        // table subspace create
        tableDataStore.createTableHistorySubspace(context, projectId);
        tableDataStore.createTableDataPartitionSet(context, projectId);
        tableDataStore.createTableIndexHistorySubspace(context, projectId);
        tableDataStore.createTableIndexSubspace(context, projectId);
        tableMetaStore.createTableBaseHistorySubspace(context, projectId);
        tableMetaStore.createTableSchemaHistorySubspace(context, projectId);
        tableMetaStore.createTableStorageHistorySubspace(context, projectId);
        tableMetaStore.createTableCommitSubspace(context, projectId);
        tableDataStore.createColumnStatisticsSubspace(context, projectId);

        userPrivilegeStore.createUserPrivilegeSubspace(context, projectId);
        roleStore.createRoleSubspace(context, projectId);
        objectNameMapStore.createObjectNameMapSubspace(context, projectId);

        //TODO all store should impl CreateSubSpaceStore
        createSubSpace.forEach(store -> store.createSubspace(context, projectId));
    }

    @Override
    public void createResource(String projectId) {
        CheckUtil.checkNameLegality("projectId", projectId);
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.run(context -> {
            createResourceInner(context, projectId);
            return null;
        }).getResult();
        projectIds.add(projectId);
    }

    @Override
    public Boolean doesExistResource(String projectId) {
        CheckUtil.checkNameLegality("projectId", projectId);
        TransactionFrameRunner runner = new TransactionFrameRunner();
        return runner.run(context -> {
            return resourceStore.doesExistResource(context, projectId);
        }).getResult();
    }

    /**
     * projectId exists
     *
     * @param projectId projectId
     * @return
     */
    @Override
    public Boolean doesExistsProjectId(String projectId) {
        return projectIds.contains(projectId);
    }

    private void dropResourceInner(TransactionContext context, String projectId) {
        userPrivilegeStore.dropUserPrivilegeSubspace(context, projectId);
        roleStore.dropRoleSubspace(context, projectId);
        catalogStore.dropCatalogSubspace(context, projectId);
        objectNameMapStore.dropObjectNameMapSubspace(context, projectId);
        resourceStore.dropResource(context, projectId);

    }

    @Override
    public void dropResource(String projectId) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.run(context -> {
            dropResourceInner(context, projectId);
            return null;
        }).getResult();
    }

    @Override
    public void resourceCheck() {
        TransactionRunnerUtil.transactionRunThrow(context -> {
            resourceStore.resourceCheck(context);
            return null;
        }).getResult();
    }

    @Override
    public List<String> listProjects() {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            return resourceStore.listProjects(context);
        }).getResult();
    }
}
