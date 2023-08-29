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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.AcceleratorObject;
import io.polycat.catalog.common.model.AcceleratorPropertiesObject;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.DatabaseObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.SqlTemplate;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.AcceleratorInput;
import io.polycat.catalog.common.utils.PathUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.service.api.AcceleratorService;
import io.polycat.catalog.store.api.AcceleratorStore;
import io.polycat.catalog.store.api.Transaction;
import io.polycat.catalog.store.common.StoreConvertor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class AcceleratorServiceImpl implements AcceleratorService {
    @Autowired
    private Transaction storeTransaction;
    @Autowired
    private AcceleratorStore acceleratorStore;

    @Override
    public void createAccelerator(DatabaseName database, AcceleratorInput acceleratorInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, database);
            if (null == databaseIdent) {
                throw new CatalogServerException(ErrorCode.DATABASE_NOT_FOUND, database.getDatabaseName());
            }

            String acceleratorId = UuidUtil.generateId();
            if(acceleratorStore.acceleratorObjectNameExist(context, databaseIdent, acceleratorInput.getName())) {
                throw new CatalogServerException(ErrorCode.ACCELERATOR_ALREADY_EXISTS, acceleratorInput.getName());
            }

            DatabaseObject databaseRecord = DatabaseObjectHelper.getDatabaseObject(context, databaseIdent);
            String location = PathUtil.tablePath(databaseRecord.getLocation(), acceleratorInput.getName(), acceleratorId);
            acceleratorStore.insertAcceleratorObjectName(context, databaseIdent, acceleratorInput.getName(),
                acceleratorId);
            acceleratorStore.insertAcceleratorProperties(context, database.getProjectId(), acceleratorId,
                acceleratorInput, location);
            acceleratorStore.insertTemplates(context, database.getProjectId(), acceleratorId,
                acceleratorInput.getSqlTemplateList());
            context.commit();
        }
    }

    @Override
    public List<AcceleratorObject> showAccelerators(DatabaseName database) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, database);
            if (null == databaseIdent) {
                throw new MetaStoreException(ErrorCode.DATABASE_NOT_FOUND);
            }

            List<AcceleratorPropertiesObject> acceleratorPropertiesObjectList =
                acceleratorStore.listAcceleratorProperties(context, databaseIdent);
            List<AcceleratorObject> acceleratorObjectList = new ArrayList<>(acceleratorPropertiesObjectList.size());
            for (AcceleratorPropertiesObject acceleratorProperties : acceleratorPropertiesObjectList) {
                List<SqlTemplate> sqlTemplateList = acceleratorStore.listAcceleratorTemplate(context, database.getProjectId(),
                    acceleratorProperties.getAcceleratorId());
                acceleratorObjectList.add(new AcceleratorObject(acceleratorProperties.getLocation(),
                    acceleratorProperties.getLib(), acceleratorProperties.getSqlStatement(),
                    acceleratorProperties.isCompiled(), sqlTemplateList));
            }
            context.commit();
            fillNameInfo(database, acceleratorObjectList);
            return acceleratorObjectList;
        }
    }

    @Override
    public void dropAccelerators(String projectId, String catalogName, String databaseName, String acceleratorName) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            CatalogName catalogNameObject = StoreConvertor
                .catalogName(projectId, catalogName);
            DatabaseName databaseNameObject = StoreConvertor.databaseName(catalogNameObject, databaseName);
            DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseNameObject);
            if (databaseIdent == null) {
                throw new CatalogServerException(ErrorCode.DATABASE_NOT_FOUND, databaseName);
            }

            String acceleratorId = acceleratorStore.getAcceleratorId(context, databaseIdent, acceleratorName);
            if (acceleratorId == null) {
                throw new CatalogServerException(ErrorCode.ACCELERATOR_ALREADY_EXISTS, acceleratorName);
            }

            acceleratorStore.deleteAcceleratorObjectName(context, databaseIdent, acceleratorName);
            acceleratorStore.deleteAcceleratorProperties(context, projectId, acceleratorId);
            acceleratorStore.deleteAcceleratorTemplateAll(context, projectId, acceleratorId);

            context.commit();
        }
    }

    @Override
    public void alterAccelerator(String projectId, String catalogName, String databaseName, String acceleratorName,
        AcceleratorInput acceleratorInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            // find database
            CatalogName catalogNameObject = StoreConvertor
                .catalogName(projectId, catalogName);
            DatabaseName databaseNameObject = StoreConvertor.databaseName(catalogNameObject, databaseName);
            DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseNameObject);
            if (databaseIdent == null) {
                throw new CatalogServerException(ErrorCode.DATABASE_NOT_FOUND, databaseName);
            }
            // find accelerator
            String acceleratorId =
                acceleratorStore.getAcceleratorId(context, databaseIdent, acceleratorName);
            // update sql properties status to be compiled
            acceleratorStore.updateAcceleratorPropertiesCompiled(context, projectId, acceleratorId, true);
            //// update sql template status to be compiled
            acceleratorInput.getSqlTemplateList()
                .forEach(x -> acceleratorStore.updateTemplateStatusCompiled(context, projectId, acceleratorId, x));

            context.commit();
        }
    }

    private void fillNameInfo(DatabaseName database, List<AcceleratorObject> acceleratorList) {
        Map<String, DatabaseObject> databaseRecordMap = new HashMap<>();
        Map<String, String> tableDescriptorMap = new HashMap<>();
        for (AcceleratorObject accelerator : acceleratorList) {
            accelerator.setCatalogName(database.getCatalogName());
            accelerator.setDatabaseName(database.getDatabaseName());
            for (SqlTemplate sqlTemplate : accelerator.getSqlTemplateList()) {
                DatabaseObject databaseRecord = databaseRecordMap.computeIfAbsent(sqlTemplate.getDatabaseId(), x ->
                    DatabaseObjectHelper.getDatabaseObject(
                        new DatabaseIdent(database.getProjectId(), sqlTemplate.getCatalogId(),
                            sqlTemplate.getDatabaseId())));
                String tableName = tableDescriptorMap.computeIfAbsent(sqlTemplate.getTableId(), x ->
                    TableObjectHelper.getTableName(new TableIdent(database.getProjectId(),
                        sqlTemplate.getCatalogId(), sqlTemplate.getDatabaseId(),
                        sqlTemplate.getTableId()), false));
                sqlTemplate.setCatalogName(database.getCatalogName());
                sqlTemplate.setDatabaseName(databaseRecord.getName());
                sqlTemplate.setTableName(tableName);
            }
        }
    }
}
