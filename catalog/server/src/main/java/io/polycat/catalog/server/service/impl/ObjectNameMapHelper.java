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

import java.util.Map;
import java.util.Optional;

import io.polycat.catalog.server.util.TransactionRunner;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.MetaObjectName;
import io.polycat.catalog.common.model.ObjectNameMap;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.CatalogStore;
import io.polycat.catalog.store.api.ObjectNameMapStore;
import io.polycat.catalog.store.api.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class ObjectNameMapHelper {

    private static CatalogStore catalogStore;
    private static ObjectNameMapStore objectNameMapStore;
    private static Transaction transaction;

    public static final String LMS_KEY = "lms_name";

    @Autowired
    public void setObjectNameMapStore(ObjectNameMapStore objectNameMapStore) {
        this.objectNameMapStore = objectNameMapStore;
    }

    @Autowired
    public void setCatalogStore(CatalogStore catalogStore) {
        this.catalogStore = catalogStore;
    }

    @Autowired
    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public static Optional<MetaObjectName> getObjectFromNameMap(String projectId, String objectType,
                                                                String databaseName, String objectName) {
        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
                Optional<ObjectNameMap> objectNameMap = objectNameMapStore.getObjectNameMap(context, projectId,
                        ObjectType.valueOf(objectType), databaseName, objectName);
                return objectNameMap.map(objectMap -> {
                    CatalogIdent catalogIdent = new CatalogIdent(objectMap.getProjectId(), objectMap.getTopObjectId());
                    CatalogObject catalogRecord = catalogStore.getCatalogById(context, catalogIdent);

                    return Optional.ofNullable(catalogRecord)
                            .map(record -> {
                                return new MetaObjectName(projectId, record.getName(), databaseName, objectName);
                            }).orElseThrow(
                                    () -> new MetaStoreException(ErrorCode.CATALOG_ID_NOT_FOUND, catalogIdent.getCatalogId()));
                });
            });
        }
    }

    public static Optional<MetaObjectName> getObjectFromNameMap(String projectId, String objectType,
                                                                String objectName) {
        try (TransactionRunner runner = new TransactionRunner()) {
            return runner.run(context -> {
                Optional<ObjectNameMap> objectNameMap = objectNameMapStore.getObjectNameMap(context, projectId,
                        ObjectType.valueOf(objectType), "null", objectName);
                return objectNameMap.map(objectMap -> {
                    CatalogIdent catalogIdent = new CatalogIdent(objectMap.getProjectId(), objectMap.getUpperObjectId());
                    CatalogObject catalogRecord = catalogStore.getCatalogById(context, catalogIdent);

                    return Optional.ofNullable(catalogRecord).map(record -> {
                        return new MetaObjectName(projectId, record.getName(),
                                objectMap.getObjectName(), "");
                    }).orElseThrow(
                            () -> new MetaStoreException(ErrorCode.CATALOG_ID_NOT_FOUND, catalogIdent.getCatalogId()));
                });
            });
        }
    }

    public static void saveObjectNameMapIfNotExist(TransactionContext context, DatabaseName databaseName,
        DatabaseIdent databaseIdent, Map<String, String> properties) {
        Optional<String> catalogName = resolveCatalogNameFromInput(properties, LMS_KEY);
        if (!catalogName.isPresent()) {
            return;
        }

        Optional<ObjectNameMap> objectNameMapOptional = objectNameMapStore
            .getObjectNameMap(context, databaseName.getProjectId(), ObjectType.DATABASE, "null",
                databaseName.getDatabaseName());
        // 2 layer object name, which may map to multiple Catalogs
        // If catalog ids are different, the same table object map record exist
        // If the catalog Id is different from the current table,
        // the map is manually modified and the 2Layer object is mapped to another catalog.
        // In this case, the map table is not modified
        if ((objectNameMapOptional.isPresent())
            && (!objectNameMapOptional.get().getUpperObjectId().equals(databaseIdent.getCatalogId()))) {
            return;
        }

        ObjectNameMap objectNameMap = new ObjectNameMap(databaseName, databaseIdent);
        objectNameMapStore.insertObjectNameMap(context, objectNameMap);
    }

    public static Optional<String> resolveCatalogNameFromInput(Map<String, String> properties, String key) {
        if (properties == null) {
            return Optional.empty();
        }

        String layer3TableName = properties.get(key);
        if (layer3TableName != null) {
            return Optional.of(layer3TableName);
        }

        return Optional.empty();
    }
}