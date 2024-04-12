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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import io.polycat.catalog.server.util.TransactionRunnerUtil;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.DatabaseObject;
import io.polycat.catalog.common.model.ObjectIdent;
import io.polycat.catalog.common.model.TableObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.ViewName;
import io.polycat.catalog.service.api.ViewService;
import io.polycat.catalog.store.api.Transaction;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.fdb.record.impl.RoleStoreImpl;
import io.polycat.catalog.store.fdb.record.impl.ShareStoreImpl;

import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableName;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class ServiceImplHelper {
    private static final Map<ObjectType, Function<ObjectIdent, CatalogInnerObject>> objectNameToIdFunMap = new ConcurrentHashMap<>();
    private static final Map<ObjectType, Function<ObjectIdent, CatalogInnerObject>> droppedObjectNameToIdFunMap = new ConcurrentHashMap<>();
    private static ViewService viewService;
    private static Transaction storeTransaction;

    @Autowired
    public void setStoreTransaction(Transaction storeTransaction) {
        ServiceImplHelper.storeTransaction = storeTransaction;
    }

    @Autowired
    private void setViewService(ViewService viewService) {
        ServiceImplHelper.viewService = viewService;
    }

    private static final Function<ObjectIdent, CatalogInnerObject> getCatalogObjectByCatalogName = (objectIdent) -> {
        String catalogId = getCatalogIdByName(objectIdent.getProjectId(), objectIdent.getObjectName());
        CatalogInnerObject catalogInnerObject = new CatalogInnerObject();
        catalogInnerObject.setCatalogId(catalogId);
        catalogInnerObject.setDatabaseId("");
        catalogInnerObject.setObjectId(catalogId);
        catalogInnerObject.setProjectId(objectIdent.getProjectId());
        return catalogInnerObject;
    };

    private static final Function<ObjectIdent, CatalogInnerObject> getCatalogObjectByDatabaseName = (objectIdent) -> {
        String[] strArray = objectIdent.getObjectName().split("\\.");
        if (strArray.length != 2) {
            throw new MetaStoreException(ErrorCode.OBJECT_NAME_IS_ILLEGAL, objectIdent.getObjectName());
        }
        String catalogId = getCatalogIdByName(objectIdent.getProjectId(), strArray[0]);
        String databaseId = getDatabaseIdByName(objectIdent.getProjectId(), strArray[0], strArray[1]);
        CatalogInnerObject catalogInnerObject = new CatalogInnerObject();
        catalogInnerObject.setCatalogId(catalogId);
        catalogInnerObject.setDatabaseId(databaseId);
        catalogInnerObject.setObjectId(databaseId);
        catalogInnerObject.setProjectId(objectIdent.getProjectId());
        return catalogInnerObject;
    };

    private static final Function<ObjectIdent, CatalogInnerObject> getCatalogObjectByShareName = (objectIdent) -> {
        String shareId = getShareIdByName(objectIdent.getProjectId(), objectIdent.getObjectName());
        CatalogInnerObject catalogInnerObject = new CatalogInnerObject();
        catalogInnerObject.setCatalogId(shareId);
        catalogInnerObject.setDatabaseId("");
        catalogInnerObject.setObjectId(shareId);
        catalogInnerObject.setProjectId(objectIdent.getProjectId());
        return catalogInnerObject;
    };

    private static final Function<ObjectIdent, CatalogInnerObject> getCatalogObjectByRoleName = (objectIdent) -> {
        String roleId = getRoleIdByName(objectIdent.getProjectId(), objectIdent.getObjectName());
        CatalogInnerObject catalogInnerObject = new CatalogInnerObject();
        catalogInnerObject.setCatalogId(roleId);
        catalogInnerObject.setDatabaseId("");
        catalogInnerObject.setObjectId(roleId);
        catalogInnerObject.setProjectId(objectIdent.getProjectId());
        return catalogInnerObject;
    };

    private static final Function<ObjectIdent, CatalogInnerObject> getCatalogObjectByDelegateName = (objectIdent -> {
        CatalogInnerObject catalogInnerObject = new CatalogInnerObject();
        catalogInnerObject.setProjectId(objectIdent.getProjectId());
        catalogInnerObject.setObjectId(objectIdent.getObjectName());
        return catalogInnerObject;
    });


    private static final Function<ObjectIdent, CatalogInnerObject> getCatalogObjectByViewName = (objectIdent) -> {
        String[] strArray = objectIdent.getObjectName().split("\\.");
        if (strArray.length != 3) {
            throw new MetaStoreException(ErrorCode.OBJECT_NAME_IS_ILLEGAL, objectIdent.getObjectName());
        }
        String catalogId = getCatalogIdByName(objectIdent.getProjectId(), strArray[0]);
        String databaseId = getDatabaseIdByName(objectIdent.getProjectId(), strArray[0], strArray[1]);
        String viewId = getViewIdByName(objectIdent.getProjectId(), strArray[0], strArray[1], strArray[2]);
        CatalogInnerObject catalogInnerObject = new CatalogInnerObject();
        catalogInnerObject.setCatalogId(catalogId);
        catalogInnerObject.setDatabaseId(databaseId);
        catalogInnerObject.setObjectId(viewId);
        catalogInnerObject.setProjectId(objectIdent.getProjectId());
        return catalogInnerObject;
    };

    private static final Function<ObjectIdent, CatalogInnerObject> getCatalogObjectByTableName = (objectIdent) -> {
        String[] strArray = objectIdent.getObjectName().split("\\.");
        if (strArray.length != 3) {
            throw new MetaStoreException(ErrorCode.OBJECT_NAME_IS_ILLEGAL, objectIdent.getObjectName());
        }

        try {
            String catalogId = getCatalogIdByName(objectIdent.getProjectId(), strArray[0]);
            String databaseId = getDatabaseIdByName(objectIdent.getProjectId(), strArray[0], strArray[1]);
            String tableId = getTableIdByName(objectIdent.getProjectId(), strArray[0], strArray[1], strArray[2]);
            CatalogInnerObject catalogInnerObject = new CatalogInnerObject();
            catalogInnerObject.setCatalogId(catalogId);
            catalogInnerObject.setDatabaseId(databaseId);
            catalogInnerObject.setObjectId(tableId);
            catalogInnerObject.setProjectId(objectIdent.getProjectId());
            return catalogInnerObject;
        } catch (MetaStoreException e) {
            throw new MetaStoreException(ErrorCode.TABLE_NOT_FOUND, strArray[2]);
        }

    };

    private static final Function<ObjectIdent, CatalogInnerObject> getDroppedCatalogObjectByTableName = (objectIdent) -> {
        String[] strArray = objectIdent.getObjectName().split("\\.");
        if ((strArray.length != 3) && (strArray.length != 4)) {
            throw new MetaStoreException(ErrorCode.OBJECT_NAME_IS_ILLEGAL, objectIdent.getObjectName());
        }

        String catalogId = getCatalogIdByName(objectIdent.getProjectId(), strArray[0]);
        String databaseId = getDatabaseIdByName(objectIdent.getProjectId(), strArray[0], strArray[1]);
        String tableId = null;
        if (strArray.length == 3) {
            TableName tableName = StoreConvertor.tableName(objectIdent.getProjectId(), strArray[0], strArray[1],
                strArray[2]);
            TableIdent droppedTableIdent = TableObjectHelper.getDroppedTableIdent(tableName);
            if (droppedTableIdent == null) {
                throw new MetaStoreException(ErrorCode.TABLE_NOT_FOUND, tableName.getTableName());
            }
            tableId = droppedTableIdent.getTableId();
        } else {
            tableId = strArray[3];
        }

        CatalogInnerObject catalogInnerObject = new CatalogInnerObject();
        catalogInnerObject.setCatalogId(catalogId);
        catalogInnerObject.setDatabaseId(databaseId);
        catalogInnerObject.setObjectId(tableId);
        catalogInnerObject.setProjectId(objectIdent.getProjectId());
        return catalogInnerObject;
    };

    private static final Function<ObjectIdent, CatalogInnerObject> getDroppedCatalogObjectByDatabaseName = (objectIdent) -> {
        String[] strArray = objectIdent.getObjectName().split("\\.");
        if ((strArray.length != 2) && (strArray.length != 3)) {
            throw new MetaStoreException(ErrorCode.OBJECT_NAME_IS_ILLEGAL, objectIdent.getObjectName());
        }

        String catalogId = getCatalogIdByName(objectIdent.getProjectId(), strArray[0]);
        Optional<String> databaseId;
        if (strArray.length == 2) {
            DatabaseName databaseName = StoreConvertor.databaseName(objectIdent.getProjectId(), strArray[0], strArray[1]);
            DatabaseIdent databaseIdent = DatabaseObjectHelper.getOnlyDroppedDatabaseIdentOrElseThrow(databaseName);
            if (databaseIdent == null) {
                throw new MetaStoreException(ErrorCode.DATABASE_NOT_FOUND, databaseName.getDatabaseName());
            }
            databaseId = Optional.of(databaseIdent.getDatabaseId());
        } else {
            databaseId = Optional.of(strArray[2]);
        }

        CatalogInnerObject catalogInnerObject = new CatalogInnerObject();
        catalogInnerObject.setCatalogId(catalogId);
        catalogInnerObject.setDatabaseId(databaseId.get());
        catalogInnerObject.setObjectId(databaseId.get());
        catalogInnerObject.setProjectId(objectIdent.getProjectId());
        return catalogInnerObject;
    };

    static {
        //init objectNameToIdFunMap
        objectNameToIdFunMap.put(ObjectType.CATALOG, getCatalogObjectByCatalogName);
        objectNameToIdFunMap.put(ObjectType.DATABASE, getCatalogObjectByDatabaseName);
        objectNameToIdFunMap.put(ObjectType.VIEW, getCatalogObjectByViewName);
        objectNameToIdFunMap.put(ObjectType.TABLE, getCatalogObjectByTableName);
        objectNameToIdFunMap.put(ObjectType.SHARE, getCatalogObjectByShareName);
        objectNameToIdFunMap.put(ObjectType.ROLE, getCatalogObjectByRoleName);
        objectNameToIdFunMap.put(ObjectType.DELEGATE, getCatalogObjectByDelegateName);
        droppedObjectNameToIdFunMap.put(ObjectType.TABLE, getDroppedCatalogObjectByTableName);
        droppedObjectNameToIdFunMap.put(ObjectType.DATABASE, getDroppedCatalogObjectByDatabaseName);
    }

    public static CatalogInnerObject getCatalogObject(String projectId, String objectType, String objectName) {
        ObjectIdent objectIdent = new ObjectIdent();
        objectIdent.setProjectId(projectId);
        objectIdent.setObjectName(objectName);
        return objectNameToIdFunMap.get(ObjectType.valueOf(objectType)).apply(objectIdent);
    }

    private static String getRoleIdByName(String projectId, String name) {
        RoleStoreImpl roleStore = RoleStoreImpl.getInstance();
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            String roleId = roleStore.getRoleId(context, projectId, name);
            context.commit();
            return roleId;
        }).getResult();
    }

    private static String getShareIdByName(String projectId, String name) {
        ShareStoreImpl shareStore = ShareStoreImpl.getInstance();
        try (TransactionContext context = storeTransaction.openTransaction()) {
            String shareId = shareStore.getShareId(context, projectId, name);
            context.commit();
            return shareId;
        }
    }

    private static String getCatalogIdByName(String projectId, String name) {
        CatalogName catalogName =new CatalogName(projectId, name);
        CatalogObject catalogRecord = CatalogObjectHelper.getCatalogObject(catalogName);
        if (catalogRecord == null) {
            throw new MetaStoreException(ErrorCode.CATALOG_NOT_FOUND, catalogName.getCatalogName());
        }
        return catalogRecord.getCatalogId();
    }

    private static String getDatabaseIdByName(String projectId, String catalogName, String dbName) {
        DatabaseName databaseName = new DatabaseName(projectId, catalogName, dbName);
        DatabaseObject database = DatabaseObjectHelper.getDatabaseObject(databaseName);
        if (database == null) {
            throw new MetaStoreException(ErrorCode.DATABASE_NOT_FOUND, dbName);
        }
        return database.getDatabaseId();
    }

    private static String getTableIdByName(String projectId, String catalogName, String dbName, String tbName) {
        TableName tableName = new TableName(projectId, catalogName, dbName, tbName);
        TableObject table = TableObjectHelper.getTableObject(tableName);
        if (table == null) {
            throw new MetaStoreException(ErrorCode.TABLE_NOT_FOUND, tbName);
        }
        return table.getTableId();
    }

    private static String getViewIdByName(String projectId, String catalogName, String dbName, String vName) {
        ViewName viewName = new ViewName(projectId, catalogName, dbName, vName);
        return viewService.getViewByName(viewName).getViewId();
    }

    public static CatalogInnerObject getDroppedCatalogObject(String projectId, String objectType, String objectName) {
        ObjectIdent objectIdent = new ObjectIdent();
        objectIdent.setProjectId(projectId);
        objectIdent.setObjectName(objectName);
        return droppedObjectNameToIdFunMap.get(ObjectType.valueOf(objectType)).apply(objectIdent);
    }
}
