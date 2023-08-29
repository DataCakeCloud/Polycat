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

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;
import io.polycat.catalog.store.api.CatalogStore;
import io.polycat.catalog.store.api.RoleStore;
import io.polycat.catalog.store.api.ShareStore;
import io.polycat.catalog.store.api.ViewStore;
import io.polycat.catalog.util.CheckUtil;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class RolePrivilegeHelper {

    public final static String WILDCARD_SYMBOL = "*";

    public final static String OBJECT_SEPARATE_SYMBOL = "\\.";

    @Getter
    public static boolean ROLE_PRIVILEGE_ENABLE_MVCC;

    @Value("${role.privilege.enable.mvcc:false}")
    public void setRolePrivilegeEnableMvcc(Boolean enable) {
        ROLE_PRIVILEGE_ENABLE_MVCC = enable;
    }


    private static final RolePrivilegeType[] CATALOG_PRIVILEGES = {RolePrivilegeType.DESC, RolePrivilegeType.DROP,
            RolePrivilegeType.ALTER, RolePrivilegeType.USE, RolePrivilegeType.CREATE_BRANCH,
            RolePrivilegeType.SHOW_BRANCH, RolePrivilegeType.SHOW,
            RolePrivilegeType.CREATE_DATABASE,
            RolePrivilegeType.SHOW_DATABASE, RolePrivilegeType.SHOW_ACCESSSTATS};
    private static final RolePrivilegeType[] DATABASE_PRIVILEGES = {RolePrivilegeType.DESC, RolePrivilegeType.DROP,
            RolePrivilegeType.ALTER, RolePrivilegeType.USE, RolePrivilegeType.CREATE_TABLE,
            RolePrivilegeType.SHOW_TABLE, RolePrivilegeType.CREATE_VIEW, RolePrivilegeType.UNDROP};
    private static final RolePrivilegeType[] TABLE_PRIVILEGES = {RolePrivilegeType.DESC, RolePrivilegeType.DROP,
            RolePrivilegeType.ALTER, RolePrivilegeType.UNDROP, RolePrivilegeType.RESTORE,
            RolePrivilegeType.SELECT, RolePrivilegeType.INSERT, RolePrivilegeType.DESC_ACCESSSTATS,
            RolePrivilegeType.SHOW_DATALINEAGE};
    private static final RolePrivilegeType[] VIEW_PRIVILEGES = {RolePrivilegeType.ALTER, RolePrivilegeType.DROP,
            RolePrivilegeType.DESC};
    private static final RolePrivilegeType[] MATERIALIZED_VIEW_PRIVILEGES =
            {RolePrivilegeType.ALTER, RolePrivilegeType.DROP, RolePrivilegeType.DESC};
    private static final RolePrivilegeType[] SHARE_PRIVILEGES = {RolePrivilegeType.ALTER, RolePrivilegeType.DROP,
            RolePrivilegeType.DESC};
    private static final RolePrivilegeType[] ROLE_PRIVILEGES = {RolePrivilegeType.ALTER, RolePrivilegeType.DROP,
            RolePrivilegeType.DESC};
    @Autowired
    private static CatalogStore catalogStore;
    @Autowired
    private static ShareStore shareStore;
    @Autowired
    private static ViewStore viewStore;
    @Autowired
    private static RoleStore roleStore;
    private static Map<Operation, OperationPrivilege> operationPrivilegeMap = new ConcurrentHashMap<>();

    static {
        //init operationPrivilegeMap
        operationPrivilegeMap.put(Operation.DESC_CATALOG,
                new OperationPrivilege(ObjectType.CATALOG,
                        RolePrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.DROP_CATALOG,
                new OperationPrivilege(ObjectType.CATALOG,
                        RolePrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.ALTER_CATALOG,
                new OperationPrivilege(ObjectType.CATALOG,
                        RolePrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.USE_CATALOG,
                new OperationPrivilege(ObjectType.CATALOG,
                        RolePrivilegeType.USE.getType()));

        operationPrivilegeMap.put(Operation.CREATE_BRANCH,
                new OperationPrivilege(ObjectType.CATALOG,
                        RolePrivilegeType.CREATE_BRANCH.getType()));

        operationPrivilegeMap.put(Operation.USE_BRANCH,
                new OperationPrivilege(ObjectType.CATALOG,
                        RolePrivilegeType.USE.getType()));

        operationPrivilegeMap.put(Operation.SHOW_BRANCH,
                new OperationPrivilege(ObjectType.CATALOG,
                        RolePrivilegeType.SHOW_BRANCH.getType()));

        operationPrivilegeMap.put(Operation.MERGE_BRANCH,
                new OperationPrivilege(ObjectType.CATALOG,
                        RolePrivilegeType.MERGE_BRANCH.getType()));

        operationPrivilegeMap.put(Operation.CREATE_DATABASE,
                new OperationPrivilege(ObjectType.CATALOG,
                        RolePrivilegeType.CREATE_DATABASE.getType()));

        operationPrivilegeMap.put(Operation.SHOW_DATABASE,
                new OperationPrivilege(ObjectType.CATALOG,
                        RolePrivilegeType.SHOW_DATABASE.getType()));

        operationPrivilegeMap.put(Operation.DESC_DATABASE,
                new OperationPrivilege(ObjectType.DATABASE,
                        RolePrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.DROP_DATABASE,
                new OperationPrivilege(ObjectType.DATABASE,
                        RolePrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.UNDROP_DATABASE,
                new OperationPrivilege(ObjectType.DATABASE,
                        RolePrivilegeType.UNDROP.getType()));

        operationPrivilegeMap.put(Operation.ALTER_DATABASE,
                new OperationPrivilege(ObjectType.DATABASE,
                        RolePrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.USE_DATABASE,
                new OperationPrivilege(ObjectType.DATABASE,
                        RolePrivilegeType.USE.getType()));

        operationPrivilegeMap.put(Operation.CREATE_TABLE,
                new OperationPrivilege(ObjectType.DATABASE,
                        RolePrivilegeType.CREATE_TABLE.getType()));

        operationPrivilegeMap.put(Operation.SHOW_TABLE,
                new OperationPrivilege(ObjectType.DATABASE,
                        RolePrivilegeType.SHOW_TABLE.getType()));

        operationPrivilegeMap.put(Operation.CREATE_VIEW,
                new OperationPrivilege(ObjectType.DATABASE,
                        RolePrivilegeType.CREATE_VIEW.getType()));

        operationPrivilegeMap.put(Operation.SHOW_VIEW,
                new OperationPrivilege(ObjectType.DATABASE,
                        RolePrivilegeType.SHOW_VIEW.getType()));

        operationPrivilegeMap.put(Operation.DESC_TABLE,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.DROP_TABLE,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.PURGE_TABLE,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.ALTER_TABLE,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.UNDROP_TABLE,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.UNDROP.getType()));

        operationPrivilegeMap.put(Operation.RESTORE_TABLE,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.RESTORE.getType()));

        operationPrivilegeMap.put(Operation.SELECT_TABLE,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.SELECT.getType()));

        operationPrivilegeMap.put(Operation.INSERT_TABLE,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.INSERT.getType()));

        operationPrivilegeMap.put(Operation.ALTER_VIEW,
                new OperationPrivilege(ObjectType.VIEW,
                        RolePrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.DROP_VIEW,
                new OperationPrivilege(ObjectType.VIEW,
                        RolePrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.DESC_VIEW,
                new OperationPrivilege(ObjectType.VIEW,
                        RolePrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.CREATE_ACCELERATOR,
                new OperationPrivilege(ObjectType.DATABASE,
                        RolePrivilegeType.CREATE_ACCELERATOR.getType()));

        operationPrivilegeMap.put(Operation.SHOW_ACCELERATORS,
                new OperationPrivilege(ObjectType.DATABASE,
                        RolePrivilegeType.SHOW_ACCELERATORS.getType()));

        operationPrivilegeMap.put(Operation.DROP_ACCELERATOR,
                new OperationPrivilege(ObjectType.ACCELERATOR,
                        RolePrivilegeType.DROP_ACCELERATOR.getType()));

        operationPrivilegeMap.put(Operation.ALTER_SHARE,
                new OperationPrivilege(ObjectType.SHARE,
                        RolePrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.DROP_SHARE,
                new OperationPrivilege(ObjectType.SHARE,
                        RolePrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.DESC_SHARE,
                new OperationPrivilege(ObjectType.SHARE,
                        RolePrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.ALTER_ROLE,
                new OperationPrivilege(ObjectType.ROLE,
                        RolePrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.DROP_ROLE,
                new OperationPrivilege(ObjectType.ROLE,
                        RolePrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.DESC_ROLE,
                new OperationPrivilege(ObjectType.ROLE,
                        RolePrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.CREATE_STREAM,
                new OperationPrivilege(ObjectType.STREAM,
                        RolePrivilegeType.CREATE_STREAM.getType()));

        operationPrivilegeMap.put(Operation.DESC_DELEGATE,
                new OperationPrivilege(ObjectType.DELEGATE,
                        RolePrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.DROP_DELEGATE,
                new OperationPrivilege(ObjectType.DELEGATE,
                        RolePrivilegeType.DROP.getType()));

        operationPrivilegeMap.put(Operation.REVOKE_ALL_OPERATION_FROM_ROLE,
                new OperationPrivilege(ObjectType.ROLE,
                        RolePrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.ALTER_COLUMN,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.CHANGE_SCHEMA.getType()));

        operationPrivilegeMap.put(Operation.SHOW_ACCESS_STATS_FOR_CATALOG,
                new OperationPrivilege(ObjectType.CATALOG,
                        RolePrivilegeType.SHOW_ACCESSSTATS.getType()));

        operationPrivilegeMap.put(Operation.DESC_ACCESS_STATS_FOR_TABLE,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.DESC_ACCESSSTATS.getType()));

        operationPrivilegeMap.put(Operation.SHOW_DATA_LINEAGE_FOR_TABLE,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.SHOW_DATALINEAGE.getType()));

        operationPrivilegeMap.put(Operation.SET_PROPERTIES,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.UNSET_PROPERTIES,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.ALTER.getType()));

        operationPrivilegeMap.put(Operation.ADD_PARTITION,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.INSERT.getType()));

        operationPrivilegeMap.put(Operation.DROP_PARTITION,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.DELETE.getType()));

        operationPrivilegeMap.put(Operation.COPY_INTO,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.COPY_INTO.getType()));

        operationPrivilegeMap.put(Operation.SHOW_CREATE_TABLE,
                new OperationPrivilege(ObjectType.TABLE,
                        RolePrivilegeType.DESC.getType()));

        operationPrivilegeMap.put(Operation.CREATE_MATERIALIZED_VIEW,
                new OperationPrivilege(ObjectType.DATABASE,
                        RolePrivilegeType.CREATE_MATERIALIZED_VIEW.getType()));

        operationPrivilegeMap.put(Operation.DROP_MATERIALIZED_VIEW,
                new OperationPrivilege(ObjectType.DATABASE,
                        RolePrivilegeType.DROP_MATERIALIZED_VIEW.getType()));

        operationPrivilegeMap.put(Operation.SHOW_MATERIALIZED_VIEWS,
                new OperationPrivilege(ObjectType.DATABASE,
                        RolePrivilegeType.SHOW_MATERIALIZED_VIEWS.getType()));

        operationPrivilegeMap.put(Operation.REFRESH_MATERIALIZED_VIEW,
                new OperationPrivilege(ObjectType.DATABASE,
                        RolePrivilegeType.REFRESH_MATERIALIZED_VIEW.getType()));
    }

    public static OperationPrivilege getOperationPrivilege(Operation operation) {
        return operationPrivilegeMap.get(operation);
    }

    public static long getObjectAllPrivilegesByType(String objectType) {
        switch (ObjectType.valueOf(objectType)) {
            case CATALOG:
                return convertObjectPrivileges(CATALOG_PRIVILEGES);
            case DATABASE:
                return convertObjectPrivileges(DATABASE_PRIVILEGES);
            case TABLE:
                return convertObjectPrivileges(TABLE_PRIVILEGES);
            case VIEW:
                return convertObjectPrivileges(VIEW_PRIVILEGES);
            case MATERIALIZED_VIEW:
                return convertObjectPrivileges(MATERIALIZED_VIEW_PRIVILEGES);
            case SHARE:
                return convertObjectPrivileges(SHARE_PRIVILEGES);
            case ROLE:
                return convertObjectPrivileges(ROLE_PRIVILEGES);
            default:
                return 0;
        }
    }

    public static String getRolePrivilegeObjectIdByTable(TableIdent tableIdent, TableName tableName) {
        String rolePrivilegeObjectId;
        if (RolePrivilegeHelper.checkDisableMvcc()) {
            rolePrivilegeObjectId = String.format("%s.%s.%s", tableName.getCatalogName(), tableName.getDatabaseName(), tableName.getTableName());
        } else {
            rolePrivilegeObjectId = String.format("%s.%s.%s", tableIdent.getCatalogId(), tableIdent.getDatabaseId(), tableIdent.getTableId());
        }
        return rolePrivilegeObjectId;
    }

    public static CatalogInnerObject getCatalogObject(String projectId, String objectType, String objectName) {
        CatalogInnerObject catalogInnerObject;
        // add support wildcard
        if (!ROLE_PRIVILEGE_ENABLE_MVCC || objectName.contains(RolePrivilegeHelper.WILDCARD_SYMBOL)) {
            catalogInnerObject = new CatalogInnerObject();
            catalogInnerObject.setProjectId(projectId);
            catalogInnerObject.setObjectName(objectName);
            // hacker TODO
            catalogInnerObject.setObjectId(objectName);
        } else {
            catalogInnerObject = ServiceImplHelper.getCatalogObject(projectId,
                    objectType, objectName);
        }
        return catalogInnerObject;
    }

    public static String getRolePrivilegeObjectId(String objectType, CatalogInnerObject catalogInnerObject) {
        if (checkDisableMvcc() || checkEnableWildcard(catalogInnerObject.getObjectId())) {
            return catalogInnerObject.getObjectId();
        }
        if (objectType.equals(ObjectType.TABLE.name()) || objectType.equals(ObjectType.VIEW.name())) {
            return catalogInnerObject.getCatalogId() + "." + catalogInnerObject.getDatabaseId() + "."
                    + catalogInnerObject.getObjectId();
        } else if (objectType.equals(ObjectType.DATABASE.name())) {
            return catalogInnerObject.getCatalogId() + "." + catalogInnerObject.getObjectId();
        } else {
            return catalogInnerObject.getObjectId();
        }
    }


    public static String getRolePrivilegeObjectIdSkipCheck(String objectType, CatalogInnerObject catalogInnerObject) {
        if (objectType.equals(ObjectType.TABLE.name()) || objectType.equals(ObjectType.VIEW.name())) {
            return catalogInnerObject.getCatalogId() + "." + catalogInnerObject.getDatabaseId() + "."
                    + catalogInnerObject.getObjectId();
        } else if (objectType.equals(ObjectType.DATABASE.name())) {
            return catalogInnerObject.getCatalogId() + "." + catalogInnerObject.getObjectId();
        } else {
            return catalogInnerObject.getObjectId();
        }
    }


    private static long convertObjectPrivileges(RolePrivilegeType[] privileges) {
        long privilege = 0;
        for (RolePrivilegeType i : privileges) {
            privilege |= convertRolePrivilegeType(i);
        }
        return privilege;
    }

    public static List<ObjectPrivilege> convertRolePrivilege(TransactionContext context, String projectId,
                                                             List<RolePrivilegeObject> rolePrivilegeObjectList) {
        List<ObjectPrivilege> objectPrivilegeList = new ArrayList<>();
        String objectName;
        for (RolePrivilegeObject rolePrivilege : rolePrivilegeObjectList) {
            String objectType = rolePrivilege.getObjectType();
            if (checkDisableMvcc() || checkEnableWildcard(rolePrivilege.getObjectId())) {
                objectName = rolePrivilege.getObjectId();
            } else {
                CatalogInnerObject catalogInnerObject = new CatalogInnerObject();
                catalogInnerObject.setProjectId(projectId);
                catalogInnerObject.setCatalogId(rolePrivilege.getCatalogId());
                catalogInnerObject.setDatabaseId(rolePrivilege.getDatabaseId());
                catalogInnerObject.setObjectId(getObjectIdByType(rolePrivilege.getObjectType(), rolePrivilege.getObjectId()));

                //todo use RecordStoreHelper.getObjectNameById
                objectName = getObjectNameById(context, catalogInnerObject, rolePrivilege.getObjectType());
            }
            for (RolePrivilegeType rolePrivilegeType : RolePrivilegeType.values()) {
                if ((rolePrivilege.getPrivilege() & (convertRolePrivilegeType(rolePrivilegeType))) == 0) {
                    continue;
                }
                String privilege = convertPrivilegeString(rolePrivilegeType, objectType);
                ObjectPrivilege objectPrivilege = new ObjectPrivilege(objectType, objectName, privilege);
                objectPrivilegeList.add(objectPrivilege);
            }
        }
        return objectPrivilegeList;
    }

    public static boolean checkDisableMvcc() {
        return !ROLE_PRIVILEGE_ENABLE_MVCC;
    }

    public static boolean checkEnableWildcard(String objectId) {
        return objectId.contains(WILDCARD_SYMBOL);
    }

    private static String convertPrivilegeString(RolePrivilegeType rolePrivilegeType, String objectType) {
        String privilege;
        if (rolePrivilegeType == RolePrivilegeType.CREATE_DATABASE ||
                rolePrivilegeType == RolePrivilegeType.CREATE_TABLE ||
                rolePrivilegeType == RolePrivilegeType.CREATE_VIEW ||
                rolePrivilegeType == RolePrivilegeType.CREATE_BRANCH ||
                rolePrivilegeType == RolePrivilegeType.SHOW_DATABASE ||
                rolePrivilegeType == RolePrivilegeType.SHOW_TABLE ||
                rolePrivilegeType == RolePrivilegeType.SHOW_BRANCH ||
                rolePrivilegeType == RolePrivilegeType.SHOW_VIEW) {
            String[] src = rolePrivilegeType.toString().split("_");
            privilege = src[0] + " " + src[1];
        } else {
            privilege = rolePrivilegeType.toString() + " " + objectType;
        }
        return privilege;
    }

    public static long convertOperationPrivilege(OperationPrivilege operationPrivilege) {
        return 1L << operationPrivilege.getPrivilege();
    }

    public static long convertRolePrivilegeType(RolePrivilegeType rolePrivilegeType) {
        return 1L << rolePrivilegeType.getType();
    }

    private static String getObjectIdByType(String objectType, String objId) {
        if (objectType.equals(ObjectType.TABLE.name()) || objectType.equals(ObjectType.VIEW.name())) {
            String[] objectId = objId.split(OBJECT_SEPARATE_SYMBOL);
            return objectId[2];
        } else if (objectType.equals(ObjectType.DATABASE.name())) {
            String[] objectId = objId.split(OBJECT_SEPARATE_SYMBOL);
            return objectId[1];
        }
        return objId;
    }

    private static String getObjectNameById(TransactionContext context, CatalogInnerObject catalogInnerObject, String objectType) {
        String objectName;
        switch (ObjectType.valueOf(objectType)) {
            case CATALOG:
                objectName = getCatalogNameById(context, catalogInnerObject);
                break;
            case SHARE:
                objectName = getShareName(context, catalogInnerObject);
                break;
            case ROLE:
                objectName = roleStore.getRoleId(context, "catalogInnerObject", "");
                //objectName = roleStore.getRoleId(context, catalogInnerObject);
                break;
            case VIEW:
                objectName = "";
                objectName = getViewName(context, catalogInnerObject);
                break;
            case DATABASE:
                objectName = getCatalogNameById(context, getCatalogFromDatabase(catalogInnerObject))
                        + "." + getDatabaseNameViaBranch(context, catalogInnerObject);
                break;
            case TABLE:
                objectName = getCatalogNameById(context, getCatalogFromTable(catalogInnerObject))
                        + "." + getDatabaseNameViaBranch(context, getDatabaseFromTable(catalogInnerObject))
                        + "." + getTableNameViaBranch(context, catalogInnerObject);
                break;
            default:
                throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
        return objectName;
    }

    public static String constructObjectName(ObjectType objectType, AuthorizationInput authorizationInput) {

        String objectName;
        if (objectType == ObjectType.CATALOG) {
            objectName = authorizationInput.getCatalogInnerObject().getObjectName();
        } else if (objectType == ObjectType.DATABASE) {
            objectName = authorizationInput.getCatalogInnerObject().getCatalogName()
                    + "." + authorizationInput.getCatalogInnerObject().getObjectName();
        } else if (objectType == ObjectType.TABLE) {
            objectName = authorizationInput.getCatalogInnerObject().getCatalogName()
                    + "." + authorizationInput.getCatalogInnerObject().getDatabaseName()
                    + "." + authorizationInput.getCatalogInnerObject().getObjectName();
        } else {
            objectName = authorizationInput.getCatalogInnerObject().getObjectName();
        }

        return objectName;
    }
    private static String getCatalogNameById(TransactionContext context, CatalogInnerObject catalogInnerObject) {
        CatalogIdent catalogIdent = new CatalogIdent(catalogInnerObject.getProjectId(), catalogInnerObject.getCatalogId());
        CatalogObject catalogRecord = catalogStore.getCatalogById(context, catalogIdent);
        CheckUtil.assertNotNull(catalogRecord, ErrorCode.CATALOG_ID_NOT_FOUND, catalogIdent.getCatalogId());
        return catalogRecord.getName();
    }

    private static String getShareName(TransactionContext context, CatalogInnerObject catalogInnerObject) {
        ShareObject shareObject = shareStore.getShareProperties(context, catalogInnerObject.getProjectId(),
                catalogInnerObject.getObjectId());
        CheckUtil.assertNotNull(shareObject, ErrorCode.SHARE_ID_NOT_FOUND, catalogInnerObject.getObjectId());
        return shareObject.getShareName();
    }

    private static String getViewName(TransactionContext context, CatalogInnerObject catalogInnerObject) {
        ViewIdent viewIdent = new ViewIdent();
        viewIdent.setProjectId(catalogInnerObject.getProjectId());
        viewIdent.setViewId(catalogInnerObject.getObjectId());
        ViewRecordObject view = viewStore.getView(context, viewIdent);
        if (view == null) {
            throw new MetaStoreException(ErrorCode.VIEW_NOT_FOUND);
        }

        return view.getName();


    }

    private static CatalogInnerObject getCatalogFromDatabase(CatalogInnerObject databaseObject) {
        CatalogInnerObject catalog = new CatalogInnerObject();
        catalog.setProjectId(databaseObject.getProjectId());
        catalog.setCatalogId(databaseObject.getCatalogId());
        catalog.setObjectId(databaseObject.getObjectId());
        return catalog;
    }

    private static CatalogInnerObject getDatabaseFromTable(CatalogInnerObject table) {
        CatalogInnerObject database = new CatalogInnerObject();
        database.setProjectId(table.getProjectId());
        database.setCatalogId(table.getCatalogId());
        database.setDatabaseId(table.getDatabaseId());
        database.setObjectId(table.getDatabaseId());
        return database;
    }

    private static CatalogInnerObject getCatalogFromTable(CatalogInnerObject table) {
        CatalogInnerObject catalog = new CatalogInnerObject();
        catalog.setProjectId(table.getProjectId());
        catalog.setCatalogId(table.getCatalogId());
        catalog.setDatabaseId(table.getDatabaseId());
        catalog.setObjectId(table.getObjectId());
        return catalog;
    }

    private static String getDatabaseNameViaBranch(TransactionContext context, CatalogInnerObject catalogInnerObject) {
        DatabaseIdent databaseIdent = new DatabaseIdent(catalogInnerObject.getProjectId(), catalogInnerObject.getCatalogId(),
                catalogInnerObject.getObjectId());
        String databaseName = DatabaseObjectHelper.getDatabaseNameById(context, databaseIdent, true);
        CheckUtil.assertNotNull(databaseName, ErrorCode.DATABASE_ID_NOT_FOUND, databaseIdent.getDatabaseId());

        return databaseName;
    }

    private static String getTableNameViaBranch(TransactionContext context, CatalogInnerObject catalogInnerObject) {
        TableIdent tableIdent = new TableIdent(catalogInnerObject.getProjectId(),
                catalogInnerObject.getCatalogId(),
                catalogInnerObject.getDatabaseId(),
                catalogInnerObject.getObjectId());
        String tableName = TableObjectHelper.getTableName(context, tableIdent, true);
        CheckUtil.assertNotNull(tableName, ErrorCode.TABLE_ID_NOT_FOUND, tableIdent.getTableId());

        return tableName;
    }

    @Autowired
    public void setCatalogStore(CatalogStore catalogStore) {
        RolePrivilegeHelper.catalogStore = catalogStore;
    }

    @Autowired
    public void setCatalogStore(ShareStore shareStore) {
        RolePrivilegeHelper.shareStore = shareStore;
    }

    @Autowired
    public void setCatalogStore(ViewStore viewStore) {
        RolePrivilegeHelper.viewStore = viewStore;
    }

    @Autowired
    public void setCatalogStore(RoleStore roleStore) {
        RolePrivilegeHelper.roleStore = roleStore;
    }
}
