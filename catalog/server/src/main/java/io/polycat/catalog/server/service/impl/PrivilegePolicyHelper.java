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
import java.util.List;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.EffectType;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.ObjectPrivilege;
import io.polycat.catalog.common.model.OperationPrivilegeType;
import io.polycat.catalog.common.model.ShareObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.ViewIdent;
import io.polycat.catalog.common.model.ViewRecordObject;
import io.polycat.catalog.store.fdb.record.impl.CatalogStoreImpl;
import io.polycat.catalog.store.fdb.record.impl.GlobalShareStoreImpl;
import io.polycat.catalog.store.fdb.record.impl.NewRoleStoreImpl;
import io.polycat.catalog.store.fdb.record.impl.PolicyStoreImpl;
import io.polycat.catalog.store.fdb.record.impl.ViewStoreImpl;

public class PrivilegePolicyHelper {
    private static final PolicyStoreImpl policyStore = PolicyStoreImpl.getInstance();
    private static final CatalogStoreImpl catalogStore = CatalogStoreImpl.getInstance();
    private static final ViewStoreImpl viewStore = ViewStoreImpl.getInstance();
    private static final GlobalShareStoreImpl shareStore = GlobalShareStoreImpl.getInstance();
    private static final NewRoleStoreImpl roleStore = NewRoleStoreImpl.getInstance();


    private static final OperationPrivilegeType[] CATALOG_PRIVILEGES = {OperationPrivilegeType.DESC, OperationPrivilegeType.DROP,
            OperationPrivilegeType.ALTER, OperationPrivilegeType.USE, OperationPrivilegeType.CREATE_BRANCH,
            OperationPrivilegeType.SHOW_BRANCH, OperationPrivilegeType.CREATE_DATABASE,
            OperationPrivilegeType.SHOW_DATABASE, OperationPrivilegeType.SHOW_ACCESSSTATS};
    private static final OperationPrivilegeType[] DATABASE_PRIVILEGES = {OperationPrivilegeType.DESC, OperationPrivilegeType.DROP,
            OperationPrivilegeType.ALTER, OperationPrivilegeType.USE, OperationPrivilegeType.CREATE_TABLE,
            OperationPrivilegeType.SHOW_TABLE, OperationPrivilegeType.CREATE_VIEW, OperationPrivilegeType.UNDROP};
    private static final OperationPrivilegeType[] TABLE_PRIVILEGES = {OperationPrivilegeType.DESC, OperationPrivilegeType.DROP,
            OperationPrivilegeType.ALTER, OperationPrivilegeType.UNDROP, OperationPrivilegeType.RESTORE,
            OperationPrivilegeType.SELECT, OperationPrivilegeType.INSERT, OperationPrivilegeType.DESC_ACCESSSTATS,
            OperationPrivilegeType.SHOW_DATALINEAGE};
    private static final OperationPrivilegeType[] VIEW_PRIVILEGES = {OperationPrivilegeType.ALTER, OperationPrivilegeType.DROP,
            OperationPrivilegeType.DESC};
    private static final OperationPrivilegeType[] SHARE_PRIVILEGES = {OperationPrivilegeType.ALTER, OperationPrivilegeType.DROP,
            OperationPrivilegeType.DESC};
    private static final OperationPrivilegeType[] ROLE_PRIVILEGES = {OperationPrivilegeType.ALTER, OperationPrivilegeType.DROP,
            OperationPrivilegeType.DESC};
    private static final OperationPrivilegeType[] DELEGATE_PRIVILEGES = {OperationPrivilegeType.ALTER, OperationPrivilegeType.DROP,
            OperationPrivilegeType.DESC, OperationPrivilegeType.DROP, OperationPrivilegeType.SHOW};


    public static List<ObjectPrivilege> convertObjectPrivilegeList(TransactionContext context, String projectId,
        List<MetaPrivilegePolicy> metaPrivilegesList) {
        List<ObjectPrivilege> objectPrivilegeList = new ArrayList<>();
        for (MetaPrivilegePolicy metaPrivilege : metaPrivilegesList) {
            ObjectPrivilege objectPrivilege = convertObjectPrivilege(context,projectId,metaPrivilege);
            objectPrivilegeList.add(objectPrivilege);
        }
        return objectPrivilegeList;
    }

    public static ObjectPrivilege convertObjectPrivilege(TransactionContext context, String projectId,
        MetaPrivilegePolicy metaPrivilege) {

        String objectType = ObjectType.forNum(metaPrivilege.getObjectType()).name();
        CatalogInnerObject catalogInnerObject = convertObjectIdToCatalogObject(projectId, metaPrivilege.getObjectType(),
            metaPrivilege.getObjectId());
        String objectName = getObjectNameById(context, catalogInnerObject, metaPrivilege.getObjectType());

        long privilege  = metaPrivilege.getPrivilege();
        String privilegeString;
        if (isOwnerOfObject(privilege)) {
            privilegeString = convertPrivilegeToString(metaPrivilege.isEffect(),
                OperationPrivilegeType.OWNER, objectType);
        } else {
            OperationPrivilegeType operationPrivilegeType = convertPrivilegeToOperationType(privilege);
            privilegeString = convertPrivilegeToString(metaPrivilege.isEffect(),
                operationPrivilegeType, objectType);
        }
        ObjectPrivilege objectPrivilege = new ObjectPrivilege(objectType, objectName, privilegeString);

        return objectPrivilege;
    }

    public static String convertObjectName(TransactionContext context, String projectId,
        MetaPrivilegePolicy metaPrivilege) {

        String objectType = ObjectType.forNum(metaPrivilege.getObjectType()).name();
        CatalogInnerObject catalogInnerObject = convertObjectIdToCatalogObject(projectId, metaPrivilege.getObjectType(),
            metaPrivilege.getObjectId());
        String objectName = getObjectNameById(context, catalogInnerObject, metaPrivilege.getObjectType());
        return objectName;
    }

    private static String getCatalogNameById(TransactionContext context, CatalogInnerObject catalogInnerObject) {
        CatalogIdent catalogIdent = new CatalogIdent(catalogInnerObject.getProjectId(), catalogInnerObject.getObjectId());
        CatalogObject catalogRecord = catalogStore.getCatalogById(context, catalogIdent);
        if (catalogRecord == null) {
            throw new MetaStoreException(ErrorCode.CATALOG_ID_NOT_FOUND, catalogIdent.getCatalogId());
        }
        return catalogRecord.getName();
    }

    private static String getShareName(TransactionContext context, CatalogInnerObject catalogInnerObject) {
        ShareObject shareObject = shareStore.getSharePropertiesById(context, catalogInnerObject.getProjectId(),
            catalogInnerObject.getObjectId());
        if (shareObject == null) {
            throw new MetaStoreException(ErrorCode.SHARE_ID_NOT_FOUND, catalogInnerObject.getObjectId());
        }
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
        catalog.setObjectId(databaseObject.getCatalogId());
        return  catalog;
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
        catalog.setObjectId(table.getCatalogId());
        return catalog;
    }

    private static String getDatabaseNameViaBranch(TransactionContext context, CatalogInnerObject catalogInnerObject) {
        DatabaseIdent databaseIdent = new DatabaseIdent(catalogInnerObject.getProjectId(), catalogInnerObject.getCatalogId(),
            catalogInnerObject.getObjectId());
        String databaseName = DatabaseObjectHelper.getDatabaseNameById(context, databaseIdent, true);
        if (databaseName == null) {
            throw new CatalogServerException(ErrorCode.DATABASE_ID_NOT_FOUND, databaseIdent.getDatabaseId());
        }

        return databaseName;
    }

    private static String getTableNameViaBranch(TransactionContext context, CatalogInnerObject catalogInnerObject) {
        TableIdent tableIdent = new TableIdent(catalogInnerObject.getProjectId(),
            catalogInnerObject.getCatalogId(),
            catalogInnerObject.getDatabaseId(),
            catalogInnerObject.getObjectId());
        String tableName = TableObjectHelper.getTableName(context, tableIdent, true);
        if (tableName == null) {
            throw new CatalogServerException(ErrorCode.TABLE_ID_NOT_FOUND, tableIdent.getTableId());
        }
        return tableName;
    }

    private static String getObjectNameById(TransactionContext context, CatalogInnerObject cataloginnerObject, int objectType) {
        String objectName;
        switch (ObjectType.forNum(objectType)) {
            case CATALOG:
                objectName = getCatalogNameById(context, cataloginnerObject);
                break;
            case SHARE:
                objectName = getShareName(context, cataloginnerObject);
                break;
            case ROLE:
                objectName = roleStore.getRoleName(context, cataloginnerObject);
                break;
            case VIEW:
                objectName = getViewName(context, cataloginnerObject);
                break;
            case DATABASE:
                objectName = getCatalogNameById(context, getCatalogFromDatabase(cataloginnerObject))
                    + "." + getDatabaseNameViaBranch(context, cataloginnerObject);
                break;
            case TABLE:
                objectName = getCatalogNameById(context, getCatalogFromTable(cataloginnerObject))
                    + "." + getDatabaseNameViaBranch(context, getDatabaseFromTable(cataloginnerObject))
                    + "." + getTableNameViaBranch(context, cataloginnerObject);
                break;
            default:
                throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
        return objectName;
    }

    private  static String convertPrivilegeToString(boolean effect,
        OperationPrivilegeType operationPrivilegeType, String objectType ) {

        StringBuffer sb = new StringBuffer();
        sb.append(convertEffectToString(effect))
            .append(":")
            .append(convertPrivilegeToOperationName(operationPrivilegeType, objectType));
        return  sb.toString();
    }

    private  static String convertEffectToString(boolean effect_bool_value) {
        if (effect_bool_value) {
            return EffectType.ALLOW.toString();
        }
        return EffectType.DENY.toString();
    }

    private static String convertPrivilegeToOperationName(OperationPrivilegeType operationPrivilegeType, String objectType) {
        String privilege;
        if (operationPrivilegeType == OperationPrivilegeType.CREATE_DATABASE ||
            operationPrivilegeType == OperationPrivilegeType.CREATE_TABLE ||
            operationPrivilegeType == OperationPrivilegeType.CREATE_VIEW ||
            operationPrivilegeType == OperationPrivilegeType.CREATE_BRANCH ||
            operationPrivilegeType == OperationPrivilegeType.SHOW_DATABASE ||
            operationPrivilegeType == OperationPrivilegeType.SHOW_TABLE ||
            operationPrivilegeType == OperationPrivilegeType.SHOW_BRANCH ||
            operationPrivilegeType == OperationPrivilegeType.SHOW_VIEW) {
            String[] src = operationPrivilegeType.toString().split("_");
            privilege = src[0] + " " + src[1];
        } else if (operationPrivilegeType == OperationPrivilegeType.SHARE_PRIVILEGE_USAGE ||
            operationPrivilegeType == OperationPrivilegeType.SHARE_PRIVILEGE_SELECT) {
            String[] src = operationPrivilegeType.toString().split("_");
            privilege = src[0] + " " + src[1] + " " + src[2];
        } else {
            privilege = operationPrivilegeType.toString() + " " + objectType;
        }
        return privilege;
    }



    private static String getObjectIdByType(int objectType, String objId) {
        if (objectType == ObjectType.TABLE.getNum() || objectType == ObjectType.VIEW.getNum()) {
            String[] objectId = objId.split("\\.");
            return objectId[2];
        } else if (objectType == ObjectType.DATABASE.getNum()) {
            String[] objectId = objId.split("\\.");
            return objectId[1];
        }
        return objId;
    }

    private String getObjectIdByType(String objectType, String objId) {
        if (objectType.equals(io.polycat.catalog.common.ObjectType.TABLE.name()) || objectType.equals(
            io.polycat.catalog.common.ObjectType.VIEW.name())) {
            String[] objectId = objId.split("\\.");
            return objectId[2];
        } else if (objectType.equals(io.polycat.catalog.common.ObjectType.DATABASE.name())) {
            String[] objectId = objId.split("\\.");
            return objectId[1];
        }
        return objId;
    }

    private static CatalogInnerObject convertObjectIdToCatalogObject(String projectId, int objectType, String objId) {
        CatalogInnerObject catalogInnerObject = new CatalogInnerObject();
        catalogInnerObject.setProjectId(projectId);
        if (objectType == ObjectType.TABLE.getNum()
            || objectType == ObjectType.VIEW.getNum()) {
            String[] objectId = objId.split("\\.");
            catalogInnerObject.setCatalogId(objectId[0]);
            catalogInnerObject.setDatabaseId(objectId[1]);
            catalogInnerObject.setObjectId(objectId[2]);
        } else if (objectType == ObjectType.DATABASE.getNum()) {
            String[] objectId = objId.split("\\.");
            catalogInnerObject.setCatalogId(objectId[0]);
            catalogInnerObject.setDatabaseId(objectId[1]);
            catalogInnerObject.setObjectId(objectId[1]);
        } else {
            String[] objectId = objId.split("\\.");
            catalogInnerObject.setObjectId(objectId[0]);
        }
        return  catalogInnerObject;
    }


    private static long convertPrivilegeType(OperationPrivilegeType operationPrivilegeType) {
        return 1L << operationPrivilegeType.getType();
    }

    private static OperationPrivilegeType convertPrivilegeToOperationType(long privilege) {
        OperationPrivilegeType[] values = OperationPrivilegeType.values();
        for (OperationPrivilegeType type : values) {
            if ((1L << type.getType()) == privilege) {
                return type;
            }
        }
        return null;
    }

    public static boolean isOwnerOfObject(long privilege, int objectType) {
        return privilege == getObjectOwnerPrivilegesByType(objectType);
    }

    public static boolean isOwnerOfObject(long privilege) {
        return privilege == 0;
    }


    private static long convertObjectPrivileges(OperationPrivilegeType[] privileges) {
        long privilege = 0;
        for (OperationPrivilegeType i : privileges) {
            privilege |= (1 << (i.getType()));
        }
        return privilege;
    }

    public static long getObjectOwnerPrivilegesByType(int objectType) {
        ObjectType type = ObjectType.forNum(objectType);
        if (type == null) {
            throw new CatalogServerException(ErrorCode.ROLE_PRIVILEGE_INVALID);
        }
        switch (type) {
            case CATALOG:
                return convertObjectPrivileges(CATALOG_PRIVILEGES);
            case DATABASE:
                return convertObjectPrivileges(DATABASE_PRIVILEGES);
            case TABLE:
                return convertObjectPrivileges(TABLE_PRIVILEGES);
            case VIEW:
                return convertObjectPrivileges(VIEW_PRIVILEGES);
            case SHARE:
                return convertObjectPrivileges(SHARE_PRIVILEGES);
            case ROLE:
                return convertObjectPrivileges(ROLE_PRIVILEGES);
            default:
                return 0;
        }
    }


    private static long[] convertObjectPrivilegesArray(OperationPrivilegeType[] privileges) {
        long[] arr = new long[privileges.length];
        for (int i = 0; i < privileges.length; i++) {
            arr[i] = (1 << (privileges[i].getType()));
        }
        return arr;
    }

    public static long[] getObjectAllPrivilegeArrayByType(int objectType) {
        ObjectType type = ObjectType.forNum(objectType);
        if (type == null) {
            throw new CatalogServerException(ErrorCode.ROLE_PRIVILEGE_INVALID);
        }
        switch (type) {
            case CATALOG:
                return convertObjectPrivilegesArray(CATALOG_PRIVILEGES);
            case DATABASE:
                return convertObjectPrivilegesArray(DATABASE_PRIVILEGES);
            case TABLE:
                return convertObjectPrivilegesArray(TABLE_PRIVILEGES);
            case VIEW:
                return convertObjectPrivilegesArray(VIEW_PRIVILEGES);
            case SHARE:
                return convertObjectPrivilegesArray(SHARE_PRIVILEGES);
            case ROLE:
                return convertObjectPrivilegesArray(ROLE_PRIVILEGES);
            default:
                return null;
        }
    }
}
