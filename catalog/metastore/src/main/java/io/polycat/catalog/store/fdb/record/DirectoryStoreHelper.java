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
package io.polycat.catalog.store.fdb.record;

import java.util.UUID;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.IndexIdent;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.ViewIdent;
import io.polycat.catalog.store.common.StoreMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryStoreHelper {

    private static final KeySpaceDirectory objectPartitionSetsSpace =
        DirectoryStringMap.OBJECT_PARTITION_SET.addDirectory()
            .addSubdirectory(DirectoryStringMap.OBJECT_ID.addDirectory()
                .addSubdirectory(DirectoryStringMap.INDEX_PARTITION_SET.addDirectory())
                .addSubdirectory(DirectoryStringMap.DATA_PARTITION_SET.addDirectory()));

    private static final KeySpaceDirectory roleSpace =
        DirectoryStringMap.ROLE_OBJECT.addDirectory()
            .addSubdirectory(DirectoryStringMap.ROLE_OBJECT_NAME.addDirectory())
            .addSubdirectory(DirectoryStringMap.ROLE_PROPERTIES.addDirectory())
            .addSubdirectory(DirectoryStringMap.ROLE_USER.addDirectory())
            .addSubdirectory(DirectoryStringMap.ROLE_PRIVILEGE.addDirectory())
            .addSubdirectory(DirectoryStringMap.ROLE_PRINCIPAL.addDirectory());

    private static final KeySpaceDirectory userGroupSpace =
        DirectoryStringMap.USER_GROUP_OBJECT.addDirectory()
            .addSubdirectory(DirectoryStringMap.USER_PROPERTIES.addDirectory())
            .addSubdirectory(DirectoryStringMap.GROUP_PROPERTIES.addDirectory())
            .addSubdirectory(DirectoryStringMap.GROUP_USER.addDirectory());

    private static final KeySpaceDirectory policySpace =
        DirectoryStringMap.POLICY_OBJECT.addDirectory()
            .addSubdirectory(DirectoryStringMap.META_PRIVILEGE_POLICY_RECORD.addDirectory())
            .addSubdirectory(DirectoryStringMap.META_PRIVILEGE_POLICY_HISTORY_RECORD.addDirectory())
            .addSubdirectory(DirectoryStringMap.DATA_PRIVILEGE_POLICY_RECORD.addDirectory());

    private static final KeySpaceDirectory globalShareSpace =
        DirectoryStringMap.GLOBAL_SHARE_OBJECT.addDirectory()
            .addSubdirectory(DirectoryStringMap.GLOBAL_SHARE_PROPERTIES.addDirectory())
            .addSubdirectory(DirectoryStringMap.GLOBAL_SHARE_CONSUMER.addDirectory())
            .addSubdirectory(DirectoryStringMap.GLOBAL_SHARE_META_PRIVILEGE_POLICY_RECORD.addDirectory())
            .addSubdirectory(DirectoryStringMap.GLOBAL_SHARE_META_PRIVILEGE_POLICY_HISTORY_RECORD.addDirectory())
            .addSubdirectory(DirectoryStringMap.GLOBAL_SHARE_DATA_PRIVILEGE_POLICY_RECORD.addDirectory());


    private static final KeySpaceDirectory shareSpace =
        DirectoryStringMap.SHARE_OBJECT.addDirectory()
            .addSubdirectory(DirectoryStringMap.SHARE_OBJECT_NAME.addDirectory())
            .addSubdirectory(DirectoryStringMap.SHARE_PROPERTIES.addDirectory())
            .addSubdirectory(DirectoryStringMap.SHARE_ID.addDirectory()
                .addSubdirectory(DirectoryStringMap.SHARE_CONSUMER.addDirectory())
                .addSubdirectory(DirectoryStringMap.SHARE_PRIVILEGE.addDirectory()));

    private static final KeySpaceDirectory functionSpace = DirectoryStringMap.FUNCTION_RECORD.addDirectory();

    private static final KeySpaceDirectory viewSpace =
        DirectoryStringMap.VIEW_OBJECT.addDirectory()
            .addSubdirectory(DirectoryStringMap.VIEW_NAME.addDirectory())
            .addSubdirectory(DirectoryStringMap.VIEW_ID.addDirectory()
                .addSubdirectory(DirectoryStringMap.VIEW.addDirectory()));

    private static final KeySpaceDirectory tableSpace =
        DirectoryStringMap.TABLE_OBJECT.addDirectory()
            .addSubdirectory(DirectoryStringMap.TABLE.addDirectory())
            .addSubdirectory(DirectoryStringMap.TABLE_REFERENCE.addDirectory())
            .addSubdirectory(DirectoryStringMap.TABLE_DROPPED_OBJECT_NAME.addDirectory())
            .addSubdirectory(DirectoryStringMap.TABLE_ID.addDirectory()
                .addSubdirectory(DirectoryStringMap.TABLE_ID_VAR.addDirectory()
                    .addSubdirectory(DirectoryStringMap.TABLE_BASE_HISTORY.addDirectory())
                    .addSubdirectory(DirectoryStringMap.TABLE_STORAGE_HISTORY.addDirectory())
                    .addSubdirectory(DirectoryStringMap.TABLE_SCHEMA_HISTORY.addDirectory())
                    .addSubdirectory(DirectoryStringMap.TABLE_COMMIT.addDirectory())
                    .addSubdirectory(DirectoryStringMap.TABLE_HISTORY.addDirectory())
                    .addSubdirectory(DirectoryStringMap.TABLE_INVISIBLE_SCHEMA_HISTORY.addDirectory())
                    .addSubdirectory(DirectoryStringMap.TABLE_INDEXES.addDirectory())
                    .addSubdirectory(DirectoryStringMap.TABLE_INDEXES_HISTORY.addDirectory())));


    // index space directory
    private static final KeySpaceDirectory indexSpace =
        DirectoryStringMap.INDEX_OBJECT.addDirectory()
            .addSubdirectory(DirectoryStringMap.INDEX_OBJECT_NAME.addDirectory())
            .addSubdirectory(DirectoryStringMap.DROPPED_INDEX_OBJECT_NAME.addDirectory())
            .addSubdirectory(DirectoryStringMap.INDEX_ID.addDirectory()
                .addSubdirectory(DirectoryStringMap.INDEX_RECORD.addDirectory())
                .addSubdirectory(DirectoryStringMap.INDEX_SCHEMA.addDirectory())
                .addSubdirectory(DirectoryStringMap.INDEX_PARTITION.addDirectory()));

    private static final KeySpaceDirectory databaseSpace =
            DirectoryStringMap.DATABASE_OBJECT.addDirectory()
                    .addSubdirectory(DirectoryStringMap.DATABASE_OBJECT_NAME.addDirectory())
                    .addSubdirectory(DirectoryStringMap.DATABASE_DROPPED_OBJECT_NAME.addDirectory())
                    .addSubdirectory(DirectoryStringMap.DATABASE_ID.addDirectory()
                            .addSubdirectory(DirectoryStringMap.DATABASE_ID_VAR.addDirectory()
                                    .addSubdirectory(DirectoryStringMap.DATABASE.addDirectory())
                                    .addSubdirectory(DirectoryStringMap.DATABASE_HISTORY.addDirectory())
                                    .addSubdirectory(tableSpace)
                                    .addSubdirectory(functionSpace)
                                    .addSubdirectory(viewSpace)
                                    .addSubdirectory(indexSpace)));


    private static final KeySpaceDirectory catalogSpace =
        DirectoryStringMap.CATALOG_OBJECT.addDirectory()
            .addSubdirectory(DirectoryStringMap.CATALOG.addDirectory())
            //.addSubdirectory(DirectoryStringMap.CATALOG_NAME.addDirectory())
            .addSubdirectory(DirectoryStringMap.CATALOG_DROPPED_NAME.addDirectory())
            .addSubdirectory(DirectoryStringMap.CATALOG_ID.addDirectory()
                .addSubdirectory(DirectoryStringMap.CATALOG_ID_VAR.addDirectory()
                    .addSubdirectory(DirectoryStringMap.CATALOG_COMMIT.addDirectory())
                    .addSubdirectory(DirectoryStringMap.CATALOG_SUBBRANCH.addDirectory())
                    .addSubdirectory(DirectoryStringMap.CATALOG_HISTORY.addDirectory())
                    .addSubdirectory(databaseSpace)));


    private static final KeySpaceDirectory usageProfile = DirectoryStringMap.USAGE_PROFILE_RECORD.addDirectory();

    private static final KeySpaceDirectory dataLineage = DirectoryStringMap.DATA_LINEAGE_RECORD.addDirectory();

    private static final KeySpaceDirectory delegate = DirectoryStringMap.DELEGATE_RECORD.addDirectory();

    private static final KeySpaceDirectory objectNameMap = DirectoryStringMap.OBJECT_NAME_MAP_RECORD.addDirectory();

    private static final KeySpaceDirectory userPrivilege = DirectoryStringMap.USER_PRIVILEGE_RECORD.addDirectory();

    private static final KeySpaceDirectory accelerator =
        DirectoryStringMap.ACCELERATOR_OBJECT.addDirectory()
            .addSubdirectory(DirectoryStringMap.ACCELERATOR_OBJECT_NAME.addDirectory())
            .addSubdirectory(DirectoryStringMap.ACCELERATOR_PROPERTIES.addDirectory())
            .addSubdirectory(DirectoryStringMap.ACCELERATOR_TEMPLATE.addDirectory());

    // Top-level object

    private static final KeySpace backendTaskObject = new KeySpace(DirectoryStringMap.BACKEND_TASK_RECORD.addDirectory());

    private static final KeySpace globalShareObject = new KeySpace(globalShareSpace);

    private static final KeySpace tenantRoot =
        new KeySpace(
            new KeySpaceDirectory(DirectoryStringMap.PROJECT_ID.getName(), KeyType.STRING)
                .addSubdirectory(objectPartitionSetsSpace)
                .addSubdirectory(userPrivilege)
                .addSubdirectory(userGroupSpace)
                .addSubdirectory(policySpace)
                .addSubdirectory(roleSpace)
                .addSubdirectory(shareSpace)
                .addSubdirectory(delegate)
                .addSubdirectory(objectNameMap)
                .addSubdirectory(usageProfile)
                .addSubdirectory(accelerator)
                .addSubdirectory(dataLineage)
                .addSubdirectory(catalogSpace));

    public static final Logger logger = LoggerFactory.getLogger(RecordStoreHelper.class);

    static FDBRecordStore getStore(FDBRecordContext context, KeySpacePath keySpacePath, RecordMetaData metaData) {
        return FDBRecordStore.newBuilder().setMetaDataProvider(metaData).setContext(context)
            .setKeySpacePath(keySpacePath).createOrOpen();
    }

    public static FDBRecordStore getTableBaseHistoryStore(FDBRecordContext context, TableIdent tableIdent) {
        KeySpacePath keySpacePath = getTableKeyPath(tableIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.TABLE_BASE_HISTORY.getName()),
            StoreMetadata.TABLE_BASE_HISTORY.getRecordMetaData());
    }

    public static FDBRecordStore getTableIndexesHistoryStore(FDBRecordContext context, TableIdent tableIdent) {
        KeySpacePath keySpacePath = getTableKeyPath(tableIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.TABLE_INDEXES_HISTORY.getName()),
            StoreMetadata.TABLE_INDEXES_HISTORY.getRecordMetaData());
    }

    public static FDBRecordStore getTableIndexesStore(FDBRecordContext context, TableIdent tableIdent) {
        KeySpacePath keySpacePath = getTableKeyPath(tableIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.TABLE_INDEXES.getName()),
            StoreMetadata.TABLE_INDEXES.getRecordMetaData());
    }

    public static FDBRecordStore getTableStorageHistoryStore(FDBRecordContext context, TableIdent tableIdent) {
        KeySpacePath keySpacePath = getTableKeyPath(tableIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.TABLE_STORAGE_HISTORY.getName()),
            StoreMetadata.TABLE_STORAGE_HISTORY.getRecordMetaData());
    }

    public static FDBRecordStore getTableHistoryStore(FDBRecordContext context, TableIdent tableIdent) {
        KeySpacePath keySpacePath = getTableKeyPath(tableIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.TABLE_HISTORY.getName()),
            StoreMetadata.TABLE_HISTORY.getRecordMetaData());
    }

    public static FDBRecordStore getTableSchemaHistoryStore(FDBRecordContext context, TableIdent tableIdent) {
        KeySpacePath keySpacePath = getTableKeyPath(tableIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.TABLE_SCHEMA_HISTORY.getName()),
            StoreMetadata.TABLE_SCHEMA_HISTORY.getRecordMetaData());
    }

    public static FDBRecordStore getTableCommitStore(FDBRecordContext context, TableIdent tableIdent) {
        KeySpacePath keySpacePath = getTableKeyPath(tableIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.TABLE_COMMIT.getName()),
            StoreMetadata.TABLE_COMMIT.getRecordMetaData());
    }

    public static FDBRecordStore getTableStore(FDBRecordContext context, DatabaseIdent databaseIdent) {
        KeySpacePath keySpacePath = getDatabaseKeyPath(databaseIdent).add(DirectoryStringMap.TABLE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.TABLE.getName()),
            StoreMetadata.TABLE.getRecordMetaData());
    }

    public static FDBRecordStore getTableStore(FDBRecordContext context, TableIdent tableIdent) {
        KeySpacePath keySpacePath = getDatabaseKeyPath(tableIdent).add(DirectoryStringMap.TABLE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.TABLE.getName()),
            StoreMetadata.TABLE.getRecordMetaData());
    }

    public static FDBRecordStore getTableReferenceStore(FDBRecordContext context, DatabaseIdent databaseIdent) {
        KeySpacePath keySpacePath = getDatabaseKeyPath(databaseIdent).add(DirectoryStringMap.TABLE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.TABLE_REFERENCE.getName()),
            StoreMetadata.TABLE_REFERENCE.getRecordMetaData());
    }

    public static FDBRecordStore getTableReferenceStore(FDBRecordContext context, TableIdent tableIdent) {
        KeySpacePath keySpacePath = getDatabaseKeyPath(tableIdent).add(DirectoryStringMap.TABLE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.TABLE_REFERENCE.getName()),
            StoreMetadata.TABLE_REFERENCE.getRecordMetaData());
    }

    public static FDBRecordStore getUsageProfileStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath =  getProjectKeyPath(projectId).add(DirectoryStringMap.USAGE_PROFILE_RECORD.getName());
        return getStore(context, keySpacePath, StoreMetadata.USAGE_PROFILE_RECORD.getRecordMetaData());
    }

    public static FDBRecordStore getDataLineageStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.DATA_LINEAGE_RECORD.getName());
        return getStore(context, keySpacePath, StoreMetadata.DATA_LINEAGE_RECORD.getRecordMetaData());
    }

    public static FDBRecordStore getDatabaseObjectNameStore(FDBRecordContext context, CatalogIdent catalogIdent) {
        KeySpacePath keySpacePath = getCatalogKeyPath(catalogIdent).add(DirectoryStringMap.DATABASE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.DATABASE_OBJECT_NAME.getName()),
            StoreMetadata.DATABASE_OBJECT_NAME.getRecordMetaData());
    }

    public static FDBRecordStore getDroppedDatabaseObjectNameStore(FDBRecordContext context,
        CatalogIdent catalogIdent) {
        KeySpacePath keySpacePath = getCatalogKeyPath(catalogIdent).add(DirectoryStringMap.DATABASE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.DATABASE_DROPPED_OBJECT_NAME.getName()),
            StoreMetadata.DATABASE_DROPPED_OBJECT_NAME.getRecordMetaData());
    }

    /*public static FDBRecordStore getCatalogObjectNameStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.CATALOG_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.CATALOG_NAME.getName()),
            StoreMetadata.CATALOG_NAME.getRecordMetaData());
    }*/

    public static FDBRecordStore getDroppedTableObjectNameStore(FDBRecordContext context, DatabaseIdent databaseIdent) {
        KeySpacePath keySpacePath = getDatabaseKeyPath(databaseIdent).add(DirectoryStringMap.TABLE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.TABLE_DROPPED_OBJECT_NAME.getName()),
            StoreMetadata.TABLE_DROPPED_OBJECT_NAME.getRecordMetaData());
    }

    public static FDBRecordStore getCatalogStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.CATALOG_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.CATALOG.getName()),
            StoreMetadata.CATALOG.getRecordMetaData());
    }

    public static FDBRecordStore getCatalogSubBranchStore(FDBRecordContext context, CatalogIdent catalogIdent) {
        KeySpacePath keySpacePath = getCatalogKeyPath(catalogIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.CATALOG_SUBBRANCH.getName()),
            StoreMetadata.CATALOG_SUBBRANCH.getRecordMetaData());
    }

    public static FDBRecordStore getDatabaseStore(FDBRecordContext context, DatabaseIdent databaseIdent) {
        KeySpacePath keySpacePath = getDatabaseKeyPath(databaseIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.DATABASE.getName()),
            StoreMetadata.DATABASE.getRecordMetaData());
    }

    public static FDBRecordStore getDatabaseHistoryStore(FDBRecordContext context, DatabaseIdent databaseIdent) {
        KeySpacePath keySpacePath = getDatabaseKeyPath(databaseIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.DATABASE_HISTORY.getName()),
            StoreMetadata.DATABASE_HISTORY_RECORD.getRecordMetaData());
    }

    public static FDBRecordStore getCatalogCommitStore(FDBRecordContext context, CatalogIdent catalogIdent) {
        KeySpacePath keySpacePath = getCatalogKeyPath(catalogIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.CATALOG_COMMIT.getName()),
            StoreMetadata.CATALOG_COMMIT.getRecordMetaData());
    }

    public static FDBRecordStore getCatalogHistoryStore(FDBRecordContext context, CatalogIdent catalogIdent) {
        KeySpacePath keySpacePath = getCatalogKeyPath(catalogIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.CATALOG_HISTORY.getName()),
            StoreMetadata.CATALOG_HISTORY.getRecordMetaData());
    }

    public static FDBRecordStore getUserPrivilegeStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.USER_PRIVILEGE_RECORD.getName());
        return getStore(context, keySpacePath, StoreMetadata.USER_PRIVILEGE_RECORD.getRecordMetaData());
    }

    public static FDBRecordStore getRoleObjectNameStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.ROLE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.ROLE_OBJECT_NAME.getName()),
            StoreMetadata.ROLE_OBJECT_NAME.getRecordMetaData());
    }

    public static FDBRecordStore getRolePropertiesStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.ROLE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.ROLE_PROPERTIES.getName()),
            StoreMetadata.ROLE_PROPERTIES.getRecordMetaData());
    }

    public static FDBRecordStore getRoleUserStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.ROLE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.ROLE_USER.getName()), StoreMetadata.ROLE_USER
            .getRecordMetaData());
    }

    public static FDBRecordStore getRolePrincipalStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.ROLE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.ROLE_PRINCIPAL.getName()),
            StoreMetadata.ROLE_PRINCIPAL.getRecordMetaData());
    }

    public static FDBRecordStore getRolePrivilegeStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.ROLE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.ROLE_PRIVILEGE.getName()),
            StoreMetadata.ROLE_PRIVILEGE.getRecordMetaData());
    }

    public static FDBRecordStore getUserPropertiesStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.USER_GROUP_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.USER_PROPERTIES.getName()),
            StoreMetadata.USER_PROPERTIES.getRecordMetaData());
    }
    public static FDBRecordStore getGroupPropertiesStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.USER_GROUP_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.GROUP_PROPERTIES.getName()),
            StoreMetadata.GROUP_PROPERTIES.getRecordMetaData());
    }
    public static FDBRecordStore getGroupUserStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.USER_GROUP_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.GROUP_USER.getName()),
            StoreMetadata.GROUP_USER.getRecordMetaData());
    }

    public static FDBRecordStore getMetaPrivilegePolicyStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.POLICY_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.META_PRIVILEGE_POLICY_RECORD.getName()),
            StoreMetadata.META_PRIVILEGE_POLICY.getRecordMetaData());
    }

    public static FDBRecordStore getMetaPrivilegePolicyHistoryStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.POLICY_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.META_PRIVILEGE_POLICY_HISTORY_RECORD.getName()),
            StoreMetadata.META_PRIVILEGE_POLICY_HISTORY.getRecordMetaData());
    }

    public static FDBRecordStore getDataPrivilegePolicyStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.POLICY_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.DATA_PRIVILEGE_POLICY_RECORD.getName()),
            StoreMetadata.DATA_PRIVILEGE_POLICY.getRecordMetaData());
    }

    public static FDBRecordStore getGlobalSharePropertiesStore(FDBRecordContext context) {
        KeySpacePath keySpacePath = globalShareObject.path(DirectoryStringMap.GLOBAL_SHARE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.GLOBAL_SHARE_PROPERTIES.getName()),
            StoreMetadata.GLOBAL_SHARE_PROPERTIES.getRecordMetaData());
    }

    public static FDBRecordStore getGlobalShareConsumerStore(FDBRecordContext context) {
        KeySpacePath keySpacePath = globalShareObject.path(DirectoryStringMap.GLOBAL_SHARE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.GLOBAL_SHARE_CONSUMER.getName()),
            StoreMetadata.GLOBAL_SHARE_CONSUMER.getRecordMetaData());
    }

    public static FDBRecordStore getShareMetaPrivilegePolicyStore(FDBRecordContext context) {
        KeySpacePath keySpacePath = globalShareObject.path(DirectoryStringMap.GLOBAL_SHARE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.GLOBAL_SHARE_META_PRIVILEGE_POLICY_RECORD.getName()),
            StoreMetadata.GLOBAL_SHARE_META_PRIVILEGE_POLICY.getRecordMetaData());
    }

    public static FDBRecordStore getShareMetaPrivilegePolicyHistoryStore(FDBRecordContext context) {
        KeySpacePath keySpacePath = globalShareObject.path(DirectoryStringMap.GLOBAL_SHARE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.GLOBAL_SHARE_META_PRIVILEGE_POLICY_HISTORY_RECORD.getName()),
            StoreMetadata.GLOBAL_SHARE_META_PRIVILEGE_POLICY_HISTORY.getRecordMetaData());
    }

    public static FDBRecordStore getShareDataPrivilegePolicyStore(FDBRecordContext context) {
        KeySpacePath keySpacePath = globalShareObject.path(DirectoryStringMap.GLOBAL_SHARE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.GLOBAL_SHARE_DATA_PRIVILEGE_POLICY_RECORD.getName()),
            StoreMetadata.GLOBAL_SHARE_DATA_PRIVILEGE_POLICY.getRecordMetaData());
    }

    public static FDBRecordStore getViewReferenceStore(FDBRecordContext context, ViewIdent viewIdent) {
        KeySpacePath keySpacePath = getViewKeyPath(viewIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.VIEW.getName()), StoreMetadata.VIEW
            .getRecordMetaData());
    }

    public static FDBRecordStore getViewObjectNameStore(FDBRecordContext context, DatabaseIdent databaseIdent) {
        KeySpacePath keySpacePath = getDatabaseKeyPath(databaseIdent).add(DirectoryStringMap.VIEW_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.VIEW_NAME.getName()), StoreMetadata.VIEW_NAME
            .getRecordMetaData());
    }

    public static FDBRecordStore getFunctionStore(FDBRecordContext context, DatabaseIdent databaseIdent) {
        KeySpacePath keySpacePath = getDatabaseKeyPath(databaseIdent).add(DirectoryStringMap.FUNCTION_RECORD.getName());
        return getStore(context, keySpacePath, StoreMetadata.FUNCTION_RECORD.getRecordMetaData());
    }

    public static FDBRecordStore getAcceleratorObjectNameStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.ACCELERATOR_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.ACCELERATOR_OBJECT_NAME.getName()),
            StoreMetadata.ACCELERATOR_OBJECT_NAME.getRecordMetaData());
    }

    public static FDBRecordStore getAcceleratorPropertiesStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.ACCELERATOR_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.ACCELERATOR_PROPERTIES.getName()),
            StoreMetadata.ACCELERATOR_PROPERTIES.getRecordMetaData());
    }

    public static FDBRecordStore getAcceleratorTemplateStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.ACCELERATOR_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.ACCELERATOR_TEMPLATE.getName()),
            StoreMetadata.ACCELERATOR_TEMPLATE.getRecordMetaData());
    }

    public static FDBRecordStore getDelegateStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath =  getProjectKeyPath(projectId).add(DirectoryStringMap.DELEGATE_RECORD.getName());
        return getStore(context, keySpacePath, StoreMetadata.DELEGATE_RECORD.getRecordMetaData());
    }

    public static FDBRecordStore getShareObjectNameStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.SHARE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.SHARE_OBJECT_NAME.getName()),
            StoreMetadata.SHARE_OBJECT_NAME.getRecordMetaData());
    }

    public static FDBRecordStore getSharePropertiesStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath = getProjectKeyPath(projectId).add(DirectoryStringMap.SHARE_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.SHARE_PROPERTIES.getName()), StoreMetadata.SHARE_PROPERTIES
            .getRecordMetaData());
    }

    public static FDBRecordStore getShareConsumerStore(FDBRecordContext context, String projectId, String shareId) {
        KeySpacePath keySpacePath = getShareKeyPath(projectId, shareId);
        return getStore(context, keySpacePath.add(DirectoryStringMap.SHARE_CONSUMER.getName()),
            StoreMetadata.SHARE_CONSUMER.getRecordMetaData());
    }

    public static FDBRecordStore getSharePrivilegeStore(FDBRecordContext context, String projectId, String shareId) {
        KeySpacePath keySpacePath = getShareKeyPath(projectId, shareId);
        return getStore(context, keySpacePath.add(DirectoryStringMap.SHARE_PRIVILEGE.getName()),
            StoreMetadata.SHARE_PRIVILEGE.getRecordMetaData());
    }

    public static FDBRecordStore getObjectNameMapStore(FDBRecordContext context, String projectId) {
        KeySpacePath keySpacePath =  getProjectKeyPath(projectId).add(DirectoryStringMap.OBJECT_NAME_MAP_RECORD.getName());
        return getStore(context, keySpacePath, StoreMetadata.OBJECT_NAME_MAP_RECORD.getRecordMetaData());
    }

    public static FDBRecordStore getBackendTaskObjectStore(FDBRecordContext context) {
        KeySpacePath keySpacePath = backendTaskObject.path(DirectoryStringMap.BACKEND_TASK_RECORD.getName());
        return getStore(context, keySpacePath, StoreMetadata.BACKEND_TASK_RECORD.getRecordMetaData());
    }

    // object sets begin
    public static FDBRecordStore getDataPartitionSetStore(FDBRecordContext context, TableIdent tableIdent) {
        KeySpacePath keySpacePath = getObjectSetPath(tableIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.DATA_PARTITION_SET.getName()),
            StoreMetadata.DATA_PARTITION_SET.getRecordMetaData());
    }

    public static FDBRecordStore getIndexPartitionSetStore(FDBRecordContext context, TableIdent tableIdent) {
        KeySpacePath keySpacePath = getObjectSetPath(tableIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.INDEX_PARTITION_SET.getName()),
            StoreMetadata.INDEX_PARTITION_SET.getRecordMetaData());
    }

    private static KeySpacePath getTableKeyPath(TableIdent tableIdent) {
        return tenantRoot.path(DirectoryStringMap.PROJECT_ID.getName(), tableIdent.getProjectId())
            .add(DirectoryStringMap.CATALOG_OBJECT.getName())
            .add(DirectoryStringMap.CATALOG_ID.getName())
            .add(DirectoryStringMap.CATALOG_ID_VAR.getName(), tableIdent.getCatalogId())
            .add(DirectoryStringMap.DATABASE_OBJECT.getName())
            .add(DirectoryStringMap.DATABASE_ID.getName())
            .add(DirectoryStringMap.DATABASE_ID_VAR.getName(), tableIdent.getDatabaseId())
            .add(DirectoryStringMap.TABLE_OBJECT.getName())
            .add(DirectoryStringMap.TABLE_ID.getName())
            .add(DirectoryStringMap.TABLE_ID_VAR.getName(), tableIdent.getTableId());
    }

    private static KeySpacePath getDatabaseKeyPath(DatabaseIdent databaseIdent) {
        return tenantRoot.path(DirectoryStringMap.PROJECT_ID.getName(), databaseIdent.getProjectId())
            .add(DirectoryStringMap.CATALOG_OBJECT.getName())
            .add(DirectoryStringMap.CATALOG_ID.getName())
            .add(DirectoryStringMap.CATALOG_ID_VAR.getName(), databaseIdent.getCatalogId())
            .add(DirectoryStringMap.DATABASE_OBJECT.getName())
            .add(DirectoryStringMap.DATABASE_ID.getName())
            .add(DirectoryStringMap.DATABASE_ID_VAR.getName(), databaseIdent.getDatabaseId());
    }

    private static KeySpacePath getDatabaseKeyPath(TableIdent tableIdent) {
        return tenantRoot.path(DirectoryStringMap.PROJECT_ID.getName(), tableIdent.getProjectId())
            .add(DirectoryStringMap.CATALOG_OBJECT.getName())
            .add(DirectoryStringMap.CATALOG_ID.getName())
            .add(DirectoryStringMap.CATALOG_ID_VAR.getName(), tableIdent.getCatalogId())
            .add(DirectoryStringMap.DATABASE_OBJECT.getName())
            .add(DirectoryStringMap.DATABASE_ID.getName())
            .add(DirectoryStringMap.DATABASE_ID_VAR.getName(), tableIdent.getDatabaseId());
    }

    private static KeySpacePath getCatalogKeyPath(CatalogIdent catalogIdent) {
        return tenantRoot.path(DirectoryStringMap.PROJECT_ID.getName(), catalogIdent.getProjectId())
            .add(DirectoryStringMap.CATALOG_OBJECT.getName())
            .add(DirectoryStringMap.CATALOG_ID.getName())
            .add(DirectoryStringMap.CATALOG_ID_VAR.getName(), catalogIdent.getCatalogId());
    }

    private static KeySpacePath getShareKeyPath(String projectId, String shareId) {
        return tenantRoot.path(DirectoryStringMap.PROJECT_ID.getName(), projectId)
            .add(DirectoryStringMap.SHARE_OBJECT.getName())
            .add(DirectoryStringMap.SHARE_ID.getName(), UUID.fromString(shareId));
    }

    private static KeySpacePath getViewKeyPath(ViewIdent viewIdent) {
        return tenantRoot.path(DirectoryStringMap.PROJECT_ID.getName(), viewIdent.getProjectId())
            .add(DirectoryStringMap.CATALOG_OBJECT.getName())
            .add(DirectoryStringMap.CATALOG_ID.getName())
            .add(DirectoryStringMap.CATALOG_ID_VAR.getName(), viewIdent.getCatalogId())
            .add(DirectoryStringMap.DATABASE_OBJECT.getName())
            .add(DirectoryStringMap.DATABASE_ID.getName())
            .add(DirectoryStringMap.DATABASE_ID_VAR.getName(), viewIdent.getDatabaseId())
            .add(DirectoryStringMap.VIEW_OBJECT.getName())
            .add(DirectoryStringMap.VIEW_ID.getName(), UUID.fromString(viewIdent.getViewId()));
    }

    private static KeySpacePath getObjectSetPath(TableIdent tableIdent) {
        return tenantRoot.path(DirectoryStringMap.PROJECT_ID.getName(), tableIdent.getProjectId())
            .add(DirectoryStringMap.OBJECT_PARTITION_SET.getName())
            .add(DirectoryStringMap.OBJECT_ID.getName(), tableIdent.getTableId());
    }

    private static KeySpacePath getProjectKeyPath(String projectId) {
        return tenantRoot.path(DirectoryStringMap.PROJECT_ID.getName(), projectId);
    }

    public static KeySpace getRoot() {
        return tenantRoot;
    }

    public static FDBRecordStore getIndexObjectNameStore(FDBRecordContext context,
        DatabaseIdent databaseIdent) {
        KeySpacePath keySpacePath =
            getDatabaseKeyPath(databaseIdent).add(DirectoryStringMap.INDEX_OBJECT.getName());
        return getStore(context, keySpacePath.add(DirectoryStringMap.INDEX_OBJECT_NAME.getName()),
            StoreMetadata.INDEX_NAME.getRecordMetaData());
    }

    public static FDBRecordStore getDroppedIndexObjectNameStore(FDBRecordContext context,
        DatabaseIdent databaseIdent) {
        KeySpacePath keySpacePath =
            getDatabaseKeyPath(databaseIdent).add(DirectoryStringMap.INDEX_OBJECT.getName());
        return getStore(context,
            keySpacePath.add(DirectoryStringMap.DROPPED_INDEX_OBJECT_NAME.getName()),
            StoreMetadata.DROPPED_INDEX_NAME.getRecordMetaData());
    }

    public static FDBRecordStore getIndexRecordStore(FDBRecordContext context,
        IndexIdent indexIdent) {
        KeySpacePath keySpacePath = getIndexKeyPath(indexIdent);
        return getStore(context, keySpacePath.add(DirectoryStringMap.INDEX_RECORD.getName()),
            StoreMetadata.INDEX_RECORD.getRecordMetaData());
    }

    public static FDBRecordStore getIndexSchemaStore(FDBRecordContext context, IndexIdent indexIdent) {
        KeySpacePath keySpacePath = getIndexKeyPath(indexIdent);
        return getStore(context,
            keySpacePath.add(DirectoryStringMap.INDEX_SCHEMA.getName()),
            StoreMetadata.INDEX_SCHEMA.getRecordMetaData());
    }

    public static FDBRecordStore getIndexPartitionStore(FDBRecordContext context, IndexIdent indexIdent) {
        KeySpacePath keySpacePath = getIndexKeyPath(indexIdent);
        return getStore(context,
            keySpacePath.add(DirectoryStringMap.INDEX_PARTITION.getName()),
            StoreMetadata.INDEX_PARTITION.getRecordMetaData());
    }

    private static KeySpacePath getIndexKeyPath(IndexIdent indexIdent) {
        return tenantRoot.path(DirectoryStringMap.PROJECT_ID.getName(), indexIdent.getProjectId())
            .add(DirectoryStringMap.CATALOG_OBJECT.getName())
            .add(DirectoryStringMap.CATALOG_ID.getName())
            .add(DirectoryStringMap.CATALOG_ID_VAR.getName(), indexIdent.getCatalogId())
            .add(DirectoryStringMap.DATABASE_OBJECT.getName())
            .add(DirectoryStringMap.DATABASE_ID.getName())
            .add(DirectoryStringMap.DATABASE_ID_VAR.getName(), indexIdent.getDatabaseId())
            .add(DirectoryStringMap.INDEX_OBJECT.getName())
            .add(DirectoryStringMap.INDEX_ID.getName(), UUID.fromString(indexIdent.getIndexId()));
    }
}
