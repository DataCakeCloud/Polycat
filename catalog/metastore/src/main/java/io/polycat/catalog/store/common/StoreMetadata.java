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
package io.polycat.catalog.store.common;

import java.util.Arrays;

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.store.fdb.record.DirectoryStringMap;
import io.polycat.catalog.store.protos.AcceleratorObjectNameProto;
import io.polycat.catalog.store.protos.AcceleratorPropertiesProto;
import io.polycat.catalog.store.protos.AcceleratorTemplateProto;
import io.polycat.catalog.store.protos.BackendTaskRecordProto;
import io.polycat.catalog.store.protos.CatalogCommitProto;
import io.polycat.catalog.store.protos.CatalogHistoryProto;
import io.polycat.catalog.store.protos.CatalogProto;
import io.polycat.catalog.store.protos.DataLineageProto;
import io.polycat.catalog.store.protos.DataPartitionSetProto;
import io.polycat.catalog.store.protos.DataPrivilegePolicyProto;
import io.polycat.catalog.store.protos.DatabaseHistoryProto;
import io.polycat.catalog.store.protos.DatabaseProto;
import io.polycat.catalog.store.protos.DelegateProto;
import io.polycat.catalog.store.protos.DroppedObjectNameProto;
import io.polycat.catalog.store.protos.DroppedTableObjectNameProto;
import io.polycat.catalog.store.protos.FunctionRecordProto;
import io.polycat.catalog.store.protos.GlobalShareConsumerProto;
import io.polycat.catalog.store.protos.GlobalSharePropertiesProto;
import io.polycat.catalog.store.protos.GroupPropertiesProto;
import io.polycat.catalog.store.protos.GroupUserProto;
import io.polycat.catalog.store.protos.IndexObjectNameProto;
import io.polycat.catalog.store.protos.IndexPartitionProto;
import io.polycat.catalog.store.protos.IndexPartitionSetProto;
import io.polycat.catalog.store.protos.IndexRecordProto;
import io.polycat.catalog.store.protos.IndexSchemaProto;

import io.polycat.catalog.store.protos.MetaPrivilegePolicyHistoryProto;
import io.polycat.catalog.store.protos.MetaPrivilegePolicyProto;

import io.polycat.catalog.store.protos.ObjectNameMapProto;
import io.polycat.catalog.store.protos.ObjectNameProto;
import io.polycat.catalog.store.protos.RoleObjectNameProto;
import io.polycat.catalog.store.protos.RolePrincipalProto;
import io.polycat.catalog.store.protos.RolePrivilegeProto;
import io.polycat.catalog.store.protos.RolePropertiesProto;
import io.polycat.catalog.store.protos.RoleUserProto;
import io.polycat.catalog.store.protos.ShareConsumerProto;
import io.polycat.catalog.store.protos.ShareDataPrivilegePolicyProto;
import io.polycat.catalog.store.protos.ShareMetaPrivilegePolicyHistoryProto;
import io.polycat.catalog.store.protos.ShareMetaPrivilegePolicyProto;
import io.polycat.catalog.store.protos.ShareObjectNameProto;
import io.polycat.catalog.store.protos.SharePrivilegeProto;
import io.polycat.catalog.store.protos.SharePropertiesProto;
import io.polycat.catalog.store.protos.SubBranchRecordProto;
import io.polycat.catalog.store.protos.TableBaseHistoryProto;
import io.polycat.catalog.store.protos.TableCommitProto;
import io.polycat.catalog.store.protos.TableHistoryProto;
import io.polycat.catalog.store.protos.TableIndexesHistoryProto;
import io.polycat.catalog.store.protos.TableIndexesProto;
import io.polycat.catalog.store.protos.TableProto;
import io.polycat.catalog.store.protos.TableReferenceProto;
import io.polycat.catalog.store.protos.TableSchemaHistoryProto;
import io.polycat.catalog.store.protos.TableStorageHistoryProto;
import io.polycat.catalog.store.protos.UsageProfileProto;
import io.polycat.catalog.store.protos.UserPrivilegeProto;
import io.polycat.catalog.store.protos.UserPropertiesProto;
import io.polycat.catalog.store.protos.ViewObjectNameProto;
import io.polycat.catalog.store.protos.ViewProto;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.Key.Expressions;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.google.protobuf.Descriptors.FileDescriptor;

public enum StoreMetadata {
    //Accelerator
    ACCELERATOR_OBJECT_NAME(DirectoryStringMap.ACCELERATOR_OBJECT_NAME.getName(),
        new String[]{"catalog_id", "database_id", "name"},
        null,
        null,
        AcceleratorObjectNameProto.getDescriptor(),
        "AcceleratorObjectName"),

    ACCELERATOR_PROPERTIES(DirectoryStringMap.ACCELERATOR_PROPERTIES.getName(),
        new String[]{"accelerator_id"},
        null,
        null,
        AcceleratorPropertiesProto.getDescriptor(),
        "AcceleratorProperties"),

    ACCELERATOR_TEMPLATE(DirectoryStringMap.ACCELERATOR_TEMPLATE.getName(),
        new String[]{"accelerator_id", "catalog_id", "database_id", "table_id", "hash_code"},
        null,
        null,
        AcceleratorTemplateProto.getDescriptor(),
        "AcceleratorTemplate"),

    //role
    ROLE_OBJECT_NAME(DirectoryStringMap.ROLE_OBJECT_NAME.getName(),
        new String[]{"name"},
        null,
        null,
        RoleObjectNameProto.getDescriptor(),
        "RoleObjectName"),

    ROLE_PROPERTIES(DirectoryStringMap.ROLE_PROPERTIES.getName(),
        new String[]{"role_id"},
        null,
        null,
        RolePropertiesProto.getDescriptor(),
        "RoleProperties"),

    ROLE_USER(DirectoryStringMap.ROLE_USER.getName(),
        new String[]{"role_id", "user_id"},
        null,
        null,
        RoleUserProto.getDescriptor(),
        "RoleUser"),

    ROLE_PRIVILEGE(DirectoryStringMap.ROLE_PRIVILEGE.getName(),
        new String[]{"role_id", "object_type", "object_id"},
        null,
        null,
        RolePrivilegeProto.getDescriptor(),
        "RolePrivilege"),

    ROLE_PRINCIPAL(DirectoryStringMap.ROLE_PRINCIPAL.getName(),
        new String[]{"role_id", "principal_type", "principal_source", "principal_id"},
        null,
        null,
        RolePrincipalProto.getDescriptor(),
        "RolePrincipal"),

    // user-group
    USER_PROPERTIES(DirectoryStringMap.USER_PROPERTIES.getName(),
        new String[]{"user_id"},
        null,
        null,
        UserPropertiesProto.getDescriptor(),
        "UserProperties"),

    GROUP_PROPERTIES(DirectoryStringMap.GROUP_PROPERTIES.getName(),
        new String[]{"group_id"},
        null,
        null,
        GroupPropertiesProto.getDescriptor(),
        "GroupProperties"),

    GROUP_USER(DirectoryStringMap.GROUP_USER.getName(),
        new String[]{"group_id","user_id"},
        null,
        null,
        GroupUserProto.getDescriptor(),
        "GroupUser"
        ),

    //privilege
    META_PRIVILEGE_POLICY(DirectoryStringMap.META_PRIVILEGE_POLICY_RECORD.getName(),
        new String[]{"principal_type","principal_source","principal_id",
            "object_type", "object_id", "effect", "privilege"},
        new String[]{"policy_id", "principal_type"},
        null,
        MetaPrivilegePolicyProto.getDescriptor(),
        "MetaPrivilegePolicyRecord"),

    META_PRIVILEGE_POLICY_HISTORY(DirectoryStringMap.META_PRIVILEGE_POLICY_HISTORY_RECORD.getName(),
        new String[]{"policy_id"},
        null,
        null,
        MetaPrivilegePolicyHistoryProto.getDescriptor(),
        "MetaPrivilegePolicyHistoryRecord"),

    DATA_PRIVILEGE_POLICY(DirectoryStringMap.DATA_PRIVILEGE_POLICY_RECORD.getName(),
        new String[]{"principal_type","principal_source","principal_id",
            "obs_path", "obs_endpoint"},
        null,
        null,
        DataPrivilegePolicyProto.getDescriptor(),
        "DataPrivilegePolicyRecord"),


    USER_PRIVILEGE_RECORD(DirectoryStringMap.USER_PRIVILEGE_RECORD.getName(),
        new String[]{"user_id", "object_type", "object_id"},
        null,
        null,
        UserPrivilegeProto.getDescriptor(),
        "UserPrivilegeRecord"),

    //object name map
    OBJECT_NAME_MAP_RECORD(DirectoryStringMap.OBJECT_NAME_MAP_RECORD.getName(),
        new String[]{"object_type", "upper_object_name", "object_name"},
        null,
        null,
        ObjectNameMapProto.getDescriptor(),
        "ObjectNameMapRecord"),

    //share
    SHARE_OBJECT_NAME(DirectoryStringMap.SHARE_OBJECT_NAME.getName(),
        new String[]{"name"},
        null,
        null,
        ShareObjectNameProto.getDescriptor(),
        "ShareObjectName"),

    SHARE_PROPERTIES(DirectoryStringMap.SHARE_PROPERTIES.getName(),
        new String[]{"share_id"},
        null,
        null,
        SharePropertiesProto.getDescriptor(),
        "ShareProperties"),

    SHARE_CONSUMER(DirectoryStringMap.SHARE_CONSUMER.getName(),
        new String[]{"account_id"},
        null,
        null,
        ShareConsumerProto.getDescriptor(),
        "ShareConsumer"),

    SHARE_PRIVILEGE(DirectoryStringMap.SHARE_PRIVILEGE.getName(),
        new String[]{"database_id", "object_type", "object_id"},
        null,
        null,
        SharePrivilegeProto.getDescriptor(),
        "SharePrivilege"),

    GLOBAL_SHARE_PROPERTIES(DirectoryStringMap.GLOBAL_SHARE_PROPERTIES.getName(),
        new String[]{"project_id","share_id"},
        null,
        null,
        GlobalSharePropertiesProto.getDescriptor(),
        "GlobalShareProperties"),

    GLOBAL_SHARE_CONSUMER(DirectoryStringMap.GLOBAL_SHARE_CONSUMER.getName(),
        new String[]{"project_id","share_id","consumer_id"},
        null,
        null,
        GlobalShareConsumerProto.getDescriptor(),
        "GlobalShareConsumer"),

    GLOBAL_SHARE_META_PRIVILEGE_POLICY(DirectoryStringMap.GLOBAL_SHARE_META_PRIVILEGE_POLICY_RECORD.getName(),
        new String[]{"project_id","principal_type","principal_source","principal_id","object_type", "object_id",
            "effect","privilege"},
        new String[]{"policy_id","project_id"},
        null,
        ShareMetaPrivilegePolicyProto.getDescriptor(),
        "ShareMetaPrivilegePolicyRecord"),

    GLOBAL_SHARE_META_PRIVILEGE_POLICY_HISTORY(DirectoryStringMap.GLOBAL_SHARE_META_PRIVILEGE_POLICY_HISTORY_RECORD.getName(),
        new String[]{"policy_id"},
        null,
        null,
        ShareMetaPrivilegePolicyHistoryProto.getDescriptor(),
        "ShareMetaPrivilegePolicyHistoryRecord"),

    GLOBAL_SHARE_DATA_PRIVILEGE_POLICY(DirectoryStringMap.GLOBAL_SHARE_DATA_PRIVILEGE_POLICY_RECORD.getName(),
        new String[]{"project_id","principal_type","principal_source","principal_id",
            "obs_path", "obs_endpoint"},
        null,
        null,
        ShareDataPrivilegePolicyProto.getDescriptor(),
        "ShareDataPrivilegePolicyRecord"),

    //partition
    DATA_PARTITION_SET(DirectoryStringMap.DATA_PARTITION_SET.getName(),
        new String[]{"set_id"},
        null,
        null,
        DataPartitionSetProto.getDescriptor(),
        "DataPartitionSet"),

    INDEX_PARTITION_SET(DirectoryStringMap.INDEX_PARTITION_SET.getName(),
        new String[]{"set_id"},
        null,
        null,
        IndexPartitionSetProto.getDescriptor(),
        "IndexPartitionSet"),

    //delegates
    DELEGATE_RECORD(DirectoryStringMap.DELEGATE_RECORD.getName(),
        new String[]{"delegate_name"},
        null,
        null,
        DelegateProto.getDescriptor(),
        "DelegateRecord"),

    CATALOG_DROPPED_NAME(DirectoryStringMap.CATALOG_DROPPED_NAME.getName(),
        new String[]{"type", "parent_id", "name", "object_id"},
        null,
        null,
        DroppedObjectNameProto.getDescriptor(),
        "DroppedObjectName"),

    CATALOG_COMMIT(DirectoryStringMap.CATALOG_COMMIT.getName(),
        new String[]{"commit_id"},
        null,
        new String[]{""},
        CatalogCommitProto.getDescriptor(),
        "CatalogCommit"),

    CATALOG(DirectoryStringMap.CATALOG.getName(),
        new String[]{"catalog_id"},
        new String[]{"name"},
        null,
        CatalogProto.getDescriptor(),
        "Catalog"),

    CATALOG_SUBBRANCH(DirectoryStringMap.CATALOG_SUBBRANCH.getName(),
        new String[]{"catalog_id"},
        null,
        null,
        SubBranchRecordProto.getDescriptor(),
        "SubBranchRecord"),

    CATALOG_HISTORY(DirectoryStringMap.CATALOG_HISTORY.getName(),
        new String[]{"event_id"},
        null,
        new String[]{""},
        CatalogHistoryProto.getDescriptor(),
        "CatalogHistory"),

    //database
    DATABASE_OBJECT_NAME(DirectoryStringMap.DATABASE_OBJECT_NAME.getName(),
        new String[]{"type", "parent_id", "name"},
        null,
        null,
        ObjectNameProto.getDescriptor(),
        "ObjectName"),

    DATABASE_DROPPED_OBJECT_NAME(DirectoryStringMap.DATABASE_DROPPED_OBJECT_NAME.getName(),
        new String[]{"type", "parent_id", "name", "object_id"},
        null,
        null,
        DroppedObjectNameProto.getDescriptor(),
        "DroppedObjectName"),

    DATABASE(DirectoryStringMap.DATABASE.getName(),
        new String[]{"primary_key"},
        null,
        null,
        DatabaseProto.getDescriptor(),
        "Database"),

    DATABASE_HISTORY_RECORD(DirectoryStringMap.DATABASE_HISTORY.getName(),
        new String[]{"event_id"},
        null,
        new String[]{""},
        DatabaseHistoryProto.getDescriptor(),
        "DatabaseHistory"),

    //table
    TABLE(DirectoryStringMap.TABLE.getName(),
        new String[]{"table_id"},
        new String[]{"name"},
        null,
        TableProto.getDescriptor(),
        "Table"),

    TABLE_REFERENCE(DirectoryStringMap.TABLE_REFERENCE.getName(),
        new String[]{"table_id"},
        null,
        null,
        TableReferenceProto.getDescriptor(),
        "TableReference"),

    TABLE_DROPPED_OBJECT_NAME(DirectoryStringMap.TABLE_DROPPED_OBJECT_NAME.getName(),
        new String[]{"name", "object_id"},
        null,
        null,
        DroppedTableObjectNameProto.getDescriptor(),
        "DroppedTableObjectName"),

    TABLE_COMMIT(DirectoryStringMap.TABLE_COMMIT.getName(),
        new String[]{"event_id"},
        null,
        new String[]{""},
        TableCommitProto.getDescriptor(),
        "TableCommit"),

    TABLE_HISTORY(DirectoryStringMap.TABLE_HISTORY.getName(),
        new String[]{"event_id"},
        null,
        new String[]{""},
        TableHistoryProto.getDescriptor(),
        "TableHistory"),

    TABLE_SCHEMA_HISTORY(DirectoryStringMap.TABLE_SCHEMA_HISTORY.getName(),
        new String[]{"event_id"},
        null,
        new String[]{""},
        TableSchemaHistoryProto.getDescriptor(),
        "TableSchemaHistory"),

    TABLE_BASE_HISTORY(DirectoryStringMap.TABLE_BASE_HISTORY.getName(),
        new String[]{"event_id"},
        null,
        new String[]{""},
        TableBaseHistoryProto.getDescriptor(),
        "TableBaseHistory"),

    TABLE_STORAGE_HISTORY(DirectoryStringMap.TABLE_STORAGE_HISTORY.getName(),
        new String[]{"event_id"},
        null,
        new String[]{""},
        TableStorageHistoryProto.getDescriptor(),
        "TableStorageHistory"),

    USAGE_PROFILE_RECORD(DirectoryStringMap.USAGE_PROFILE_RECORD.getName(),
        new String[]{"catalog_id", "database_id", "table_id", "start_time", "op_type"},
        null,
        null,
        UsageProfileProto.getDescriptor(),
        "UsageProfileRecord"),

    TABLE_INDEXES(DirectoryStringMap.TABLE_INDEXES.getName(),
        new String[]{"primary_key"},
        null,
        null,
        TableIndexesProto.getDescriptor(),
        "TableIndexes"),

    TABLE_INDEXES_HISTORY(DirectoryStringMap.TABLE_INDEXES_HISTORY.getName(),
        new String[]{"event_id"},
        null,
        new String[]{""},
        TableIndexesHistoryProto.getDescriptor(),
        "TableIndexesHistory"),

    DATA_LINEAGE_RECORD(DirectoryStringMap.DATA_LINEAGE_RECORD.getName(),
        new String[]{"catalog_id", "database_id", "table_id", "data_source_type",
            "data_source_content", "operation"},
        new String[]{"data_source_type", "data_source_content"},
        null,
        DataLineageProto.getDescriptor(),
        "DataLineageRecord"),

    BACKEND_TASK_RECORD(DirectoryStringMap.BACKEND_TASK_RECORD.getName(),
        new String[]{"task_id"},
        null,
        null,
        BackendTaskRecordProto.getDescriptor(),
        "BackendTaskRecord"),

    //view
    VIEW_NAME(DirectoryStringMap.VIEW_NAME.getName(),
        new String[]{"name"},
        null,
        null,
        ViewObjectNameProto.getDescriptor(),
        "ViewObjectName"),

    DROPPED_VIEW_NAME(DirectoryStringMap.DROPPED_VIEW_NAME.getName(),
        new String[]{"type", "parent_id", "name", "object_id"},
        null,
        null,
        DroppedObjectNameProto.getDescriptor(),
        "DroppedObjectName"),

    FUNCTION_RECORD(DirectoryStringMap.FUNCTION_RECORD.getName(),
        new String[]{"function_name"},
        null,
        null,
        FunctionRecordProto.getDescriptor(),
        "FunctionRecord"),

    VIEW(DirectoryStringMap.VIEW.getName(),
        new String[]{"primary_key"},
        null,
        null,
        ViewProto.getDescriptor(),
        "View"),

    // index related metadata
    INDEX_NAME(DirectoryStringMap.INDEX_OBJECT_NAME.getName(),
        new String[]{"name"},
        null,
        null,
        IndexObjectNameProto.getDescriptor(),
        "IndexObjectName"),

    DROPPED_INDEX_NAME(DirectoryStringMap.DROPPED_INDEX_OBJECT_NAME.getName(),
        new String[]{"type", "parent_id", "name", "object_id"},
        null,
        null,
        DroppedObjectNameProto.getDescriptor(),
        "DroppedObjectName"),

    INDEX_SCHEMA(DirectoryStringMap.INDEX_SCHEMA.getName(),
        new String[]{"primary_key"},
        null,
        null,
        IndexSchemaProto.getDescriptor(),
        "IndexSchema"),

    INDEX_PARTITION(DirectoryStringMap.INDEX_PARTITION.getName(),
        new String[]{"primary_key"},
        null,
        null,
        IndexPartitionProto.getDescriptor(),
        "IndexPartition"),

    INDEX_RECORD(DirectoryStringMap.INDEX_RECORD.getName(),
        new String[]{"primary_key"},
        null,
        null,
        IndexRecordProto.getDescriptor(),
        "IndexRecord");


    private final String name;
    private final String[] primaryIndex;
    private final String[] secondaryIndex;
    private final String[] versionIndex;
    private final FileDescriptor fileDescriptor;
    private final String recordTypeName;

    private final RecordMetaData recordMetaData;

    StoreMetadata(String name, String[] primaryIndex, String[] secondaryIndex, String[] versionIndex,
        FileDescriptor fileDescriptor, String recordTypeName) {
        this.name = name;
        this.primaryIndex = primaryIndex;
        this.secondaryIndex = secondaryIndex;
        this.versionIndex = versionIndex;
        this.fileDescriptor = fileDescriptor;
        this.recordTypeName = recordTypeName;
        this.recordMetaData = generateMetaData();
    }

    RecordMetaData generateMetaData() {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(this.fileDescriptor);
        addPrimaryIndex(builder, this.recordTypeName);
        addSecondaryIndex(builder, this.recordTypeName);
        return builder.build();
    }

    public void addPrimaryIndex(RecordMetaDataBuilder builder, String recordTypeName) {
        KeyExpression expression;
        if (primaryIndex.length == 0) {
            throw new MetaStoreException(ErrorCode.TABLE_PK_NOT_FOUND);
        }
        if (primaryIndex.length == 1) {
            expression = Expressions.field(primaryIndex[0]);
        } else {
            expression = Expressions.concatenateFields(Arrays.asList(primaryIndex));
        }

        builder.getRecordType(recordTypeName).setPrimaryKey(expression);
    }

    private void addSecondaryIndex(RecordMetaDataBuilder builder, String recordTypeName) {
        if (versionIndex != null) {
            builder.setStoreRecordVersions(true);

            if (versionIndex[0].isEmpty()) {
                builder.addIndex(builder.getRecordType(recordTypeName),
                    new Index(name + "-version-index", VersionKeyExpression.VERSION, IndexTypes.VERSION));
            } else {
                builder.addIndex(builder.getRecordType(recordTypeName),
                    new Index(name + "-version-index",
                        Key.Expressions.concat(Key.Expressions.concatenateFields(Arrays.asList(versionIndex)),
                            VersionKeyExpression.VERSION), IndexTypes.VERSION));
            }
        }

        if (secondaryIndex != null) {
            if (secondaryIndex.length >= 2) {
                builder.addIndex(builder.getRecordType(recordTypeName), new Index(name + "-secondary-index",
                    Key.Expressions.concatenateFields(Arrays.asList(secondaryIndex))));
            } else {
                builder.addIndex(recordTypeName, secondaryIndex[0]);
            }
        }
    }

    public RecordMetaData getRecordMetaData() {
        return recordMetaData;
    }

    public String getRecordTypeName() {
        return recordTypeName;
    }

    public String getName() {
        return name;
    }
}
