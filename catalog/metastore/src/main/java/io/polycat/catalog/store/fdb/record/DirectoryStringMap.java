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

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType;
import lombok.Getter;

@Getter
public enum DirectoryStringMap {
    PROJECT_ID("ProjectId", KeyType.STRING),
    // accelerator
    ACCELERATOR_OBJECT("AcceleratorObject", KeyType.STRING, "AccelO"),
    ACCELERATOR_OBJECT_NAME("AcceleratorObjectName", KeyType.STRING, "AccelON"),
    ACCELERATOR_PROPERTIES("AcceleratorProperties", KeyType.STRING, "AccelPR"),
    ACCELERATOR_TEMPLATE("AcceleratorTemplate", KeyType.STRING, "AccelT"),

    // role
    ROLE_OBJECT("RoleObject", KeyType.STRING, "RoleO"),
    ROLE_OBJECT_NAME("RoleObjectName", KeyType.STRING, "RoleON"),
    ROLE_PROPERTIES("RoleProperties", KeyType.STRING, "RolePR"),
    ROLE_USER("RoleUser", KeyType.STRING, "RoleU"),
    ROLE_PRIVILEGE("RolePrivilege", KeyType.STRING, "RoleP"),
    ROLE_PRINCIPAL("RolePrincipal", KeyType.STRING, "RolePrincipal"),

    // userGroup
    USER_GROUP_OBJECT("UserGroupObject", KeyType.STRING, "UserGroupO"),
    USER_PROPERTIES("UserProperties", KeyType.STRING, "UserPR"),
    GROUP_PROPERTIES("GroupProperties", KeyType.STRING, "GroupPR"),
    GROUP_USER("GroupUser", KeyType.STRING, "GroupU"),

    // privilege
    POLICY_OBJECT("PolicyObject", KeyType.STRING, "PolicyO"),
    META_PRIVILEGE_POLICY_RECORD("MetaPrivilegePolicyRecord", KeyType.STRING, "MetaPrivPolR"),
    META_PRIVILEGE_POLICY_HISTORY_RECORD("MetaPrivilegePolicyHistoryRecord", KeyType.STRING, "MetaPrivPolHR"),
    DATA_PRIVILEGE_POLICY_RECORD("DataPrivilegePolicyRecord", KeyType.STRING, "DataPrivPolR"),

    // global share
    GLOBAL_SHARE_OBJECT("GlobalSharePolicyObject", KeyType.STRING, "GShareO"),
    GLOBAL_SHARE_PROPERTIES("GlobalShareProperties", KeyType.STRING, "GSharePR"),
    GLOBAL_SHARE_CONSUMER("GlobalShareConsumer", KeyType.STRING, "GShareC"),
    GLOBAL_SHARE_META_PRIVILEGE_POLICY_RECORD("GlobalShareMetaPrivilegePolicyRecord", KeyType.STRING, "GShareMetaPrivPolR"),
    GLOBAL_SHARE_META_PRIVILEGE_POLICY_HISTORY_RECORD("GlobalShareMetaPrivilegePolicyHistoryRecord", KeyType.STRING, "GShareMetaPrivPolHR"),
    GLOBAL_SHARE_DATA_PRIVILEGE_POLICY_RECORD("GlobalShareDataPrivilegePolicyRecord", KeyType.STRING, "GShareDataPrivPolR"),


    USER_PRIVILEGE_RECORD("UserPrivilegeRecord", KeyType.STRING, "UPrivR"),

    //object name map
    OBJECT_NAME_MAP_RECORD("ObjectNameMapRecord", KeyType.STRING, "ObjNM"),

    // share
    SHARE_OBJECT_NAME("ShareObjectName", KeyType.STRING, "ShareON"),
    SHARE_OBJECT("ShareObject", KeyType.STRING, "ShareO"),
    SHARE_PROPERTIES("ShareProperties", KeyType.STRING, "SharePR"),
    SHARE_ID("ShareId", KeyType.UUID),
    SHARE_CONSUMER("ShareConsumer", KeyType.STRING, "ShareC"),
    SHARE_PRIVILEGE("SharePrivilege", KeyType.STRING, "ShareP"),

    // object partition set
    OBJECT_PARTITION_SET("PartitionSet", KeyType.STRING, "PartS"),
    OBJECT_ID("ObjectId", KeyType.STRING),
    DATA_PARTITION_SET("DataPartitionSet", KeyType.STRING, "PartSD"),
    INDEX_PARTITION_SET("IndexPartitionSet", KeyType.STRING, "PartSI"),

    // UVO object
    UVO_OBJECT("UVO", KeyType.STRING, "Uvo"),
    UVO_OBJECT_NAME("UVOObjectName", KeyType.STRING, "UvoN"),
    DROPPED_UVO_OBJECT_NAME("DroppedUVOObjectName", KeyType.STRING, "UvoDON"),
    UVO_ID("UVOID", KeyType.UUID),
    UVO_COMMIT("UVOCommit", KeyType.STRING, "UvoC"),
    UVO_PARTITION("UVOPartition", KeyType.STRING, "UvoPart"),
    UVO_PARTITION_HISTORY("UVOPartitionHistory", KeyType.STRING, "UvoPartH"),
    UVO_SCHEMA("UVOSchema", KeyType.STRING, "UvoSc"),
    UVO_SCHEMA_HISTORY("UVOSchemaHistory", KeyType.STRING, "UvoScH"),
    UVO_PROPERTIES("UVOProperties", KeyType.STRING, "UvoProp"),
    UVO_PROPERTIES_HISTORY("UVOPropertiesHistory", KeyType.STRING, "UvoPropH"),
    UVO_USAGE_PROFILE("UVOUsageProfileRecord", KeyType.STRING, "UvoUP"),
    UVO_DATA_LINEAGE_RECORD("UVODataLineageRecord", KeyType.STRING, "UvoDLR"),

    // delegate
    DELEGATE_RECORD("DelegateRecord", KeyType.STRING, "DelegateR"),

    // catalog
    CATALOG_OBJECT("CatalogObject", KeyType.STRING, "CtlgO"),
    CATALOG("Catalog", KeyType.STRING, "Ctlg"),
    CATALOG_NAME("CatalogName", KeyType.STRING, "CtlgName"),
    CATALOG_DROPPED_NAME("CatalogDroppedName", KeyType.STRING, "CtlgDN"),
    CATALOG_ID("CatalogId", KeyType.STRING, "CtlgId"),
    CATALOG_ID_VAR("CatalogIdVar", KeyType.STRING),
    CATALOG_COMMIT("CatalogCommit", KeyType.STRING, "CtlgC"),
    CATALOG_SUBBRANCH("CatalogSubBranch", KeyType.STRING, "CtlgSB"),
    CATALOG_HISTORY("CatalogHistory", KeyType.STRING, "CtlgH"),

    // database
    DATABASE_OBJECT("DatabaseObject", KeyType.STRING, "DbO"),
    DATABASE_ID("DatabaseId", KeyType.STRING, "DbId"),
    DATABASE_ID_VAR("DatabaseIdVar", KeyType.STRING),
    DATABASE_OBJECT_NAME("DatabaseObjectName", KeyType.STRING, "DbON"),
    DATABASE_DROPPED_OBJECT_NAME("DatabaseDroppedObjectName", KeyType.STRING, "DbDON"),
    DATABASE("Database", KeyType.STRING, "Db"),
    DATABASE_HISTORY("DatabaseHistory", KeyType.STRING, "DbH"),

    // table
    TABLE_OBJECT("TableObject", KeyType.STRING, "TblO"),
    TABLE("Table", KeyType.STRING, "Tb"),
    TABLE_REFERENCE("TableReference", KeyType.STRING, "TblR"),
    TABLE_DROPPED_OBJECT_NAME("TableDroppedObjectName", KeyType.STRING, "TblDON"),
    TABLE_ID("TableId", KeyType.STRING, "TableId"),
    TABLE_ID_VAR("TableIdVar", KeyType.STRING),
    TABLE_COMMIT("TableCommit", KeyType.STRING, "TblC"),
    TABLE_HISTORY("TableHistory", KeyType.STRING, "TblH"),
    TABLE_SCHEMA_HISTORY("TableSchemaHistory", KeyType.STRING, "TblScH"),
    TABLE_INVISIBLE_SCHEMA_HISTORY("InvisibleSchemaHistory", KeyType.STRING, "TblIScH"),
    TABLE_BASE_HISTORY("TableBaseHistory", KeyType.STRING, "TblBaseH"),
    TABLE_STORAGE_HISTORY("TableStorageHistory", KeyType.STRING, "TblStoH"),
    TABLE_INDEXES("TableIndexes", KeyType.STRING, "TblIdx"),
    TABLE_INDEXES_HISTORY("TableIndexesHistory", KeyType.STRING, "TblIdxH"),

    // view
    VIEW_OBJECT("ViewObject", KeyType.STRING, "ViewO"),
    VIEW_NAME("ViewObjectName", KeyType.STRING, "ViewON"),
    DROPPED_VIEW_NAME("DroppedViewObjectName", KeyType.STRING, "ViewDON"),
    VIEW_ID("ViewId", KeyType.UUID),
    VIEW("View", KeyType.STRING, "View"),

    // function
    FUNCTION_RECORD("FunctionRecord", KeyType.STRING, "FuncR"),

    // usage profile
    USAGE_PROFILE_RECORD("UsageProfileRecord", KeyType.STRING, "UsagePR"),

    //dataLineage
    DATA_LINEAGE_RECORD("DataLineageRecord", KeyType.STRING, "DataLR"),

    // index
    INDEX_OBJECT("IndexObject", KeyType.STRING, "IndexO"),
    INDEX_OBJECT_NAME("IndexObjectName", KeyType.STRING, "IndexON"),
    DROPPED_INDEX_OBJECT_NAME("DroppedIndexObjectName", KeyType.STRING, "IndexDON"),
    INDEX_SCHEMA("IndexSchema", KeyType.STRING, "IndexSc"),
    INDEX_RECORD("IndexRecord", KeyType.STRING, "IndexRec"),
    INDEX_PARTITION("IndexPartition", KeyType.STRING, "IndexPar"),
    INDEX_ID("IndexId", KeyType.UUID),

    // backend task
    BACKEND_TASK_RECORD("BackendTaskRecord", KeyType.STRING, "BackendTaskO");

    @Getter
    private final String name;
    private final KeySpaceDirectory directory;

    DirectoryStringMap(String name, KeyType type, Object value) {
        this.name = name;
        directory = new KeySpaceDirectory(name, type, value);
    }

    DirectoryStringMap(String name, KeyType type) {
        directory = new KeySpaceDirectory(name, type);
        this.name = name;
    }

    public KeySpaceDirectory addDirectory() {
        return directory;
    }
}
