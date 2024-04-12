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
package io.polycat.catalog.client.util;

import io.polycat.catalog.common.plugin.request.base.CatalogRequestBase;
import io.polycat.catalog.common.plugin.request.base.DatabaseRequestBase;
import io.polycat.catalog.common.plugin.request.base.ProjectRequestBase;
import io.polycat.catalog.common.plugin.request.base.TableRequestBase;

/**
 * 功能描述
 *
 * @since 2021-06-07
 */
public class Constants {

    //Need to modify
    public static final String PROJECTID = "test";

    public static final String CATALOGS = "/catalogs";

    public static final String LAKEPROFILE = "/lakeprofile";

    public static final String BRANCHS = "/sub-branches";

    public static final String MERGE = "/merge";

    public static final String DATABASES = "/databases";

    public static final String GET_DATABASE_BY_NAME = "/getDatabaseByName";

    public static final String GET_TABLE_BY_NAME = "/getTableByName";

    public static final String SLASH = "/";

    public static final String QUESTION = "?";

    public static final String EQUAL = "=";

    public static final String AND = "&";

    public static final String DATABASE_NAME = "databaseName";

    public static final String DATABASE_NAME_COMBINATION = QUESTION + DATABASE_NAME + EQUAL;

    public static final String CASCADE = "cascade";

    public static final String CASCADE_COMBINATION = AND + CASCADE + EQUAL;

    public static final String ALL = "all";

    public static final String LIMIT = "limit";

    public static final String PAGETOKEN = "pageToken";

    public static final String FILTER = "filter";

    public static final String RENAME = "rename";

    public static final String TABLE_ID = "tableId";

    public static final String DATABASE_ID = "databaseId";

    public static final String PURGE = "purge";

    public static final String DELETE_DATA = "deleteData";

    public static final String IF_EXIST = "ifExist";

    public static final String UNDROP = "/undrop";

    public static final String LIST = "/list";

    public static final String LIST_TABLE_NAMES = "/listTableNames";

    public static final String LIST_TABLE_OBJECTS = "/listTableObjects";

    public static final String NAMES = "/names";

    public static final String TABLES = "/tables";

    public static final String PARTITIONS = "/partitions";

    public static final String LIST_PARTITIONS = "/listPartitions";

    public static final String LIST_PARTITION_NAMES = "/listPartitionNames";

    public static final String LIST_PARTITION_NAMES_PS = "/listPartitionNamesPs";

    public static final String LIST_PARTITION_NAMES_BY_FILTER = "/listPartitionNamesByFilter";

    public static final String LIST_PARTITIONS_PS_WITH_AUTH = "/listPartitionsPsWithAuth";

    public static final String LIST_PARTITIONS_BY_EXPR = "/listByExpr";

    public static final String SHOW_PARTITIONS = "/showPartitions";

    public static final String ADD_PARTITION = "/addPartition";

    public static final String ADD_PARTITIONS = "/addPartitions";

    public static final String ALTER_PARTITIONS = "/alterPartitions";

    public static final String DROP_PARTITION = "/dropPartition";

    public static final String GET_PARTITIONS_BY_FILTER = "/getPartitionsByFilter";

    public static final String GET_PARTITIONS_BY_NAMES = "/getPartitionsByNames";

    public static final String GET_PARTITION_WITH_AUTH = "/getPartitionWithAuth";

    public static final String GET_PARTITION_COUNT = "/getPartitionCount";
    public static final String GET_LATEST_PARTITION_NAME = "/getLatestPartitionName";

    public static final String RESTORE = "/restore";

    public static final String STAT = "/stats";

    public static final String REQ_ID = "id";

    public static final String REQ_NAME = "name";

    public static final String REQ_CATEGORY_ID = "categoryId";
    public static final String REQ_DISCOVERY_ID = "discoveryId";

    public static final String REQ_PARAM_QUALIFIED_NAME = "qualifiedName";

    public static final String REQ_PARAM_IGNORE_UNKNOWN_OBJ = "ignoreUnknownObj";

    public static final String REQ_PARAM_OBJECT_TYPE = "objectType";

    public static final String REQ_PARAM_EXACT_MATCH = "exactMatch";
    public static final String REQ_PARAM_WITH_CATEGORIES = "withCategories";

    public static final String REQ_PARAM_LOGICAL_OPERATOR = "logicalOperator";

    public static final String REQ_PARAM_OBJECT_NAME = "objectName";

    public static final String REQ_PARAM_CATALOG_NAME = "catalogName";

    public static final String REQ_PARAM_DATABASE_NAME = "databaseName";

    public static final String REQ_PARAM_TABLE_NAME = "tableName";

    public static final String REQ_PARAM_SHARE_NAME = "shareName";

    public static final String REQ_PARAM_ACCOUNT_ID = "accountId";

    public static final String REQ_PARAM_USER_ID = "userId";

    public static final String REQ_PARAM_FACT_ID = "factId";
    public static final String REQ_PARAM_DB_TYPE = "dbType";
    public static final String REQ_PARAM_DEPTH = "depth";
    public static final String REQ_PARAM_DIRECTION = "direction";

    public static final String REQ_PARAM_TASK_ID = "taskId";

    public static final String REQ_PARAM_TAG = "tag";

    public static final String REQ_PARAM_OPERATIONS = "operations";

    public static final String REQ_PARAM_ROLE_NAME = "roleName";

    public static final String REQ_PARAM_INCLUDE_DROP = "includeDrop";

    public static final String REQ_PARAM_MAX_RESULTS = "maxResults";

    public static final String REQ_PARAM_PAGE_TOKEN = "pageToken";

    public static final String REQ_PARAM_PATTERN = "pattern";

    public static final String REQ_PARAM_FILTER = "filter";

    public static final String REQ_PARAM_KEYWORD = "keyword";

    public static final String REQ_PARAM_OWNER = "owner";

    public static final String REQ_PARAM_CONTAIN_OWNER = "containOwner";

    public static final String REQ_PARAM_FILTER_TYPE = "filterType";

    public static final String REQ_PARAM_VERSION = "version";

    public static final String REQ_PARAM_DELEGATE = "delegate";

    public static final String REQ_PARAM_START_TIME = "startTime";

    public static final String REQ_PARAM_END_TIME = "endTime";

    public static final String REQ_PARAM_OP_TYPES = "opTypes";

    public static final String REQ_PARAM_TOP_TYPE = "topType";

    public static final String REQ_PARAM_ESCAPE = "escape";

    public static final String REQ_PARAM_TOP_NUM = "topNum";

    public static final String REQ_PARAM_ROW_COUNT = "rowCount";

    public static final String REQ_PARAM_HMS_TAB = "hmsTab";

    public static final String REQ_PARAM_PARENT_DATABASE_NAME = "parentDatabaseName";

    public static final String REQ_PARAM_LINEAGE_TYPE = "lineageType";

    public static final String REQ_PARAM_FUNCTION_NAME = "functionName";

    public static final String REQ_LOCK_ID = "lockId";

    public static final String LATEST_VERSION = "/latestVersion";

    public static final String LIST_FILES = "/listFiles";

    public static final String LIST_CATALOGS = "/listCatalogs";

    public static final String LIST_TABLE_COMMITS = "/listTableCommits";

    public static final String COMMIT_LOGS = "/commit-logs";

    public static final String SHOW_SHARES = "/showShares";

    public static final String SHOW_ROLES = "/showRoles";

    public static final String SHARES = "/shares";

    public static final String ADD_ACCOUNTS = "/addAccounts";

    public static final String REMOVE_ACCOUNTS = "/removeAccounts";

    public static final String ADD_USERS = "/addUsers";

    public static final String REMOVE_USERS = "/removeUsers";

    public static final String GRANT_PRIVILEGE = "/grantPrivilege";

    public static final String REVOKE_PRIVILEGE = "/revokePrivilege";

    public static final String SHOW_PRIVILEGES = "/showPrivileges";

    public static final String SHOW_ROLE_PRIVILEGES = "/showRolePrivileges";

    public static final String SHOW_PRIVILEGE_ROLES = "/showPrivilegeRoles";

    public static final String SHOW_ALL_ROLE_NAME = "/showAllRoleName";

    public static final String SHOW_PERM_OBJECTS_BY_USER = "/showPermObjectsByUser";

    public static final String LIST_PRIVILEGE = "/listPrivilegeOfId";

    public static final String LIST_POLICY_HISTORY = "/listPolicyHistoryByTime";

    public static final String GRANT_ALL_OBJECT_PRIVILEGE = "/grantAllPrivilege";

    public static final String REVOKE_ALL_OBJECT_PRIVILEGE = "/revokeAllPrivilege";

    public static final String ROLES = "/roles";

    public static final String POLICY = "/policy";

    public static final String DISCOVERY = "/discovery";

    public static final String GLOSSARY = "/glossary";

    public static final String LOCK = "/lock";

    public static final String CREATE_LOCK = "/createLock";

    public static final String HEART_BEAT = "/heartbeat";

    public static final String CHECK_LOCK = "/checkLock";

    public static final String SHOW_LOCKS = "/showLocks";

    public static final String UNLOCK = "/unlock";

    public static final String CREATE_GLOSSARY = "/createGlossary";
    public static final String ALTER_GLOSSARY = "/alterGlossary";
    public static final String DELETE_GLOSSARY = "/deleteGlossary";
    public static final String GET_GLOSSARY = "/getGlossary";
    public static final String LIST_GLOSSARY_WITHOUT_CATEGORY = "/listGlossaryWithoutCategory";
    public static final String CREATE_CATEGORY = "/createCategory";
    public static final String ALTER_CATEGORY = "/alterCategory";
    public static final String DELETE_CATEGORY = "/deleteCategory";
    public static final String GET_CATEGORY = "/getCategory";

    public static final String ADD_CATEGORY_RELATION = "/addCategoryRelation";
    public static final String REMOVE_CATEGORY_RELATION = "/removeCategoryRelation";
    public static final String GET_OBJECT_COUNT_BY_CATEGORY = "/getObjectCountByCategory";

    public static final String GET_TABLE_COUNT_BY_CATALOG = "/getTableCountByCatalog";
    public static final String GET_TABLE_CATEGORIES = "/getTableCategories";



    public static final String SEARCH = "/search";

    public static final String MATCH_LIST_NAMES = "/matchListNames";

    public static final String REQ_PARAM_PRINCIPAL_TYPE = "principalType";

    public static final String REQ_PARAM_PRINCIPAL_SOURCE = "principalSource";

    public static final String REQ_PARAM_PRINCIPAL_NAME = "principalName";

    public static final String REQ_PARAM_POLICY_UPDATE_TIME = "updatedTime";

    public static final String AUTHENTICATION = "/authentication";

    public static final String DELEGATES = "/delegates";

    public static final String ACCELERATORS = "/accelerators";

    public static final String USAGE_PROFILES = "/usageprofiles";

    public static final String DATA_LINEAGES = "/datalineages";

    public static final String RECORD_TABLE_USAGE_PROFILE = "/recordTableUsageProfile";

    public static final String GET_TOP_USAGE_PROFILES_BY_CATALOG = "/getTopUsageProfilesByCatalog";

    public static final String GET_USAGE_PROFILES_BY_TABLE = "/getUsageProfilesByTable";

    public static final String GET_USAGE_PROFILE_DETAILS = "/getUsageProfileDetails";

    public static final String GET_TABLE_ACCESS_USERS = "/getTableAccessUsers";

    public static final String GET_USAGE_PROFILE_GROUP_BY_USER = "/getUsageProfileGroupByUser";

    public static final String RECORD_DATA_LINEAGE = "/recordDataLineage";

    public static final String UPDATE_DATA_LINEAGE = "/updateDataLineage";

    public static final String SEARCH_DATA_LINEAGE = "/lineageGraph";

    public static final String GET_DATA_LINEAGE_FACT = "/getLineageFact";

    public static final String GET_DATA_LINEAGES_BY_TABLE = "/getDataLineagesByTable";

    public static final String COMPILE = "/compile";

    public static final String ALTER_COLUMN = "/alterColumn";

    public static final String SET_PROPERTIES = "/setProperties";

    public static final String UNSET_PROPERTIES = "/unsetProperties";

    public static final String OBJECT_NAME_MAP = "/objectNameMap";

    public static final String GET_OBJECT = "/getObject";

    public static final String FUNCTIONS = "/functions";

    public static final String COLUMN_STATISTICS = "/columnStatistics";

    public static final String MRS_TOKEN = "/mrsTokens";

    public static final String CONSTRAINTS = "/constraints";
    public static final String PRIMARY_KEYS = "/primaryKeys";
    public static final String FOREIGN_KEYS = "/foreignKeys";


    public static final String MATERIALIZED_VIEW = "/materializedView";

    public static final String INDEX = "/index";

    public static final String GET_MV_BY_NAME = "/getMVByName";

    public static final String REQ_PARAM_MV_NAME = "mvName";


    public static final String POLYCAT_CLIENT_HOST = "polycat.client.host";
    public static final String POLYCAT_CLIENT_PORT = "polycat.client.port";
    public static final String POLYCAT_CLIENT_USERNAME = "polycat.client.userName";
    public static final String POLYCAT_CLIENT_PASSWORD = "polycat.client.password";
    public static final String POLYCAT_CLIENT_PROJECT_ID = "polycat.client.projectId";

    public static <T extends ProjectRequestBase> String getPolicyUrlPrefix(String catalogServerUrlPrefix, T request) {
        return catalogServerUrlPrefix + getProjectId(request) + Constants.POLICY;
    }

    public static <T extends ProjectRequestBase> String getDiscoveryUrlPrefix(String catalogServerUrlPrefix, T request) {
        return catalogServerUrlPrefix + getProjectId(request) + Constants.DISCOVERY;
    }

    public static <T extends ProjectRequestBase> String getGlossaryUrlPrefix(String catalogServerUrlPrefix, T request) {
        return catalogServerUrlPrefix + getProjectId(request) + Constants.GLOSSARY;
    }

    public static <T extends ProjectRequestBase> String getRoleUrlPrefix(String catalogServerUrlPrefix, T request) {
        return catalogServerUrlPrefix + getProjectId(request) + Constants.ROLES;
    }

    public static <T extends ProjectRequestBase> String getShareUrlPrefix(String catalogServerUrlPrefix, T request) {
        return catalogServerUrlPrefix + getProjectId(request) + Constants.SHARES;
    }

    public static <T extends ProjectRequestBase> String getCatalogUrlPrefix(String catalogServerUrlPrefix, T request) {
        return catalogServerUrlPrefix + getProjectId(request) + CATALOGS;
    }

    public static <T extends  ProjectRequestBase> String getLakeProfileUrlPrefix(String catalogServerUrlPrefix, T request ) {
        return catalogServerUrlPrefix + getProjectId(request) + LAKEPROFILE;
    }

    public static <T extends CatalogRequestBase> String getDatabaseUrlPrefix(String catalogServerUrlPrefix, T request) {
        return catalogServerUrlPrefix + getProjectId(request)
            + CATALOGS + SLASH + request.getCatalogName()
            + DATABASES;
    }

    public static <T extends DatabaseRequestBase> String getTableUrlPrefix(String catalogServerUrlPrefix, T request) {
        return catalogServerUrlPrefix + getProjectId(request)
            + CATALOGS + SLASH + request.getCatalogName()
            + DATABASES + SLASH + request.getDatabaseName()
            + TABLES;
    }

    public static <T extends DatabaseRequestBase> String getAcceleratorUrlPrefix(String catalogServerUrlPrefix,
        T request) {
        return catalogServerUrlPrefix + getProjectId(request)
            + CATALOGS + SLASH + request.getCatalogName()
            + DATABASES + SLASH + request.getDatabaseName()
            + ACCELERATORS;
    }

    public static <T extends TableRequestBase> String getPartitionUrlPrefix(String catalogServerUrlPrefix,
        T request) {
        return catalogServerUrlPrefix + getProjectId(request)
            + CATALOGS + SLASH + request.getCatalogName()
            + DATABASES + SLASH + request.getDatabaseName()
            + TABLES + SLASH + request.getTableName()
            + PARTITIONS;
    }

    public static <T extends ProjectRequestBase> String getDataLineageUrlPrefix(String catalogServerUrlPrefix,
        T request) {
        return catalogServerUrlPrefix + getProjectId(request)
            + DATA_LINEAGES;
    }

    public static <T extends ProjectRequestBase> String getProjectId(T request) {
        if (request.getProjectId() == null) {
            throw new NullPointerException("Request param: projectId is null.");
        }
        return request.getProjectId();
    }

    public static <T extends CatalogRequestBase> String getUsageProfileUrlPrefix(String catalogServerUrlPrefix,
        T request) {
        return catalogServerUrlPrefix + getProjectId(request)
            + USAGE_PROFILES;
    }

    public static <T extends DatabaseRequestBase> String getFunctionUrlPrefix(String catalogServerUrlPrefix,
        T request) {
        return catalogServerUrlPrefix + getProjectId(request)
            + CATALOGS + SLASH + request.getCatalogName()
            + DATABASES + SLASH + request.getDatabaseName()
            + FUNCTIONS;
    }

    public static <T extends DatabaseRequestBase> String getMVUrlPrefix(
        String catalogServerUrlPrefix, T request) {
        return catalogServerUrlPrefix + getProjectId(request)
            + CATALOGS + SLASH + request.getCatalogName()
            + DATABASES + SLASH + request.getDatabaseName()
            + INDEX + MATERIALIZED_VIEW;
    }

    public static <T extends CatalogRequestBase> String getFunctionUrlPrefix(String catalogServerUrlPrefix,
        T request) {
        return catalogServerUrlPrefix + getProjectId(request)
            + CATALOGS + SLASH + request.getCatalogName()
            + FUNCTIONS;
    }

    public static <T extends ProjectRequestBase> String getTokenUrlPrefix(String catalogServerUrlPrefix,
        T request) {
        return catalogServerUrlPrefix + getProjectId(request)
            + MRS_TOKEN;
    }

    public static <T extends TableRequestBase> String getConstraintUrlPrefix(String catalogServerUrlPrefix, T request) {
        return catalogServerUrlPrefix + getProjectId(request)
            + CATALOGS + SLASH + request.getCatalogName()
            + DATABASES + SLASH + request.getDatabaseName()
            + TABLES + SLASH + request.getTableName()
            + CONSTRAINTS;
    }

    public static <T extends ProjectRequestBase> String getLockUrlPrefix(String catalogServerUrlPrefix, T request) {
        return catalogServerUrlPrefix + getProjectId(request) + Constants.LOCK;
    }
}
