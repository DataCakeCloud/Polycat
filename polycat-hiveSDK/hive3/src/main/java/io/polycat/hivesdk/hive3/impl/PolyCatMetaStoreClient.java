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
package io.polycat.hivesdk.hive3.impl;

import static io.polycat.hivesdk.hive3.config.ConfigKeyContents.AUTH_SOURCE_TYPE;
import static io.polycat.hivesdk.hive3.config.ConfigKeyContents.CATALOG_HOST;
import static io.polycat.hivesdk.hive3.config.ConfigKeyContents.CATALOG_PORT;
import static io.polycat.hivesdk.hive3.config.ConfigKeyContents.DEFAULT_CATALOG_NAME;
import static io.polycat.hivesdk.hive3.config.ConfigKeyContents.POLYCAT_USER_NAME;
import static io.polycat.hivesdk.hive3.config.ConfigKeyContents.POLYCAT_USER_PASSWORD;

import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.model.Constraint;
import io.polycat.catalog.common.model.ConstraintType;
import io.polycat.catalog.common.model.ForeignKey;
import io.polycat.catalog.common.model.KerberosToken;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.PartitionAlterContext;
import io.polycat.catalog.common.model.PrimaryKey;
import io.polycat.catalog.common.model.TableBrief;
import io.polycat.catalog.common.model.stats.PartitionStatisticData;
import io.polycat.catalog.common.plugin.CatalogContext;
import io.polycat.catalog.common.plugin.CatalogPlugin;
import io.polycat.catalog.common.plugin.request.AddConstraintsRequest;
import io.polycat.catalog.common.plugin.request.AddForeignKeysRequest;
import io.polycat.catalog.common.plugin.request.AddPartitionRequest;
import io.polycat.catalog.common.plugin.request.AddPrimaryKeysRequest;
import io.polycat.catalog.common.plugin.request.AddTokenRequest;
import io.polycat.catalog.common.plugin.request.AlterCatalogRequest;
import io.polycat.catalog.common.plugin.request.AlterDatabaseRequest;
import io.polycat.catalog.common.plugin.request.AlterFunctionRequest;
import io.polycat.catalog.common.plugin.request.AlterKerberosTokenRequest;
import io.polycat.catalog.common.plugin.request.AlterPartitionRequest;
import io.polycat.catalog.common.plugin.request.AlterTableRequest;
import io.polycat.catalog.common.plugin.request.AppendPartitionRequest;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.CreateFunctionRequest;
import io.polycat.catalog.common.plugin.request.CreateTableRequest;
import io.polycat.catalog.common.plugin.request.DeleteColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.DeleteConstraintRequest;
import io.polycat.catalog.common.plugin.request.DeleteDatabaseRequest;
import io.polycat.catalog.common.plugin.request.DeletePartitionColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.DeleteTableRequest;
import io.polycat.catalog.common.plugin.request.DeleteTokenRequest;
import io.polycat.catalog.common.plugin.request.DropCatalogRequest;
import io.polycat.catalog.common.plugin.request.DropPartitionRequest;
import io.polycat.catalog.common.plugin.request.DropPartitionsRequest;
import io.polycat.catalog.common.plugin.request.FunctionRequestBase;
import io.polycat.catalog.common.plugin.request.GetAggregateColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.GetAllFunctionRequest;
import io.polycat.catalog.common.plugin.request.GetCatalogRequest;
import io.polycat.catalog.common.plugin.request.GetConstraintsRequest;
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest;
import io.polycat.catalog.common.plugin.request.GetForeignKeysReq;
import io.polycat.catalog.common.plugin.request.GetFunctionRequest;
import io.polycat.catalog.common.plugin.request.GetPartitionColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.GetPartitionRequest;
import io.polycat.catalog.common.plugin.request.GetPartitionWithAuthRequest;
import io.polycat.catalog.common.plugin.request.GetPrimaryKeysRequest;
import io.polycat.catalog.common.plugin.request.GetTableColumnStatisticRequest;
import io.polycat.catalog.common.plugin.request.GetTableMetaRequest;
import io.polycat.catalog.common.plugin.request.GetTableNamesRequest;
import io.polycat.catalog.common.plugin.request.GetTableObjectsByNameRequest;
import io.polycat.catalog.common.plugin.request.GetTablePartitionsByNamesRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.GetTokenRequest;
import io.polycat.catalog.common.plugin.request.GetTokenWithRenewerRequest;
import io.polycat.catalog.common.plugin.request.ListCatalogsRequest;
import io.polycat.catalog.common.plugin.request.ListDatabasesRequest;
import io.polycat.catalog.common.plugin.request.ListFunctionRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionByFilterRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionNamesPsRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionsByExprRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionsWithAuthRequest;
import io.polycat.catalog.common.plugin.request.ListTablePartitionsRequest;
import io.polycat.catalog.common.plugin.request.ListTokenRequest;
import io.polycat.catalog.common.plugin.request.RenamePartitionRequest;
import io.polycat.catalog.common.plugin.request.SetPartitionColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.TruncatePartitionRequest;
import io.polycat.catalog.common.plugin.request.TruncateTableRequest;
import io.polycat.catalog.common.plugin.request.UpdatePartitionColumnStatisticRequest;
import io.polycat.catalog.common.plugin.request.UpdateTableColumnStatisticRequest;
import io.polycat.catalog.common.plugin.request.input.AddConstraintsInput;
import io.polycat.catalog.common.plugin.request.input.AddForeignKeysInput;
import io.polycat.catalog.common.plugin.request.input.AddPartitionInput;
import io.polycat.catalog.common.plugin.request.input.AddPrimaryKeysInput;
import io.polycat.catalog.common.plugin.request.input.AlterPartitionInput;
import io.polycat.catalog.common.plugin.request.input.AlterTableInput;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.ColumnStatisticsInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionsByExprsInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionColumnStaticsInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionWithAuthInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionsByExprInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionsWithAuthInput;
import io.polycat.catalog.common.plugin.request.input.PartitionDescriptorInput;
import io.polycat.catalog.common.plugin.request.input.PartitionFilterInput;
import io.polycat.catalog.common.plugin.request.input.SetPartitionColumnStatisticsInput;
import io.polycat.catalog.common.plugin.request.input.TokenInput;
import io.polycat.catalog.common.plugin.request.input.TruncatePartitionInput;
import io.polycat.hivesdk.hive3.config.HiveCatalogContext;
import io.polycat.hivesdk.hive3.tools.HiveClientHelper;
import io.polycat.hivesdk.hive3.tools.HiveDataAccessor;
import io.polycat.hivesdk.hive3.tools.PolyCatDataAccessor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionState;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.thrift.TException;

public class PolyCatMetaStoreClient implements IMetaStoreClient {

    CatalogPlugin polyCatClient;

    @Setter
    HiveCatalogContext context;

    Configuration conf;

    public PolyCatMetaStoreClient(HiveCatalogContext context) {
        this.context = context;
        polyCatClient = new PolyCatClient();
        polyCatClient.setContext(this.context.getCatalogContext());
    }

    public PolyCatMetaStoreClient() {
        this(new HiveConf(), null, true);
    }

    public PolyCatMetaStoreClient(Configuration hiveConf) {
        this(hiveConf, null, true);
    }

    public PolyCatMetaStoreClient(Configuration hiveConf, HiveMetaHookLoader hookLoader) {
        this(hiveConf, hookLoader, true);
    }

    public PolyCatMetaStoreClient(Configuration hiveConf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded) {
        conf = hiveConf;
        String userName = conf.get(POLYCAT_USER_NAME, "test");
        String passWord = conf.get(POLYCAT_USER_PASSWORD, "dash");
        // getHost
        String catalogHost = conf.get(CATALOG_HOST, "127.0.0.1");
        int catalogPort = conf.getInt(CATALOG_PORT, 8082);
        // init
        polyCatClient = new PolyCatClient(catalogHost, catalogPort);
        // auth info
        String authSourceType = conf.get(AUTH_SOURCE_TYPE, "ldap");
        CatalogContext context = CatalogUserInformation.doAuth(userName, passWord);
        context.setDefaultCatalogName(conf.get(DEFAULT_CATALOG_NAME, "hive"));
        context.setAuthSourceType(authSourceType);
        this.context = new HiveCatalogContext(context);

        polyCatClient.setContext(context);
    }

    @Override
    public boolean isCompatibleWith(Configuration configuration) {
        return false;
    }

    // to be compatible with hive 2.3.7
    public boolean isCompatibleWith(HiveConf conf) {
        return true;
    }

    @Override
    public void setHiveAddedJars(String s) {

    }

    @Override
    public boolean isLocalMetaStore() {
        return false;
    }

    @Override
    public void reconnect() throws MetaException {

    }

    @Override
    public void close() {

    }

    @Override
    public void setMetaConf(String s, String s1) throws MetaException, TException {

    }

    @Override
    public String getMetaConf(String s) throws MetaException, TException {
        return null;
    }

    @Override
    public void createCatalog(Catalog catalog)
        throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
        CreateCatalogRequest request = new CreateCatalogRequest();
        CatalogInput input = PolyCatDataAccessor.toCatalogInput(catalog);
        request.setInput(input);
        request.setProjectId(context.getProjectId());
        HiveClientHelper.FuncExceptionHandler(() -> {
            polyCatClient.createCatalog(request);
        });
    }

    @Override
    public void alterCatalog(String catalogName, Catalog catalog)
        throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        AlterCatalogRequest request = new AlterCatalogRequest(context.getProjectId(), catalogName);
        CatalogInput input = new CatalogInput()
            .setCatalogName(catalog.getName())
            .setDescription(catalog.getDescription())
            .setLocation(catalog.getLocationUri());
        request.setInput(input);
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.alterCatalog(request));
    }

    @Override
    public Catalog getCatalog(String catalogName) throws NoSuchObjectException, MetaException, TException {
        GetCatalogRequest request = new GetCatalogRequest(context.getProjectId(), catalogName);
        io.polycat.catalog.common.model.Catalog result = HiveClientHelper.FuncExceptionHandler(
            () -> polyCatClient.getCatalog(request));
        return HiveDataAccessor.toCatalog(result);
    }

    @Override
    public List<String> getCatalogs() throws MetaException, TException {
        ListCatalogsRequest request = new ListCatalogsRequest(context.getProjectId());
        PagedList<io.polycat.catalog.common.model.Catalog> hiveResult = HiveClientHelper.FuncExceptionHandler(
            () -> polyCatClient.listCatalogs(request));
        return Arrays.stream(hiveResult.getObjects()).map(catalog -> catalog.getCatalogName())
            .collect(Collectors.toList());
    }

    @Override
    public void dropCatalog(String catalogName)
        throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        DropCatalogRequest request = new DropCatalogRequest(context.getProjectId(), catalogName);
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.dropCatalog(request));
    }

    @Override
    public List<String> getDatabases(String databasePattern) throws MetaException, TException {
        return getDatabases(context.getDefaultCatalogName(), databasePattern);
    }

    @Override
    public List<String> getDatabases(String catName, String databasePattern) throws MetaException, TException {
        ListDatabasesRequest request = new ListDatabasesRequest();
        request.setFilter(databasePattern);
        request.setMaxResults(Integer.MAX_VALUE);
        request.setPageToken(null);
        request.setIncludeDrop(false);
        request.setCatalogName(catName);
        request.setProjectId(context.getProjectId());
        return HiveClientHelper.FuncExceptionHandler(
            () -> Arrays.asList(polyCatClient.getDatabases(request).getObjects()));
    }

    @Override
    public List<String> getAllDatabases() throws MetaException, TException {
        return getDatabases("*");
    }

    @Override
    public List<String> getAllDatabases(String catalogName) throws MetaException, TException {
        return getDatabases(catalogName, "*");
    }

    @Override
    public List<String> getTables(String dbName, String tablePattern)
        throws MetaException, TException, UnknownDBException {
        return getTables(context.getDefaultCatalogName(), dbName, tablePattern);
    }

    @Override
    public List<String> getTables(String catName, String dbName, String tablePattern)
        throws MetaException, TException, UnknownDBException {
        return getTables(catName, dbName, tablePattern, null);
    }

    @Override
    public List<String> getTables(String dbName, String tablePattern, TableType tableType)
        throws MetaException, TException, UnknownDBException {
        return getTables(context.getDefaultCatalogName(), dbName, tablePattern, tableType);
    }

    @Override
    public List<String> getTables(String catName, String dbName, String tablePattern, TableType tableType)
        throws MetaException, TException, UnknownDBException {
        GetTableNamesRequest request = new GetTableNamesRequest();
        request.setCatalogName(catName);
        request.setDatabaseName(dbName);
        request.setPattern(tablePattern);
        if (tableType != null) {
            request.setTableType(tableType.name());
        }
        return Arrays.asList(polyCatClient.getTableNames(request).getObjects());
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String s)
        throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String s, String s1)
        throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
        throws MetaException, TException, UnknownDBException {
        return getTableMeta(context.getDefaultCatalogName(), dbPatterns, tablePatterns, tableTypes);
    }

    @Override
    public List<TableMeta> getTableMeta(String catName, String dbPatterns, String tablePatterns,
        List<String> tableTypes)
        throws MetaException, TException, UnknownDBException {
        return HiveClientHelper.FuncExceptionHandler(() -> {
            GetTableMetaRequest request = new GetTableMetaRequest(context.getProjectId(), catName, dbPatterns,
                tablePatterns, tableTypes);
            PagedList<TableBrief> result = polyCatClient.getTableMeta(request);
            return Arrays.stream(result.getObjects()).map(HiveDataAccessor::toTableMeta).collect(Collectors.toList());
        });
    }

    @Override
    public List<String> getAllTables(String dbName) throws MetaException, TException, UnknownDBException {
        return getAllTables(context.getDefaultCatalogName(), dbName);
    }

    @Override
    public List<String> getAllTables(String catName, String dbName)
        throws MetaException, TException, UnknownDBException {
        return HiveClientHelper.FuncExceptionHandler(() -> {
            GetTableNamesRequest request = new GetTableNamesRequest();
            request.setCatalogName(catName);
            request.setDatabaseName(dbName);
            request.setPattern("*");
            return Arrays.asList(polyCatClient.getTableNames(request).getObjects());
        });
    }

    @Override
    public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
        throws TException, InvalidOperationException, UnknownDBException {
        return listTableNamesByFilter(context.getDefaultCatalogName(), dbName, filter, maxTables);
    }

    @Override
    public List<String> listTableNamesByFilter(String catName, String dbName, String filter, int maxTables)
        throws TException, InvalidOperationException, UnknownDBException {
        GetTableNamesRequest request = new GetTableNamesRequest();
        request.setCatalogName(catName);
        request.setDatabaseName(dbName);
        request.setFilter(filter);
        request.setMaxResults(String.valueOf(maxTables));
        return Arrays.asList(polyCatClient.getTableNames(request).getObjects());
    }

    @Override
    public void dropTable(String dbname, String tableName, boolean deleteData,
        boolean ignoreUnknownTab)
        throws MetaException, TException, NoSuchObjectException {
        dropTable(context.getDefaultCatalogName(), dbname, tableName, deleteData, ignoreUnknownTab, false);
    }

    @Override
    public void dropTable(String dbname, String tableName, boolean deleteData,
        boolean ignoreUnknownTab, boolean ifPurge)
        throws MetaException, TException, NoSuchObjectException {
        dropTable(context.getDefaultCatalogName(), dbname, tableName, deleteData, ignoreUnknownTab, ifPurge);
    }

    @Override
    public void dropTable(String dbname, String tableName) throws MetaException, TException, NoSuchObjectException {
        dropTable(context.getDefaultCatalogName(), dbname, tableName, false, true, false);
    }

    @Override
    public void dropTable(String catName, String dbName, String tableName, boolean deleteData,
        boolean ignoreUnknownTable, boolean ifPurge) throws MetaException, NoSuchObjectException, TException {
        DeleteTableRequest request = new DeleteTableRequest(catName, dbName, tableName);
        request.setDeleteData(deleteData);
        request.setIgnoreUnknownObject(ignoreUnknownTable);
        request.setPurgeFlag(ifPurge);
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.deleteTable(request));
    }

    @Override
    public void truncateTable(String dbName, String tableName, List<String> partNames)
        throws MetaException, TException {
        truncateTable(context.getDefaultCatalogName(), dbName, tableName, partNames);
    }

    @Override
    public void truncateTable(String catName, String dbName, String tableName, List<String> partNames)
        throws MetaException, TException {
        if (partNames == null || partNames.isEmpty()) {
            TruncateTableRequest request = new TruncateTableRequest(context.getProjectId(), catName, dbName, tableName);
            HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.truncateTable(request));
        } else {
            TruncatePartitionInput input = new TruncatePartitionInput(partNames);
            TruncatePartitionRequest request = new TruncatePartitionRequest(context.getProjectId(), catName, dbName,
                tableName, input);
            HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.truncatePartition(request));
        }
    }

    @Override
    public CmRecycleResponse recycleDirToCmPath(CmRecycleRequest cmRecycleRequest) throws MetaException, TException {
        return null;
    }

    @Override
    public boolean tableExists(String databaseName, String tableName)
        throws MetaException, TException, UnknownDBException {
        return tableExists(context.getDefaultCatalogName(), databaseName, tableName);
    }

    @Override
    public boolean tableExists(String catName, String dbName, String tableName)
        throws MetaException, TException, UnknownDBException {
        try {
            getTable(catName, dbName, tableName);
        } catch (NoSuchObjectException e) {
            return false;
        }
        return true;
    }

    @Override
    public Database getDatabase(String databaseName) throws NoSuchObjectException, MetaException, TException {
        return getDatabase(context.getDefaultCatalogName(), databaseName);
    }

    @Override
    public Database getDatabase(String catalogName, String databaseName)
        throws NoSuchObjectException, MetaException, TException {
        GetDatabaseRequest request = new GetDatabaseRequest();
        request.setCatalogName(catalogName);
        request.setDatabaseName(databaseName);
        request.setProjectId(context.getProjectId());
        return HiveDataAccessor.toDatabase(HiveClientHelper.FuncExceptionHandler(
            () -> polyCatClient.getDatabase(request)));
    }

    @Override
    public Table getTable(String dbName, String tableName) throws MetaException, TException, NoSuchObjectException {
        return getTable(context.getDefaultCatalogName(), dbName, tableName);
    }

    @Override
    public Table getTable(String catName, String dbName, String tableName) throws MetaException, TException {
        GetTableRequest request = new GetTableRequest(context.getProjectId(), catName, dbName, tableName);
        return HiveClientHelper.FuncExceptionHandler(() -> HiveDataAccessor.toTable(polyCatClient.getTable(request)));
    }

    @Override
    public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
        throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return getTableObjectsByName(context.getDefaultCatalogName(), dbName, tableNames);
    }

    @Override
    public List<Table> getTableObjectsByName(String catName, String dbName, List<String> tableNames)
        throws MetaException, InvalidOperationException, UnknownDBException, TException {
        GetTableObjectsByNameRequest request = new GetTableObjectsByNameRequest(context.getProjectId(), catName, dbName,
            tableNames);
        return HiveClientHelper.FuncExceptionHandler(
            () -> Arrays.stream(polyCatClient.getTableObjectsByName(request).getObjects())
                .map(HiveDataAccessor::toTable).collect(Collectors.toList()));
    }

    @Override
    public Materialization getMaterializationInvalidationInfo(CreationMetadata creationMetadata, String s)
        throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return null;
    }

    @Override
    public void updateCreationMetadata(String s, String s1, CreationMetadata creationMetadata)
        throws MetaException, TException {

    }

    @Override
    public void updateCreationMetadata(String s, String s1, String s2, CreationMetadata creationMetadata)
        throws MetaException, TException {

    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition appendPartition(String dbName, String tableName,
        List<String> partVals)
        throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return appendPartition(context.getDefaultCatalogName(), dbName, tableName, partVals);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition appendPartition(String catName, String dbName,
        String tableName, List<String> partVals)
        throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        Table table = getTable(catName, dbName, tableName);
        String partName = PolyCatDataAccessor.getPartitionName(partVals, table);
        return appendPartition(catName, dbName, tableName, partName);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition appendPartition(String dbName, String tableName, String name)
        throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return appendPartition(context.getDefaultCatalogName(), dbName, tableName, name);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition appendPartition(String catName, String dbName,
        String tableName, String name)
        throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        PartitionDescriptorInput input = new PartitionDescriptorInput();
        input.setPartName(name);
        AppendPartitionRequest request = new AppendPartitionRequest(context.getProjectId(), catName, dbName, tableName,
            input);
        // todo: equals get table and add partition
        return HiveClientHelper.FuncExceptionHandler(
            () -> HiveDataAccessor.toPartition(polyCatClient.appendPartition(request)));
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition add_partition(
        org.apache.hadoop.hive.metastore.api.Partition partition)
        throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        AddPartitionRequest request = new AddPartitionRequest(context.getProjectId(),
            partition.isSetCatName() ? partition.getCatName() : context.getDefaultCatalogName(),
            partition.getDbName(),
            partition.getTableName(),
            PolyCatDataAccessor.toPartitionInput(partition));

        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.addPartition(request));
        return partition;
    }

    @Override
    public int add_partitions(List<org.apache.hadoop.hive.metastore.api.Partition> partitions)
        throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return add_partitions(partitions, false, true).size();
    }

    @Override
    public int add_partitions_pspec(PartitionSpecProxy partitionSpecProxy)
        throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return 0;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> add_partitions(
        List<org.apache.hadoop.hive.metastore.api.Partition> partitions, boolean ifNotExists, boolean needResults)
        throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        if (partitions.isEmpty()) {
            return Collections.emptyList();
        }

        org.apache.hadoop.hive.metastore.api.Partition hivePart = partitions.get(0);
        String catName = hivePart.isSetCatName() ? hivePart.getCatName() : context.getDefaultCatalogName();
        boolean ifEveryPartitionNotInSameTable = partitions.stream().
            anyMatch(part -> !Objects.equals(part.getCatName(), hivePart.getCatName())
                || !Objects.equals(part.getDbName(), hivePart.getDbName())
                || !Objects.equals(part.getTableName(), hivePart.getTableName()));
        if (ifEveryPartitionNotInSameTable) {
            throw new InvalidInputException("the Partition to add need in same Table");
        }

        AddPartitionInput input = PolyCatDataAccessor.toPartitionInput(partitions);
        input.setIfNotExist(ifNotExists);
        input.setNeedResult(needResults);
        AddPartitionRequest request = new AddPartitionRequest(context.getProjectId(),
            hivePart.isSetCatName() ? hivePart.getCatName() : context.getDefaultCatalogName(),
            hivePart.getDbName(),
            hivePart.getTableName(),
            input);

        return HiveClientHelper.FuncExceptionHandler(
            () -> HiveDataAccessor.toPartitionList(polyCatClient.addPartitions(request)));
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tblName,
        List<String> partVals)
        throws NoSuchObjectException, MetaException, TException {
        return getPartition(context.getDefaultCatalogName(), dbName, tblName, partVals);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String catName, String dbName, String tblName,
        List<String> partVals)
        throws NoSuchObjectException, MetaException, TException {
        Table table = getTable(catName, dbName, tblName);
        String partName = PolyCatDataAccessor.getPartitionName(partVals, table);
        return getPartition(catName, dbName, tblName, partName);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition exchange_partition(Map<String, String> map, String s,
        String s1, String s2, String s3)
        throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition exchange_partition(Map<String, String> map, String s,
        String s1, String s2, String s3, String s4,
        String s5) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> exchange_partitions(Map<String, String> map, String s,
        String s1, String s2, String s3)
        throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> exchange_partitions(Map<String, String> map, String s,
        String s1, String s2, String s3,
        String s4, String s5) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tblName, String name)
        throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return getPartition(context.getDefaultCatalogName(), dbName, tblName, name);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String catName, String dbName, String tblName,
        String name)
        throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        GetPartitionRequest request = new GetPartitionRequest(context.getProjectId(), catName, dbName, tblName, name);
        return HiveClientHelper.FuncExceptionHandler(
            () -> HiveDataAccessor.toPartition(polyCatClient.getPartition(request)));
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartitionWithAuthInfo(String dbName, String tableName,
        List<String> pvals, String userName, List<String> groupNames)
        throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return getPartitionWithAuthInfo(context.getDefaultCatalogName(), dbName, tableName, pvals, userName,
            groupNames);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartitionWithAuthInfo(String catName, String dbName,
        String tableName,
        List<String> pvals, String userName, List<String> groupNames)
        throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        HiveClientHelper.checkInputIsNoNull(catName, dbName, tableName, pvals);
        GetPartitionWithAuthInput input = new GetPartitionWithAuthInput(userName, groupNames, pvals);
        GetPartitionWithAuthRequest request =
            new GetPartitionWithAuthRequest(context.getProjectId(), catName, dbName, tableName, input);
        return HiveClientHelper.FuncExceptionHandler(() ->
            HiveDataAccessor.toPartition(polyCatClient.getPartitionWithAuth(request)));
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(String db_name, String tbl_name,
        short max_parts)
        throws NoSuchObjectException, MetaException, TException {
        return listPartitions(context.getDefaultCatalogName(), db_name, tbl_name, null, max_parts);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(String catName, String db_name,
        String tbl_name, int max_parts)
        throws NoSuchObjectException, MetaException, TException {
        return listPartitions(catName, db_name, tbl_name, null, max_parts);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String s, String s1, int i) throws TException {
        return null;
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String s, String s1, String s2, int i) throws TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(String db_name, String tbl_name,
        List<String> part_vals, short max_parts)
        throws NoSuchObjectException, MetaException, TException {
        return listPartitions(context.getDefaultCatalogName(), db_name, tbl_name, part_vals, max_parts);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(String catName, String db_name,
        String tbl_name, List<String> part_vals, int max_parts)
        throws NoSuchObjectException, MetaException, TException {

        PartitionFilterInput input = new PartitionFilterInput();
        input.setValues(part_vals == null ? null : part_vals.toArray(new String[0]));
        input.setMaxParts(max_parts);
        ListTablePartitionsRequest request = new ListTablePartitionsRequest(context.getProjectId(), catName, db_name,
            tbl_name, input);
        return HiveClientHelper.FuncExceptionHandler(
            () -> HiveDataAccessor.toPartitionList(polyCatClient.listPartitionPs(request)));
    }

    @Override
    public List<String> listPartitionNames(String db_name, String tbl_name,
        short max_parts)
        throws NoSuchObjectException, MetaException, TException {
        return listPartitionNames(context.getDefaultCatalogName(), db_name, tbl_name, Collections.emptyList(),
            max_parts);
    }

    @Override
    public List<String> listPartitionNames(String catName, String db_name, String tbl_name,
        int max_parts)
        throws NoSuchObjectException, MetaException, TException {
        return listPartitionNames(catName, db_name, tbl_name, Collections.emptyList(), max_parts);
    }

    @Override
    public List<String> listPartitionNames(String db_name, String tbl_name,
        List<String> part_vals, short max_parts)
        throws MetaException, TException, NoSuchObjectException {
        return listPartitionNames(context.getDefaultCatalogName(), db_name, tbl_name, part_vals, max_parts);
    }

    @Override
    public List<String> listPartitionNames(String catName, String db_name, String tbl_name,
        List<String> part_vals, int max_parts)
        throws MetaException, TException, NoSuchObjectException {
        HiveClientHelper.checkInputIsNoNull(catName, db_name, tbl_name, part_vals);
        PartitionFilterInput input = new PartitionFilterInput();
        input.setValues(Objects.isNull(part_vals) ? null : part_vals.toArray(new String[0]));
        input.setMaxParts(max_parts);
        ListPartitionNamesPsRequest request = new ListPartitionNamesPsRequest(context.getProjectId(), catName, db_name,
            tbl_name, input);
        return HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.listPartitionNamesPs(request));
    }

    @Override
    public PartitionValuesResponse listPartitionValues(PartitionValuesRequest partitionValuesRequest)
        throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public int getNumPartitionsByFilter(String dbName, String tableName, String filter)
        throws MetaException, NoSuchObjectException, TException {
        return getNumPartitionsByFilter(context.getDefaultCatalogName(), dbName, tableName, filter);
    }

    @Override
    public int getNumPartitionsByFilter(String catName, String dbName, String tableName,
        String filter)
        throws MetaException, NoSuchObjectException, TException {
        return listPartitionsByFilter(catName, dbName, tableName, filter, -1).size();
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsByFilter(String db_name, String tbl_name,
        String filter, short max_parts)
        throws MetaException, NoSuchObjectException, TException {
        return listPartitionsByFilter(context.getDefaultCatalogName(), db_name, tbl_name, filter, max_parts);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsByFilter(String catName, String db_name,
        String tbl_name,
        String filter, int max_parts)
        throws MetaException, NoSuchObjectException, TException {
        HiveClientHelper.checkInputIsNoNull(catName, db_name, tbl_name, filter);
        PartitionFilterInput input = new PartitionFilterInput();
        input.setFilter(filter);
        input.setMaxParts(max_parts);
        ListPartitionByFilterRequest request = new ListPartitionByFilterRequest(context.getProjectId(), catName,
            db_name, tbl_name, input);
        return HiveClientHelper.FuncExceptionHandler(
            () -> HiveDataAccessor.toPartitionList(polyCatClient.getPartitionsByFilter(request)));
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String s, String s1, String s2, int i)
        throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String s, String s1, String s2, String s3, int i)
        throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public boolean listPartitionsByExpr(String db_name, String tbl_name,
        byte[] expr, String default_partition_name, short max_parts,
        List<org.apache.hadoop.hive.metastore.api.Partition> result)
        throws TException {
        return listPartitionsByExpr(context.getDefaultCatalogName(), db_name, tbl_name, expr, default_partition_name,
            max_parts, result);
    }

    @Override
    public boolean listPartitionsByExpr(String catName, String db_name, String tbl_name, byte[] expr,
        String default_partition_name, int max_parts, List<org.apache.hadoop.hive.metastore.api.Partition> result)
        throws TException {
        GetPartitionsByExprInput input = new GetPartitionsByExprInput(default_partition_name, expr, max_parts);
        ListPartitionsByExprRequest request = new ListPartitionsByExprRequest(context.getProjectId(), catName, db_name,
            tbl_name, input);
        List<Partition> lsmParts = polyCatClient.listPartitionsByExpr(request);
        result.addAll(lsmParts.stream()
            .map(HiveDataAccessor::toPartition)
            .collect(Collectors.toList()));
        return true;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String dbName,
        String tableName, short maxParts, String userName, List<String> groupNames)
        throws MetaException, TException, NoSuchObjectException {
        return listPartitionsWithAuthInfo(context.getDefaultCatalogName(), dbName, tableName, null, maxParts, userName,
            groupNames);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String catName,
        String dbName, String tableName,
        int maxParts, String userName, List<String> groupNames)
        throws MetaException, TException, NoSuchObjectException {
        return listPartitionsWithAuthInfo(catName, dbName, tableName, null, maxParts, userName, groupNames);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(String db_name, String tbl_name,
        List<String> part_names)
        throws NoSuchObjectException, MetaException, TException {
        return getPartitionsByNames(context.getDefaultCatalogName(), db_name, tbl_name, part_names);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(String catName, String db_name,
        String tbl_name,
        List<String> part_names)
        throws NoSuchObjectException, MetaException, TException {
        HiveClientHelper.checkInputIsNoNull(catName, db_name, tbl_name, part_names);
        PartitionFilterInput input = new PartitionFilterInput();
        input.setPartNames(part_names.toArray(new String[0]));
        GetTablePartitionsByNamesRequest request = new GetTablePartitionsByNamesRequest(context.getProjectId(),
            catName, db_name, tbl_name, input);
        return HiveClientHelper.FuncExceptionHandler(
            () -> HiveDataAccessor.toPartitionList(polyCatClient.getPartitionsByNames(request)));
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String dbName,
        String tableName, List<String> partialPvals, short maxParts, String userName,
        List<String> groupNames) throws MetaException, TException, NoSuchObjectException {
        return listPartitionsWithAuthInfo(context.getDefaultCatalogName(), dbName, tableName, partialPvals, maxParts,
            userName, groupNames);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String catName,
        String dbName, String tableName,
        List<String> partialPvals, int maxParts, String userName,
        List<String> groupNames) throws MetaException, TException, NoSuchObjectException {
        GetPartitionsWithAuthInput input = new GetPartitionsWithAuthInput(maxParts, userName, groupNames, partialPvals);
        ListPartitionsWithAuthRequest request = new ListPartitionsWithAuthRequest(context.getProjectId(), catName,
            dbName, tableName, input);
        return HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.listPartitionsPsWithAuth(request).stream()
            .map(HiveDataAccessor::toPartition)
            .collect(Collectors.toList()));
    }

    @Override
    public void markPartitionForEvent(String s, String s1, Map<String, String> map,
        PartitionEventType partitionEventType)
        throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {

    }

    @Override
    public void markPartitionForEvent(String s, String s1, String s2, Map<String, String> map,
        PartitionEventType partitionEventType)
        throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {

    }

    @Override
    public boolean isPartitionMarkedForEvent(String s, String s1, Map<String, String> map,
        PartitionEventType partitionEventType)
        throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
        return false;
    }

    @Override
    public boolean isPartitionMarkedForEvent(String s, String s1, String s2, Map<String, String> map,
        PartitionEventType partitionEventType)
        throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
        return false;
    }

    @Override
    public void validatePartitionNameCharacters(List<String> list) throws TException, MetaException {

    }

    @Override
    public void createTable(Table table)
        throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
        CreateTableRequest request = new CreateTableRequest();
        if (!table.isSetCatName()) {
            table.setCatName(context.getDefaultCatalogName());
        }
        request.setProjectId(context.getProjectId());
        request.setCatalogName(table.getCatName());
        request.setDatabaseName(table.getDbName());
        request.setInput(PolyCatDataAccessor.toTableInput(table));
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.createTable(request));
    }

    @Override
    public void alter_table(String databaseName, String tblName, Table table)
        throws InvalidOperationException, MetaException, TException {
        alter_table(context.getDefaultCatalogName(), databaseName, tblName, table, null);
    }

    @Override
    public void alter_table(String catName, String dbName, String tblName, Table newTable,
        EnvironmentContext envContext)
        throws InvalidOperationException, MetaException, TException {
        AlterTableRequest request = new AlterTableRequest();
        request.setCatalogName(catName);
        request.setDatabaseName(dbName);
        request.setTableName(tblName);
        request.setProjectId(context.getProjectId());
        AlterTableInput alterTableInput =
            new AlterTableInput(PolyCatDataAccessor.toTableInput(newTable), PolyCatDataAccessor.toAlterTableParams(envContext));
        request.setInput(alterTableInput);
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.alterTable(request));
    }

    @Override
    public void alter_table(String dbName, String tblName, Table table,
        boolean cascade)
        throws InvalidOperationException, MetaException, TException {
        EnvironmentContext envInput = null;
        if (cascade) {
            envInput = new EnvironmentContext();
            envInput.putToProperties(HiveDataAccessor.CASCADE, HiveDataAccessor.TRUE);
        }
        alter_table(context.getDefaultCatalogName(), dbName, tblName, table, envInput);
    }

    @Override
    public void alter_table_with_environmentContext(String databaseName, String tblName, Table table,
        EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
        alter_table(context.getDefaultCatalogName(), databaseName, tblName, table, environmentContext);
    }

    @Override
    public void createDatabase(Database database)
        throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        if (!database.isSetCatalogName()) {
            database.setCatalogName(context.getDefaultCatalogName());
        }
        DatabaseInput input = PolyCatDataAccessor.toDatabaseInput(database, context.getCatalogContext().getTenantName(), context.getCatalogContext().getAuthSourceType());
        CreateDatabaseRequest request = new CreateDatabaseRequest();
        request.setInput(input);
        request.setProjectId(context.getProjectId());
        request.setCatalogName(database.getCatalogName());
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.createDatabase(request));
    }

    @Override
    public void dropDatabase(String name)
        throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        dropDatabase(name, false, false, false);
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
        throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        dropDatabase(name, deleteData, ignoreUnknownDb, false);
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
        throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        dropDatabase(context.getDefaultCatalogName(), name, deleteData, ignoreUnknownDb, cascade);
    }

    @Override
    public void dropDatabase(String catName, String dbName, boolean deleteData, boolean ignoreUnknownDb,
        boolean cascade)
        throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        DeleteDatabaseRequest req = new DeleteDatabaseRequest(context.getProjectId(), catName, dbName);
        req.setCascade(cascade);
        req.setIgnoreUnknownObj(ignoreUnknownDb);
        req.setDeleteData(deleteData);
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.deleteDatabase(req));
    }

    @Override
    public void alterDatabase(String dbName, Database newDb) throws NoSuchObjectException, MetaException, TException {
        alterDatabase(context.getDefaultCatalogName(), dbName, newDb);
    }

    @Override
    public void alterDatabase(String catName, String dbName, Database newDb)
        throws NoSuchObjectException, MetaException, TException {
        DatabaseInput input = PolyCatDataAccessor.toDatabaseInput(newDb, context.getCatalogContext().getTenantName(), context.getCatalogContext().getAuthSourceType());
        AlterDatabaseRequest request = new AlterDatabaseRequest(context.getProjectId(), catName, dbName, input);
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.alterDatabase(request));
    }

    @Override
    public boolean dropPartition(String db_name, String tbl_name,
        List<String> part_vals, boolean deleteData)
        throws NoSuchObjectException, MetaException, TException {
        return dropPartition(context.getDefaultCatalogName(), db_name, tbl_name, part_vals, deleteData);
    }

    @Override
    public boolean dropPartition(String catName, String db_name, String tbl_name,
        List<String> part_vals, boolean deleteData)
        throws NoSuchObjectException, MetaException, TException {
        PartitionDropOptions options = new PartitionDropOptions();
        options.returnResults(false);
        options.deleteData(deleteData);
        return dropPartition(catName, db_name, tbl_name, part_vals, options);
    }

    @Override
    public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals,
        PartitionDropOptions options)
        throws NoSuchObjectException, MetaException, TException {
        return dropPartition(context.getDefaultCatalogName(), db_name, tbl_name, part_vals, options);
    }

    @Override
    public boolean dropPartition(String catName, String db_name, String tbl_name, List<String> part_vals,
        PartitionDropOptions options) throws NoSuchObjectException, MetaException, TException {
        Table table = getTable(catName, db_name, tbl_name);
        String partName = PolyCatDataAccessor.getPartitionName(part_vals, table);
        Optional<PartitionDropOptions> opt = Optional.ofNullable(options);
        DropPartitionInput input = new DropPartitionInput(Collections.singletonList(partName),
            opt.map(op -> op.deleteData).orElse(true),
            opt.map(op -> op.ifExists).orElse(false),
            opt.map(op -> op.returnResults).orElse(true),
            opt.map(op -> op.purgeData).orElse(false));
        DropPartitionRequest request = new DropPartitionRequest(context.getProjectId(), catName, db_name, tbl_name,
            input);

        return HiveClientHelper.FuncExceptionHandler(() -> {
            polyCatClient.dropPartition(request);
            return true;
        });
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(String dbName, String tblName,
        List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
        boolean ifExists) throws NoSuchObjectException, MetaException, TException {
        return dropPartitions(dbName, tblName, partExprs, deleteData, ifExists, false);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(String dbName, String tblName,
        List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
        boolean ifExists, boolean needResults) throws NoSuchObjectException, MetaException, TException {
        PartitionDropOptions options = new PartitionDropOptions();
        options.deleteData(deleteData);
        options.ifExists(ifExists);
        options.returnResults(needResults);
        return dropPartitions(context.getDefaultCatalogName(), dbName, tblName, partExprs, options);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(String dbName, String tblName,
        List<ObjectPair<Integer, byte[]>> partExprs,
        PartitionDropOptions options) throws NoSuchObjectException, MetaException, TException {
        return dropPartitions(context.getDefaultCatalogName(), dbName, tblName, partExprs, options);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(String catName, String dbName,
        String tblName,
        List<ObjectPair<Integer, byte[]>> partExprs,
        PartitionDropOptions options) throws NoSuchObjectException, MetaException, TException {
        List<Pair<Integer, byte[]>> exprs = PolyCatDataAccessor.toPartitonExprs(partExprs);
        DropPartitionsByExprsInput input = new DropPartitionsByExprsInput(exprs);
        input.setDeleteData(options.deleteData);
        input.setIfExists(options.ifExists);
        input.setPurgeData(options.purgeData);
        input.setReturnResults(options.returnResults);
        DropPartitionsRequest request = new DropPartitionsRequest(context.getProjectId(), catName, dbName, tblName,
            input);
        return HiveClientHelper.FuncExceptionHandler(
            () -> Arrays.stream(polyCatClient.dropPartitionsByExpr(request))
                .map(HiveDataAccessor::toPartition)
                .collect(Collectors.toList()));
    }

    @Override
    public boolean dropPartition(String db_name, String tbl_name,
        String name, boolean deleteData)
        throws NoSuchObjectException, MetaException, TException {
        return dropPartition(context.getDefaultCatalogName(), db_name, tbl_name, name, deleteData);
    }

    @Override
    public boolean dropPartition(String catName, String db_name, String tbl_name,
        String name, boolean deleteData)
        throws NoSuchObjectException, MetaException, TException {
        DropPartitionInput input = new DropPartitionInput(Collections.singletonList(name), deleteData, false, false,
            false);
        DropPartitionRequest request = new DropPartitionRequest(context.getProjectId(), catName, db_name, tbl_name,
            input);
        return HiveClientHelper.FuncExceptionHandler(() -> {
            polyCatClient.dropPartition(request);
            return true;
        });
    }

    @Override
    public void alter_partition(String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Partition newPart)
        throws InvalidOperationException, MetaException, TException {
        alter_partition(context.getDefaultCatalogName(), dbName, tblName, newPart, null);
    }

    @Override
    public void alter_partition(String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Partition newPart,
        EnvironmentContext environmentContext)
        throws InvalidOperationException, MetaException, TException {
        alter_partition(context.getDefaultCatalogName(), dbName, tblName, newPart, environmentContext);
    }

    @Override
    public void alter_partition(String catName, String dbName, String tblName,
        org.apache.hadoop.hive.metastore.api.Partition newPart,
        EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
        alter_partitions(catName, dbName, tblName, Collections.singletonList(newPart), environmentContext);
    }

    @Override
    public void alter_partitions(String dbName, String tblName,
        List<org.apache.hadoop.hive.metastore.api.Partition> newParts)
        throws InvalidOperationException, MetaException, TException {
        alter_partitions(context.getDefaultCatalogName(), dbName, tblName, newParts, null);
    }

    @Override
    public void alter_partitions(String dbName, String tblName,
        List<org.apache.hadoop.hive.metastore.api.Partition> newParts,
        EnvironmentContext environmentContext)
        throws InvalidOperationException, MetaException, TException {
        alter_partitions(context.getDefaultCatalogName(), dbName, tblName, newParts, environmentContext);
    }

    @Override
    public void alter_partitions(String catName, String dbName, String tblName,
        List<org.apache.hadoop.hive.metastore.api.Partition> newParts,
        EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

        PartitionAlterContext[] ctxs = newParts.stream().map(PolyCatDataAccessor::toPartitionAlterContext)
            .toArray(PartitionAlterContext[]::new);
        AlterPartitionInput input = new AlterPartitionInput(ctxs);
        AlterPartitionRequest request = new AlterPartitionRequest(context.getProjectId(), catName, dbName, tblName,
            input);
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.alterPartitions(request));
    }

    @Override
    public void renamePartition(final String dbname, final String tableName, final List<String> part_vals,
        final org.apache.hadoop.hive.metastore.api.Partition newPart)
        throws InvalidOperationException, MetaException, TException {
        renamePartition(context.getDefaultCatalogName(), dbname, tableName, part_vals, newPart);
    }

    @Override
    public void renamePartition(String catName, String dbname, String tableName, List<String> part_vals,
        org.apache.hadoop.hive.metastore.api.Partition newPart)
        throws InvalidOperationException, MetaException, TException {
        PartitionAlterContext[] ctxs = new PartitionAlterContext[1];
        ctxs[0] = PolyCatDataAccessor.toPartitionAlterContext(newPart);
        ctxs[0].setOldValues(part_vals);
        AlterPartitionInput input = new AlterPartitionInput(ctxs);
        RenamePartitionRequest request = new RenamePartitionRequest(context.getProjectId(), catName, dbname, tableName,
            input);
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.renamePartition(request));
    }

    @Override
    public List<FieldSchema> getFields(String s, String s1)
        throws MetaException, TException, UnknownTableException, UnknownDBException {
        return null;
    }

    @Override
    public List<FieldSchema> getFields(String s, String s1, String s2)
        throws MetaException, TException, UnknownTableException, UnknownDBException {
        return null;
    }

    @Override
    public List<FieldSchema> getSchema(String s, String s1)
        throws MetaException, TException, UnknownTableException, UnknownDBException {
        return null;
    }

    @Override
    public List<FieldSchema> getSchema(String s, String s1, String s2)
        throws MetaException, TException, UnknownTableException, UnknownDBException {
        return null;
    }

    @Override
    public String getConfigValue(String s, String s1) throws TException, ConfigValSecurityException {
        return null;
    }

    @Override
    public List<String> partitionNameToVals(String s) throws MetaException, TException {
        return null;
    }

    @Override
    public Map<String, String> partitionNameToSpec(String s) throws MetaException, TException {
        return null;
    }

    @Override
    public boolean updateTableColumnStatistics(ColumnStatistics columnStatistics)
        throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        ColumnStatisticsDesc desc = columnStatistics.getStatsDesc();
        UpdateTableColumnStatisticRequest request = new UpdateTableColumnStatisticRequest(context.getProjectId(),
            desc.getCatName(), desc.getDbName(), desc.getTableName(),
            PolyCatDataAccessor.toColumnStatistics(columnStatistics));
        return HiveClientHelper.FuncExceptionHandler(() -> {
            polyCatClient.updateTableColumnStatistics(request);
            return true;
        });
    }

    @Override
    public boolean updatePartitionColumnStatistics(ColumnStatistics columnStatistics)
        throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        ColumnStatisticsDesc desc = columnStatistics.getStatsDesc();
        ColumnStatisticsInput input = new ColumnStatisticsInput(PolyCatDataAccessor.toColumnStatistics(columnStatistics));
        UpdatePartitionColumnStatisticRequest request = new UpdatePartitionColumnStatisticRequest(
            context.getProjectId(),
            desc.getCatName(), desc.getDbName(), desc.getTableName(), input);
        return HiveClientHelper.FuncExceptionHandler(() -> {
            return polyCatClient.updatePartitionColumnStatistics(request);
        });
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
        List<String> colNames) throws NoSuchObjectException, MetaException, TException {
        return getTableColumnStatistics(context.getDefaultCatalogName(), dbName, tableName, colNames);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName, String tableName,
        List<String> colNames) throws NoSuchObjectException, MetaException, TException {
        GetTableColumnStatisticRequest request = new GetTableColumnStatisticRequest(context.getProjectId(), catName,
            dbName, tableName, colNames);
        return HiveClientHelper.FuncExceptionHandler(
            () -> Arrays.stream(polyCatClient.getTableColumnsStatistics(request))
                .map(HiveDataAccessor::convertToHiveStatsObj).collect(Collectors.toList()));
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName,
        String tableName, List<String> partNames, List<String> colNames)
        throws NoSuchObjectException, MetaException, TException {

        return getPartitionColumnStatistics(context.getDefaultCatalogName(), dbName, tableName, partNames, colNames);
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String catName, String dbName,
        String tableName, List<String> partNames, List<String> colNames)
        throws NoSuchObjectException, MetaException, TException {
        GetPartitionColumnStaticsInput input = new GetPartitionColumnStaticsInput(partNames, colNames);
        GetPartitionColumnStatisticsRequest request =
            new GetPartitionColumnStatisticsRequest(context.getProjectId(), catName, dbName, tableName, input);
        return HiveClientHelper.FuncExceptionHandler(() -> {
            PartitionStatisticData result = polyCatClient.getPartitionColumnStatistics(request);
            return HiveDataAccessor.toColumnStatisticsMap(result.getStatisticsResults());
        });
    }

    @Override
    public boolean deletePartitionColumnStatistics(String dbName, String tableName,
        String partName, String colName)
        throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return deletePartitionColumnStatistics(context.getDefaultCatalogName(), dbName, tableName, partName, colName);
    }

    @Override
    public boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName,
        String partName, String colName)
        throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        DeletePartitionColumnStatisticsRequest request = new DeletePartitionColumnStatisticsRequest(
            context.getProjectId(), catName, dbName, tableName, partName, colName);
        return HiveClientHelper.FuncExceptionHandler(() -> {
            polyCatClient.deletePartitionColumnStatistics(request);
            return true;
        });
    }

    @Override
    public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
        throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return deleteTableColumnStatistics(context.getDefaultCatalogName(), dbName, tableName, colName);
    }

    @Override
    public boolean deleteTableColumnStatistics(String catName, String dbName, String tableName, String colName)
        throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(context.getProjectId(), catName,
            dbName, tableName, colName);
        return HiveClientHelper.FuncExceptionHandler(() -> {
            polyCatClient.deleteTableColumnStatistics(request);
            return true;
        });
    }

    @Override
    public boolean create_role(Role role) throws MetaException, TException {
        return false;
    }

    @Override
    public boolean drop_role(String s) throws MetaException, TException {
        return false;
    }

    @Override
    public List<String> listRoleNames() throws MetaException, TException {
        return null;
    }

    @Override
    public boolean grant_role(String s, String s1, PrincipalType principalType, String s2, PrincipalType principalType1,
        boolean b) throws MetaException, TException {
        return false;
    }

    @Override
    public boolean revoke_role(String s, String s1, PrincipalType principalType, boolean b)
        throws MetaException, TException {
        return false;
    }

    @Override
    public List<Role> list_roles(String s, PrincipalType principalType) throws MetaException, TException {
        return null;
    }

    @Override
    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObjectRef, String s, List<String> list)
        throws MetaException, TException {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> list_privileges(String s, PrincipalType principalType, HiveObjectRef hiveObjectRef)
        throws MetaException, TException {
        return null;
    }

    @Override
    public boolean grant_privileges(PrivilegeBag privilegeBag) throws MetaException, TException {
        return false;
    }

    @Override
    public boolean revoke_privileges(PrivilegeBag privilegeBag, boolean b) throws MetaException, TException {
        return false;
    }

    @Override
    public boolean refresh_privileges(HiveObjectRef hiveObjectRef, String s, PrivilegeBag privilegeBag)
        throws MetaException, TException {
        return false;
    }

    @Override
    public String getDelegationToken(String owner, String renewerKerberosPrincipalName)
        throws MetaException, TException {
        GetTokenWithRenewerRequest request = new GetTokenWithRenewerRequest();
        request.setProjectId(context.getProjectId());
        request.setTokenType(Constants.MRS_TOKEN);
        request.setDelegationToken(owner, renewerKerberosPrincipalName);
        return HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.getTokenWithRenewer(request).getToken());
    }

    @Override
    public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
        AlterKerberosTokenRequest request = new AlterKerberosTokenRequest();
        request.setProjectId(context.getProjectId());
        request.setTokenType(Constants.MRS_TOKEN);
        request.setTokenId(tokenStrForm);
        request.setOperatorType(Constants.RENEW_TOKEN);
        return HiveClientHelper.FuncExceptionHandler(
            () -> Long.valueOf(polyCatClient.alterToken(request).getRetention()));
    }

    @Override
    public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
        AlterKerberosTokenRequest request = new AlterKerberosTokenRequest();
        request.setProjectId(context.getProjectId());
        request.setTokenType(Constants.MRS_TOKEN);
        request.setTokenId(tokenStrForm);
        request.setOperatorType(Constants.CANCEL_TOKEN);
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.alterToken(request));
    }

    @Override
    public String getTokenStrForm() throws IOException {
        throw new IOException(String.format(Locale.ROOT, "%s is not support", "getTokenStrForm"));
    }

    @Override
    public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
        AddTokenRequest request = new AddTokenRequest();
        request.setProjectId(context.getProjectId());
        TokenInput input = new TokenInput();
        input.setMRSToken(tokenIdentifier, delegationToken);
        request.setTokenType(Constants.MRS_TOKEN);
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.addToken(request));
        return true;
    }

    @Override
    public boolean removeToken(String tokenIdentifier) throws TException {
        DeleteTokenRequest request = new DeleteTokenRequest();
        request.setProjectId(context.getProjectId());
        request.setTokenType(Constants.MRS_TOKEN);
        request.setTokenId(tokenIdentifier);
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.deleteToken(request));
        return true;
    }

    @Override
    public String getToken(String tokenIdentifier) throws TException {
        GetTokenRequest request = new GetTokenRequest();
        request.setProjectId(context.getProjectId());
        request.setTokenType(Constants.MRS_TOKEN);
        request.setTokenId(tokenIdentifier);
        return HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.getToken(request).getToken());

    }

    @Override
    public List<String> getAllTokenIdentifiers() throws TException {
        ListTokenRequest request = new ListTokenRequest();
        request.setProjectId(context.getProjectId());
        request.setTokenType(Constants.MRS_TOKEN);
        request.setPattern(null);
        return HiveClientHelper.FuncExceptionHandler(() -> {
            PagedList<KerberosToken> tokens = polyCatClient.ListToken(request);
            return Arrays.stream(tokens.getObjects()).map(KerberosToken::getTokenId).collect(Collectors.toList());
        });
    }

    @Override
    public int addMasterKey(String key) throws MetaException, TException {
        AddTokenRequest request = new AddTokenRequest();
        request.setProjectId(context.getProjectId());
        TokenInput input = new TokenInput();
        input.setMasterKey(key);
        request.setTokenType(Constants.MASTER_KEY);
        request.setInput(input);
        return HiveClientHelper.FuncExceptionHandler(
            () -> Integer.valueOf(polyCatClient.addToken(request).getTokenId()));
    }

    @Override
    public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException, MetaException, TException {
        AddTokenRequest request = new AddTokenRequest();
        request.setProjectId(context.getProjectId());
        TokenInput input = new TokenInput();
        input.setMasterKey(seqNo, key);
        request.setTokenType(Constants.MASTER_KEY);
        request.setInput(input);
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.addToken(request));
    }

    @Override
    public boolean removeMasterKey(Integer keySeq) throws TException {
        DeleteTokenRequest request = new DeleteTokenRequest();
        request.setProjectId(context.getProjectId());
        request.setTokenType(Constants.MASTER_KEY);
        request.setTokenId(String.valueOf(keySeq));
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.deleteToken(request));
        return true;
    }

    @Override
    public String[] getMasterKeys() throws TException {
        ListTokenRequest request = new ListTokenRequest();
        request.setProjectId(context.getProjectId());
        request.setTokenType(Constants.MASTER_KEY);
        return HiveClientHelper.FuncExceptionHandler(() -> {
            PagedList<KerberosToken> tokenList = polyCatClient.ListToken(request);
            return Arrays.stream(tokenList.getObjects()).map(KerberosToken::getToken).toArray(String[]::new);
        });
    }

    @Override
    public void createFunction(Function function) throws InvalidObjectException, MetaException, TException {
        HiveClientHelper.checkInputIsNoNull(function.getDbName());

        CreateFunctionRequest request = new CreateFunctionRequest(context.getProjectId(),
            function.isSetCatName() ? function.getCatName() : context.getDefaultCatalogName(),
            PolyCatDataAccessor.toFunctionInput(function));
        HiveClientHelper.FuncExceptionHandler(() -> {
            polyCatClient.createFunction(request);
            return null;
        });
    }

    @Override
    public void alterFunction(String dbName, String funcName, Function newFunction)
        throws InvalidObjectException, MetaException, TException {
        alterFunction(context.getDefaultCatalogName(), dbName, funcName, newFunction);
    }

    @Override
    public void alterFunction(String catName, String dbName, String funcName, Function newFunction)
        throws InvalidObjectException, MetaException, TException {
        HiveClientHelper.checkInputIsNoNull(catName, dbName, funcName);
        AlterFunctionRequest request = new AlterFunctionRequest(context.getProjectId(),
            catName,
            dbName,
            PolyCatDataAccessor.toFunctionInput(newFunction),
            funcName);
        HiveClientHelper.FuncExceptionHandler(() -> {
            polyCatClient.alterFunction(request);
            return null;
        });
    }

    @Override
    public void dropFunction(String dbName, String funcName)
        throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
        dropFunction(context.getDefaultCatalogName(), dbName, funcName);
    }

    @Override
    public void dropFunction(String catName, String dbName, String funcName)
        throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
        FunctionRequestBase request = new FunctionRequestBase(context.getProjectId(), catName, dbName, funcName);
        HiveClientHelper.FuncExceptionHandler(() -> {
            polyCatClient.dropFunction(request);
            return null;
        });
    }

    @Override
    public Function getFunction(String dbName, String funcName) throws MetaException, TException {
        return getFunction(context.getDefaultCatalogName(), dbName, funcName);
    }

    @Override
    public Function getFunction(String catName, String dbName, String funcName) throws MetaException, TException {
        HiveClientHelper.checkInputIsNoNull(catName, dbName, funcName);
        GetFunctionRequest request = new GetFunctionRequest(context.getProjectId(), catName, dbName, funcName);
        return HiveClientHelper.FuncExceptionHandler(
            () -> HiveDataAccessor.toFunction(polyCatClient.getFunction(request)));
    }

    @Override
    public List<String> getFunctions(String dbName, String pattern) throws MetaException, TException {
        return getFunctions(context.getDefaultCatalogName(), dbName, pattern);
    }

    @Override
    public List<String> getFunctions(String catName, String dbName, String pattern) throws MetaException, TException {
        HiveClientHelper.checkInputIsNoNull(catName, dbName);
        ListFunctionRequest request = new ListFunctionRequest(context.getProjectId(), catName, dbName, pattern);
        return HiveClientHelper.FuncExceptionHandler(
            () -> Arrays.asList(polyCatClient.listFunctions(request).getObjects()));
    }

    @Override
    public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
        GetAllFunctionRequest request = new GetAllFunctionRequest(context.getProjectId(),
            context.getDefaultCatalogName());
        List<Function> functions = HiveClientHelper.FuncExceptionHandler(
            () -> Arrays.stream(polyCatClient.getAllFunctions(request).getObjects()).map(HiveDataAccessor::toFunction)
                .collect(Collectors.toList()));
        GetAllFunctionsResponse response = new GetAllFunctionsResponse();
        response.setFunctions(functions);
        return response;
    }

    @Override
    public ValidTxnList getValidTxns() throws TException {
        return null;
    }

    @Override
    public ValidTxnList getValidTxns(long l) throws TException {
        return null;
    }

    @Override
    public ValidWriteIdList getValidWriteIds(String s) throws TException {
        return null;
    }

    @Override
    public List<TableValidWriteIds> getValidWriteIds(List<String> list, String s) throws TException {
        return null;
    }

    @Override
    public long openTxn(String s) throws TException {
        return 0;
    }

    @Override
    public List<Long> replOpenTxn(String s, List<Long> list, String s1) throws TException {
        return null;
    }

    @Override
    public OpenTxnsResponse openTxns(String s, int i) throws TException {
        return null;
    }

    @Override
    public void rollbackTxn(long l) throws NoSuchTxnException, TException {

    }

    @Override
    public void replRollbackTxn(long l, String s) throws NoSuchTxnException, TException {

    }

    @Override
    public void commitTxn(long l) throws NoSuchTxnException, TxnAbortedException, TException {

    }

    @Override
    public void replCommitTxn(long l, String s) throws NoSuchTxnException, TxnAbortedException, TException {

    }

    @Override
    public void abortTxns(List<Long> list) throws TException {

    }

    @Override
    public long allocateTableWriteId(long l, String s, String s1) throws TException {
        return 0;
    }

    @Override
    public void replTableWriteIdState(String s, String s1, String s2, List<String> list) throws TException {

    }

    @Override
    public List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> list, String s, String s1) throws TException {
        return null;
    }

    @Override
    public List<TxnToWriteId> replAllocateTableWriteIdsBatch(String s, String s1, String s2, List<TxnToWriteId> list)
        throws TException {
        return null;
    }

    @Override
    public GetOpenTxnsInfoResponse showTxns() throws TException {
        return null;
    }

    @Override
    public LockResponse lock(LockRequest lockRequest) throws NoSuchTxnException, TxnAbortedException, TException {
        return null;
    }

    @Override
    public LockResponse checkLock(long l)
        throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
        return null;
    }

    @Override
    public void unlock(long l) throws NoSuchLockException, TxnOpenException, TException {

    }

    @Override
    public ShowLocksResponse showLocks() throws TException {
        return null;
    }

    @Override
    public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
        return null;
    }

    @Override
    public void heartbeat(long l, long l1)
        throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {

    }

    @Override
    public HeartbeatTxnRangeResponse heartbeatTxnRange(long l, long l1) throws TException {
        return null;
    }

    @Override
    public void compact(String s, String s1, String s2, CompactionType compactionType) throws TException {

    }

    @Override
    public void compact(String s, String s1, String s2, CompactionType compactionType, Map<String, String> map)
        throws TException {

    }

    @Override
    public CompactionResponse compact2(String s, String s1, String s2, CompactionType compactionType,
        Map<String, String> map) throws TException {
        return null;
    }

    @Override
    public ShowCompactResponse showCompactions() throws TException {
        return null;
    }

    @Override
    public void addDynamicPartitions(long l, long l1, String s, String s1, List<String> list) throws TException {

    }

    @Override
    public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName, List<String> partNames,
        DataOperationType operationType) throws TException {

    }

    @Override
    public void insertTable(Table table, boolean b) throws MetaException {

    }

    @Override
    public NotificationEventResponse getNextNotification(long l, int i, NotificationFilter notificationFilter)
        throws TException {
        return null;
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
        //mock
        return new CurrentNotificationEventId(System.currentTimeMillis());
    }

    @Override
    public NotificationEventsCountResponse getNotificationEventsCount(
        NotificationEventsCountRequest notificationEventsCountRequest) throws TException {
        return null;
    }

    @Override
    public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
        return null;
    }

    @Override
    public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest getPrincipalsInRoleRequest)
        throws MetaException, TException {
        return null;
    }

    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
        GetRoleGrantsForPrincipalRequest getRoleGrantsForPrincipalRequest) throws MetaException, TException {
        return null;
    }

    @Override
    public AggrStats getAggrColStatsFor(String dbName, String tblName,
        List<String> colNames, List<String> partName)
        throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public AggrStats getAggrColStatsFor(String catName, String dbName, String tblName,
        List<String> colNames, List<String> partNames)
        throws NoSuchObjectException, MetaException, TException {
        GetPartitionColumnStaticsInput input = new GetPartitionColumnStaticsInput(partNames, colNames);
        GetAggregateColumnStatisticsRequest request = new GetAggregateColumnStatisticsRequest(context.getProjectId(),
            catName, dbName, tblName, input);
        return HiveClientHelper.FuncExceptionHandler(
            () -> HiveDataAccessor.toAggrStats(polyCatClient.getAggrColStats(request)));
    }

    @Override
    public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest hiveRequest)
        throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
        SetPartitionColumnStatisticsInput input = generateSetPartStatsInput(hiveRequest);
        SetPartitionColumnStatisticsRequest request = new SetPartitionColumnStatisticsRequest();
        ColumnStatisticsDesc desc = hiveRequest.getColStats().get(0).getStatsDesc();
        request.setProjectId(context.getProjectId());
        request.setCatalogName(desc.isSetCatName() ? desc.getCatName() : context.getDefaultCatalogName());
        request.setDatabaseName(desc.getDbName());
        request.setTableName(desc.getTableName());
        request.setInput(input);
        return HiveClientHelper.FuncExceptionHandler(() -> {
            polyCatClient.setPartitionsColumnStatistics(request);
            return true;
        });
    }

    private SetPartitionColumnStatisticsInput generateSetPartStatsInput(SetPartitionsStatsRequest hiveRequest)
        throws TException {
        SetPartitionColumnStatisticsInput input = new SetPartitionColumnStatisticsInput();
        input.setNeedMerge(hiveRequest.isNeedMerge());

        List<io.polycat.catalog.common.model.stats.ColumnStatistics> lsmStatsList = new ArrayList<>(
            hiveRequest.getColStatsSize());
        List<ColumnStatistics> hiveStatsList = hiveRequest.getColStats();
        for (ColumnStatistics hiveStats : hiveStatsList) {
            lsmStatsList.add(PolyCatDataAccessor.toColumnStatistics(hiveStats));
        }
        input.setStats(lsmStatsList);
        return input;
    }

    @Override
    public void flushCache() {

    }

    @Override
    public Iterable<Entry<Long, ByteBuffer>> getFileMetadata(List<Long> list) throws TException {
        return null;
    }

    @Override
    public Iterable<Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(List<Long> list, ByteBuffer byteBuffer,
        boolean b) throws TException {
        return null;
    }

    @Override
    public void clearFileMetadata(List<Long> list) throws TException {

    }

    @Override
    public void putFileMetadata(List<Long> list, List<ByteBuffer> list1) throws TException {

    }

    @Override
    public boolean isSameConfObj(Configuration configuration) {
        return false;
    }

    public boolean isSameConfObj(HiveConf conf) {
        return this.conf == conf;
    }

    @Override
    public boolean cacheFileMetadata(String s, String s1, String s2, boolean b) throws TException {
        return false;
    }

    @Override
    public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest primaryKeysRequest)
        throws MetaException, NoSuchObjectException, TException {
        GetPrimaryKeysRequest request = new GetPrimaryKeysRequest(context.getProjectId(),
            primaryKeysRequest.isSetCatName() ? primaryKeysRequest.getCatName() : context.getDefaultCatalogName(),
            primaryKeysRequest.getDb_name(),
            primaryKeysRequest.getTbl_name());
        return HiveClientHelper.FuncExceptionHandler(
            () -> Arrays.stream(polyCatClient.getPrimaryKeys(request).getObjects())
                .map(HiveDataAccessor::toSQLPrimaryKey)
                .collect(Collectors.toList()));
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest foreignKeysRequest)
        throws MetaException, NoSuchObjectException, TException {
        GetForeignKeysReq request = new GetForeignKeysReq(context.getProjectId(),
            foreignKeysRequest.isSetCatName() ? foreignKeysRequest.getCatName() : context.getDefaultCatalogName(),
            foreignKeysRequest.getForeign_db_name(),
            foreignKeysRequest.getForeign_tbl_name(),
            foreignKeysRequest.getParent_db_name(),
            foreignKeysRequest.getParent_tbl_name());
        return HiveClientHelper.FuncExceptionHandler(() ->
            Arrays.stream(polyCatClient.getForeignKeys(request).getObjects())
                .map(HiveDataAccessor::toSQLForgeinKey)
                .collect(Collectors.toList()));
    }

    @Override
    public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest uniqueConstraintsRequest)
        throws MetaException, NoSuchObjectException, TException {
        return getConstraints(
            uniqueConstraintsRequest.isSetCatName() ?
                uniqueConstraintsRequest.getCatName() : context.getDefaultCatalogName(),
            uniqueConstraintsRequest.getDb_name(),
            uniqueConstraintsRequest.getTbl_name(),
            ConstraintType.UNIQUE_CSTR.name(),
            HiveDataAccessor::toUniqueConstraint);
    }

    @Override
    public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest notNullConstraintsRequest)
        throws MetaException, NoSuchObjectException, TException {
        return getConstraints(
            notNullConstraintsRequest.isSetCatName() ?
                notNullConstraintsRequest.getCatName() : context.getDefaultCatalogName(),
            notNullConstraintsRequest.getDb_name(),
            notNullConstraintsRequest.getTbl_name(),
            ConstraintType.NOT_NULL_CSTR.name(),
            HiveDataAccessor::toNotNullConstraint);
    }

    @Override
    public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest defaultConstraintsRequest)
        throws MetaException, NoSuchObjectException, TException {
        return getConstraints(
            defaultConstraintsRequest.isSetCatName() ?
                defaultConstraintsRequest.getCatName() : context.getDefaultCatalogName(),
            defaultConstraintsRequest.getDb_name(),
            defaultConstraintsRequest.getTbl_name(),
            ConstraintType.DEFAULT_CSTR.name(),
            HiveDataAccessor::toDefaultConstraint);
    }

    @Override
    public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest checkConstraintsRequest)
        throws MetaException, NoSuchObjectException, TException {
        return getConstraints(
            checkConstraintsRequest.isSetCatName() ?
                checkConstraintsRequest.getCatName() : context.getDefaultCatalogName(),
            checkConstraintsRequest.getDb_name(),
            checkConstraintsRequest.getTbl_name(),
            ConstraintType.CHECK_CSTR.name(),
            HiveDataAccessor::toCheckConstraint);
    }

    private <T> List<T> getConstraints(String catName, String dbName, String tblName,
        String constraintType, java.util.function.Function<Constraint, T> toUniqueConstraint)
        throws TException {
        GetConstraintsRequest request = new GetConstraintsRequest(context.getProjectId(),
            catName, dbName, tblName, constraintType);
        return HiveClientHelper.FuncExceptionHandler(() ->
            Arrays.stream(polyCatClient.getConstraints(request).getObjects())
                .map(toUniqueConstraint)
                .collect(Collectors.toList()));
    }

    @Override
    public void createTableWithConstraints(
        Table tTbl,
        List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
        List<SQLUniqueConstraint> uniqueConstraints,
        List<SQLNotNullConstraint> notNullConstraints,
        List<SQLDefaultConstraint> defaultConstraints,
        List<SQLCheckConstraint> checkConstraints)
        throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
        createTable(tTbl);
        addCheckConstraint(checkConstraints);
        addNotNullConstraint(notNullConstraints);
        addDefaultConstraint(defaultConstraints);
        addUniqueConstraint(uniqueConstraints);
        addPrimaryKey(primaryKeys);
        addForeignKey(foreignKeys);
    }

    @Override
    public void dropConstraint(String dbName, String tableName, String constraintName)
        throws MetaException, NoSuchObjectException, TException {
        dropConstraint(context.getDefaultCatalogName(), dbName, tableName, constraintName);
    }

    @Override
    public void dropConstraint(String catName, String dbName, String tableName, String constraintName)
        throws MetaException, NoSuchObjectException, TException {
        DeleteConstraintRequest request = new DeleteConstraintRequest(
            context.getProjectId(),
            catName,
            dbName,
            tableName,
            constraintName);
        HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.dropConstraint(request));
    }

    @Override
    public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols)
        throws MetaException, NoSuchObjectException, TException {
        if (primaryKeyCols != null && !primaryKeyCols.isEmpty()) {
            SQLPrimaryKey firstKey = primaryKeyCols.get(0);
            List<PrimaryKey> lsmKeyCols =
                primaryKeyCols.stream().map(PolyCatDataAccessor::toPrimaryKey).collect(Collectors.toList());
            AddPrimaryKeysRequest request = new AddPrimaryKeysRequest(
                context.getProjectId(),
                firstKey.isSetCatName() ? firstKey.getCatName() : context.getDefaultCatalogName(),
                firstKey.getTable_db(),
                firstKey.getTable_name(),
                new AddPrimaryKeysInput(lsmKeyCols));
            HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.addPrimaryKey(request));
        }
    }

    @Override
    public void addForeignKey(List<SQLForeignKey> foreignKeyCols)
        throws MetaException, NoSuchObjectException, TException {
        if (foreignKeyCols != null && !foreignKeyCols.isEmpty()) {
            SQLForeignKey firstKey = foreignKeyCols.get(0);
            List<ForeignKey> lsmKeyCols =
                foreignKeyCols.stream().map(PolyCatDataAccessor::toForeignKey).collect(Collectors.toList());
            AddForeignKeysRequest request = new AddForeignKeysRequest(
                context.getProjectId(),
                firstKey.isSetCatName() ? firstKey.getCatName() : context.getDefaultCatalogName(),
                firstKey.getPktable_db(),
                firstKey.getPktable_name(),
                new AddForeignKeysInput(lsmKeyCols));
            HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.addForeignKey(request));
        }
    }

    @Override
    public void addUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraintCols)
        throws MetaException, NoSuchObjectException, TException {
        if (uniqueConstraintCols != null && !uniqueConstraintCols.isEmpty()) {
            List<Constraint> lsmConstraintCols = PolyCatDataAccessor.toUniqueConstraint(uniqueConstraintCols);
            SQLUniqueConstraint firstCstr = uniqueConstraintCols.get(0);
            AddConstraintsRequest request = new AddConstraintsRequest(
                context.getProjectId(),
                firstCstr.isSetCatName() ? firstCstr.getCatName() : context.getDefaultCatalogName(),
                firstCstr.getTable_db(),
                firstCstr.getTable_name(),
                new AddConstraintsInput(lsmConstraintCols));
            HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.addConstraint(request));
        }
    }

    @Override
    public void addNotNullConstraint(List<SQLNotNullConstraint> notNullConstraintCols)
        throws MetaException, NoSuchObjectException, TException {
        if (notNullConstraintCols != null && !notNullConstraintCols.isEmpty()) {
            List<Constraint> lsmConstraintCols = PolyCatDataAccessor.toNotNullConstraint(notNullConstraintCols);
            SQLNotNullConstraint firstCstr = notNullConstraintCols.get(0);
            AddConstraintsRequest request = new AddConstraintsRequest(
                context.getProjectId(),
                firstCstr.isSetCatName() ? firstCstr.getCatName() : context.getDefaultCatalogName(),
                firstCstr.getTable_db(),
                firstCstr.getTable_name(),
                new AddConstraintsInput(lsmConstraintCols));
            HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.addConstraint(request));
        }
    }

    @Override
    public void addDefaultConstraint(List<SQLDefaultConstraint> defaultConstraints)
        throws MetaException, NoSuchObjectException, TException {
        if (defaultConstraints != null && !defaultConstraints.isEmpty()) {
            List<Constraint> lsmConstraintCols = PolyCatDataAccessor.toDefaultConstraint(defaultConstraints);
            SQLDefaultConstraint firstCstr = defaultConstraints.get(0);
            AddConstraintsRequest request = new AddConstraintsRequest(
                context.getProjectId(),
                firstCstr.isSetCatName() ? firstCstr.getCatName() : context.getDefaultCatalogName(),
                firstCstr.getTable_db(),
                firstCstr.getTable_name(),
                new AddConstraintsInput(lsmConstraintCols));
            HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.addConstraint(request));
        }
    }

    @Override
    public void addCheckConstraint(List<SQLCheckConstraint> checkConstraints)
        throws MetaException, NoSuchObjectException, TException {
        if (checkConstraints != null && !checkConstraints.isEmpty()) {
            List<Constraint> lsmConstraintCols = PolyCatDataAccessor.toCheckConstraint(checkConstraints);
            SQLCheckConstraint firstCstr = checkConstraints.get(0);
            AddConstraintsRequest request = new AddConstraintsRequest(
                context.getProjectId(),
                firstCstr.isSetCatName() ? firstCstr.getCatName() : context.getDefaultCatalogName(),
                firstCstr.getTable_db(),
                firstCstr.getTable_name(),
                new AddConstraintsInput(lsmConstraintCols));
            HiveClientHelper.FuncExceptionHandler(() -> polyCatClient.addConstraint(request));
        }
    }

    @Override
    public String getMetastoreDbUuid() throws MetaException, TException {
        return null;
    }

    @Override
    public void createResourcePlan(WMResourcePlan wmResourcePlan, String s)
        throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public WMFullResourcePlan getResourcePlan(String s) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<WMResourcePlan> getAllResourcePlans() throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public void dropResourcePlan(String s) throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public WMFullResourcePlan alterResourcePlan(String s, WMNullableResourcePlan wmNullableResourcePlan, boolean b,
        boolean b1, boolean b2) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        return null;
    }

    @Override
    public WMFullResourcePlan getActiveResourcePlan() throws MetaException, TException {
        return null;
    }

    @Override
    public WMValidateResourcePlanResponse validateResourcePlan(String s)
        throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        return null;
    }

    @Override
    public void createWMTrigger(WMTrigger wmTrigger) throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alterWMTrigger(WMTrigger wmTrigger)
        throws NoSuchObjectException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public void dropWMTrigger(String s, String s1) throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public List<WMTrigger> getTriggersForResourcePlan(String s)
        throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public void createWMPool(WMPool wmPool)
        throws NoSuchObjectException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alterWMPool(WMNullablePool wmNullablePool, String s)
        throws NoSuchObjectException, InvalidObjectException, TException {

    }

    @Override
    public void dropWMPool(String s, String s1) throws TException {

    }

    @Override
    public void createOrUpdateWMMapping(WMMapping wmMapping, boolean b) throws TException {

    }

    @Override
    public void dropWMMapping(WMMapping wmMapping) throws TException {

    }

    @Override
    public void createOrDropTriggerToPoolMapping(String s, String s1, String s2, boolean b)
        throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public void createISchema(ISchema iSchema) throws TException {

    }

    @Override
    public void alterISchema(String s, String s1, String s2, ISchema iSchema) throws TException {

    }

    @Override
    public ISchema getISchema(String s, String s1, String s2) throws TException {
        return null;
    }

    @Override
    public void dropISchema(String s, String s1, String s2) throws TException {

    }

    @Override
    public void addSchemaVersion(SchemaVersion schemaVersion) throws TException {

    }

    @Override
    public SchemaVersion getSchemaVersion(String s, String s1, String s2, int i) throws TException {
        return null;
    }

    @Override
    public SchemaVersion getSchemaLatestVersion(String s, String s1, String s2) throws TException {
        return null;
    }

    @Override
    public List<SchemaVersion> getSchemaAllVersions(String s, String s1, String s2) throws TException {
        return null;
    }

    @Override
    public void dropSchemaVersion(String s, String s1, String s2, int i) throws TException {

    }

    @Override
    public FindSchemasByColsResp getSchemaByCols(FindSchemasByColsRqst findSchemasByColsRqst) throws TException {
        return null;
    }

    @Override
    public void mapSchemaVersionToSerde(String s, String s1, String s2, int i, String s3) throws TException {

    }

    @Override
    public void setSchemaVersionState(String s, String s1, String s2, int i, SchemaVersionState schemaVersionState)
        throws TException {

    }

    @Override
    public void addSerDe(SerDeInfo serDeInfo) throws TException {

    }

    @Override
    public SerDeInfo getSerDe(String s) throws TException {
        return null;
    }

    @Override
    public LockResponse lockMaterializationRebuild(String s, String s1, long l) throws TException {
        return null;
    }

    @Override
    public boolean heartbeatLockMaterializationRebuild(String s, String s1, long l) throws TException {
        return false;
    }

    @Override
    public void addRuntimeStat(RuntimeStat runtimeStat) throws TException {

    }

    @Override
    public List<RuntimeStat> getRuntimeStats(int i, int i1) throws TException {
        return null;
    }
}
