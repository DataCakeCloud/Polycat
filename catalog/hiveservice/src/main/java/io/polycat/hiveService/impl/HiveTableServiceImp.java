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
package io.polycat.hiveService.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.model.stats.ColumnStatistics;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import io.polycat.catalog.common.plugin.request.input.ColumnChangeInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.service.api.TableService;
import io.polycat.hiveService.common.HiveServiceHelper;
import io.polycat.hiveService.data.accesser.HiveDataAccessor;
import io.polycat.hiveService.data.accesser.PolyCatDataAccessor;
import io.polycat.hiveService.hive.HiveMetaStoreClientUtil;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HiveTableServiceImp implements TableService {


    @FunctionalInterface
    interface getTableConstraintFunction<R> {

        R getConstraintByName(String catName, String dbName, String tblName);

        default R apply(String catName, String dbName, String tblName) {
            return getConstraintByName(catName, dbName, tblName);
        }
    }

    private final Map<ConstraintType, Consumer<List<Constraint>>> addMap = new HashMap<>();
    private final Map<ConstraintType, Function<TableName, List<Constraint>>> getMap = new HashMap<>();

    public HiveTableServiceImp() {
        initStatsMap();
    }

    private void initStatsMap() {
        addMap.put(ConstraintType.CHECK_CSTR, this::addCheckConstraintToHive);
        addMap.put(ConstraintType.DEFAULT_CSTR, this::addDefaultConstraintToHive);
        addMap.put(ConstraintType.NOT_NULL_CSTR, this::addNotNullConstraintToHive);
        addMap.put(ConstraintType.UNIQUE_CSTR, this::addUniqueConstraintToHive);

        getMap.put(ConstraintType.CHECK_CSTR, this::getCheckConstraintFromHive);
        getMap.put(ConstraintType.DEFAULT_CSTR, this::getDefaultConstraintFromHive);
        getMap.put(ConstraintType.NOT_NULL_CSTR, this::getNotNullConstraintFromHive);
        getMap.put(ConstraintType.UNIQUE_CSTR, this::getUniqueConstraintFromHive);
    }

    @Override
    public void createTable(DatabaseName databaseName, TableInput tableInput) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> {
                org.apache.hadoop.hive.metastore.api.Table hiveTable = HiveDataAccessor.toTable(databaseName,
                    tableInput);
                HiveMetaStoreClientUtil.getHMSClient().createTable(hiveTable);
            });
    }

    @Override
    public TraverseCursorResult<List<TableCommit>> listTableCommits(TableName tableName, int maxResults,
        String pageToken) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "listTableCommits");
    }

    @Override
    public Table getTableByName(TableName tableName) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                org.apache.hadoop.hive.metastore.api.Table table = HiveMetaStoreClientUtil.getHMSClient().getTable(tableName.getCatalogName(),
                    tableName.getDatabaseName(), tableName.getTableName());
                return PolyCatDataAccessor.toTable(table);
            });
    }

    @Override
    public TableCommit getLatestTableCommit(TableName tableName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getLastestTable");
    }

    @Override
    public TraverseCursorResult<List<Table>> listTable(DatabaseName databaseName, Boolean includeDeleted,
        Integer maxResults, String pageToken, String filter) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "listTable");
    }

    @Override
    public PagedList<String> listTableNames(DatabaseName databaseFullName, Boolean includeDrop, Integer maxResults, String pageToken, String filter) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "listTableNames");
    }


    @Override
    public TraverseCursorResult<List<String>> getTableNames(DatabaseName databaseName, String tableType,
        Integer maxResults, String pageToken, String filter) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                List<String> tableNames;
                if (tableType != null) {
                    tableNames = HiveMetaStoreClientUtil.getHMSClient().getTables(databaseName.getCatalogName(), databaseName.getDatabaseName(), filter,
                        TableType.valueOf(tableType));
                } else {
                    tableNames = HiveMetaStoreClientUtil.getHMSClient().getTables(databaseName.getCatalogName(), databaseName.getDatabaseName(),
                        filter);
                }
                return new TraverseCursorResult<>(tableNames, null);
            });
    }

    @Override
    public List<Table> getTableObjectsByName(DatabaseName databaseName, List<String> tableNames) {
        return HiveServiceHelper.HiveExceptionHandler(() ->
            HiveMetaStoreClientUtil.getHMSClient().getTableObjectsByName(databaseName.getCatalogName(), databaseName.getDatabaseName(), tableNames)
                .stream().map(PolyCatDataAccessor::toTable).collect(Collectors.toList()));
    }

    @Override
    public TraverseCursorResult<List<String>> listTableNamesByFilter(DatabaseName databaseName, String filter,
        Integer maxNum, String pageToken) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> new TraverseCursorResult<>(
                HiveMetaStoreClientUtil.getHMSClient().listTableNamesByFilter(databaseName.getCatalogName(), databaseName.getDatabaseName(), filter,
                    maxNum), null));
    }

    @Override
    public void dropTable(TableName tableName, boolean deleteData, boolean ignoreUnknownTable, boolean ifPurge) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().dropTable(tableName.getCatalogName(), tableName.getDatabaseName(), tableName.getTableName(),
                deleteData, ignoreUnknownTable, ifPurge));
    }

    private EnvironmentContext convertToEnvironmentContext(Map<String, String> envProperties) {
        if (envProperties != null) {
            return new EnvironmentContext(envProperties);
        }
        return null;
    }

    @Override
    public void undropTable(TableName tableName, String tableId, String newName) {

    }

    @Override
    public void purgeTable(TableName tableName, String tableId) {

    }

    @Override
    public void restoreTable(TableName tableName, String version) {

    }

    @Override
    public void alterTable(TableName tableName, TableInput tableInput,
        Map<String, String> alterTableParams) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> {
                org.apache.hadoop.hive.metastore.api.Table table = HiveDataAccessor.toTable(tableName, tableInput);
                HiveMetaStoreClientUtil.getHMSClient().alter_table(tableName.getCatalogName(), tableName.getDatabaseName(), tableName.getTableName(),
                    table, convertToEnvironmentContext(alterTableParams));
            });
    }

    protected void dropTablePurge(TableName tableName, boolean ignoreUnknownTable) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> {
                HiveMetaStoreClientUtil.getHMSClient().dropTable(tableName.getCatalogName(), tableName.getDatabaseName(), tableName.getTableName(),
                    true, ignoreUnknownTable, true);
                return null;
            });
    }

    @Override
    public void truncateTable(TableName tableName) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().truncateTable(tableName.getCatalogName(), tableName.getDatabaseName(),
                tableName.getTableName(), null));
    }

    @Override
    public void alterColumn(TableName tableNameParam, ColumnChangeInput columnChangeInput) {

    }

    @Override
    public void setProperties(TableName tableName, Map<String, String> properties) {

    }

    @Override
    public void unsetProperties(TableName tableName, List<String> propertyKeys) {

    }


    @Override
    public ColumnStatisticsObj[] getTableColumnStatistics(TableName tableName, List<String> colNames) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().getTableColumnStatistics(tableName.getCatalogName(), tableName.getDatabaseName(),
                    tableName.getTableName(), colNames).stream()
                .map(PolyCatDataAccessor::toColumnStatisticsObj)
                .toArray(ColumnStatisticsObj[]::new));
    }

    @Override
    public boolean updateTableColumnStatistics(String projectId, ColumnStatistics stats) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().updateTableColumnStatistics(HiveDataAccessor.toColumnStatistics(stats)));
    }

    @Override
    public boolean deleteTableColumnStatistics(TableName tableName, String colName) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().deleteTableColumnStatistics(tableName.getCatalogName(), tableName.getDatabaseName(),
                tableName.getTableName(), colName));
    }

    // constraints
    @Override
    public void addConstraint(String projectId, List<Constraint> constraints) {
        addMap.get(constraints.get(0).getCstr_type()).accept(constraints);
    }

    @Override
    public void addForeignKey(String projectId, List<ForeignKey> foreignKeys) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().addForeignKey(HiveDataAccessor.toForeignKeys(foreignKeys)));
    }

    @Override
    public void addPrimaryKey(String projectId, List<PrimaryKey> primaryKeys) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().addPrimaryKey(HiveDataAccessor.toPrimaryKeys(primaryKeys)));
    }

    @Override
    public void dropConstraint(TableName tableName, String cstr_name) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().dropConstraint(tableName.getCatalogName(), tableName.getDatabaseName(), tableName.getTableName(),
                cstr_name));
    }

    @Override
    public List<Constraint> getConstraints(TableName tableName, ConstraintType type) {
        return getMap.get(type).apply(tableName);
    }

    @Override
    public List<ForeignKey> getForeignKeys(TableName parentTableName,
        TableName foreignTableName) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> PolyCatDataAccessor.toForeignKeys(HiveMetaStoreClientUtil.getHMSClient().getForeignKeys(
                HiveDataAccessor.toForeignKeyRequest(parentTableName, foreignTableName))));
    }

    @Override
    public List<PrimaryKey> getPrimaryKeys(TableName tableName) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> PolyCatDataAccessor.toPrimaryKeys(HiveMetaStoreClientUtil.getHMSClient().getPrimaryKeys(
                HiveDataAccessor.toPrimaryKeyReq(tableName.getCatalogName(),
                    tableName.getDatabaseName(),
                    tableName.getTableName()))));
    }

    private void addCheckConstraintToHive(List<Constraint> constraints) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().addCheckConstraint(HiveDataAccessor.toCheckConstraint(constraints)));
    }

    private void addDefaultConstraintToHive(List<Constraint> constraints) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().addDefaultConstraint(HiveDataAccessor.toDefaultConstraint(constraints)));
    }

    private void addNotNullConstraintToHive(List<Constraint> constraints) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().addNotNullConstraint(HiveDataAccessor.toNotNullConstraint(constraints)));
    }

    private void addUniqueConstraintToHive(List<Constraint> constraints) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().addUniqueConstraint(HiveDataAccessor.toUniqueConstraint(constraints)));
    }

    private List<Constraint> getUniqueConstraintFromHive(TableName tableName) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> PolyCatDataAccessor.toUniqueConstraint(
                HiveMetaStoreClientUtil.getHMSClient().getUniqueConstraints(HiveDataAccessor.toUniqueCtrReq(tableName))));
    }

    private List<Constraint> getDefaultConstraintFromHive(TableName tableName) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> PolyCatDataAccessor.toDefaultConstraint(
                HiveMetaStoreClientUtil.getHMSClient().getDefaultConstraints(HiveDataAccessor.toDefaultCtrReq(tableName))));
    }

    private List<Constraint> getNotNullConstraintFromHive(TableName tableName) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> PolyCatDataAccessor.toNotNullConstraint(
                HiveMetaStoreClientUtil.getHMSClient().getNotNullConstraints(HiveDataAccessor.toNotNullCtrReq(tableName))));
    }

    private List<Constraint> getCheckConstraintFromHive(TableName tableName) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> PolyCatDataAccessor.toCheckConstraint(
                HiveMetaStoreClientUtil.getHMSClient().getCheckConstraints(HiveDataAccessor.toCheckCtrReq(tableName))));
    }
    // end constraints
}
