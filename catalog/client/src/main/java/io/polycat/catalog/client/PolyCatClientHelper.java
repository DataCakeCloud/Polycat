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
package io.polycat.catalog.client;

import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.constants.CompatibleHiveConstants;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.exception.ExceptionUtil;
import io.polycat.catalog.common.exception.InvalidOperationException;
import io.polycat.catalog.common.exception.NoSuchObjectException;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.PartitionAlterContext;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.base.PartitionInput;
import io.polycat.catalog.common.plugin.request.AddPartitionRequest;
import io.polycat.catalog.common.plugin.request.AlterColumnRequest;
import io.polycat.catalog.common.plugin.request.AlterDatabaseRequest;
import io.polycat.catalog.common.plugin.request.AlterPartitionRequest;
import io.polycat.catalog.common.plugin.request.AlterTableRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.CreateTableRequest;
import io.polycat.catalog.common.plugin.request.DeleteDatabaseRequest;
import io.polycat.catalog.common.plugin.request.DeleteTableRequest;
import io.polycat.catalog.common.plugin.request.DoesPartitionExistsRequest;
import io.polycat.catalog.common.plugin.request.DropPartitionRequest;
import io.polycat.catalog.common.plugin.request.GetCatalogRequest;
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest;
import io.polycat.catalog.common.plugin.request.GetPartitionRequest;
import io.polycat.catalog.common.plugin.request.GetTablePartitionsByNamesRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.ListDatabasesRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionByFilterRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionsWithAuthRequest;
import io.polycat.catalog.common.plugin.request.ListTablePartitionsRequest;
import io.polycat.catalog.common.plugin.request.input.AddPartitionInput;
import io.polycat.catalog.common.plugin.request.input.AlterPartitionInput;
import io.polycat.catalog.common.plugin.request.input.AlterTableInput;
import io.polycat.catalog.common.plugin.request.input.ColumnChangeInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionsWithAuthInput;
import io.polycat.catalog.common.plugin.request.input.PartitionFilterInput;
import io.polycat.catalog.common.plugin.request.input.PartitionValuesInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.plugin.request.input.TableTypeInput;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.FileUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author liangyouze
 * @date 2024/2/8
 */
public class PolyCatClientHelper {

    public static void alterTable(Client client, Table table, Map<String, String> parameters, StorageDescriptor sd) {
        alterTableColumn(client, table, sd.getColumns());
        final TableInput tableInput = toTableInput(table);
        final AlterTableInput alterTableInput = new AlterTableInput(tableInput, parameters);
        final AlterTableRequest alterTableRequest = new AlterTableRequest();
        alterTableRequest.setCatalogName(table.getCatalogName());
        alterTableRequest.setDatabaseName(table.getDatabaseName());
        alterTableRequest.setTableName(table.getTableName());
        alterTableRequest.setProjectId(client.getProjectId());
        alterTableRequest.setInput(alterTableInput);
        client.alterTable(alterTableRequest);
    }

    public static void alterTableColumn(Client client, Table oldTable, List<Column> newColumns) {
        final List<Column> oldColumns = oldTable.getStorageDescriptor().getColumns();
        if (oldColumns.size() < newColumns.size()) {
            final List<Column> addColumns = new ArrayList<>();
            for (int i = oldColumns.size(); i < newColumns.size(); i++) {
                addColumns.add(newColumns.get(i));
            }
            addTableColumns(client, oldTable, addColumns);
        } else if (oldColumns.size() == newColumns.size()) {
            Map<String, String> renameColumns = new HashMap<>();
            Map<String, Column> changeColumns = new HashMap<>();
            for (int i = 0; i < oldColumns.size(); i++) {
                final Column newColumn = newColumns.get(i);
                final Column oldColumn = oldColumns.get(i);
                if (!newColumn.getColumnName().equals(oldColumn.getColumnName())) {
                    renameColumns.put(oldColumn.getColumnName(), newColumn.getColumnName());
                }
                if (newColumn.getComment() != null &&
                        !newColumn.getComment().equals(oldColumn.getComment()) ||
                        !newColumn.getColType().equals(oldColumn.getColType())) {
                    changeColumns.put(newColumn.getColumnName(), newColumn);
                }
            }
            if (!renameColumns.isEmpty() && !changeColumns.isEmpty()) {
                throw new InvalidOperationException("Cannot modify column names and attributes at the same time");
            }
            if (!renameColumns.isEmpty()) {
                renameTableColumns(client, oldTable, renameColumns);
            }
            if (!changeColumns.isEmpty()) {
                changeTableColumns(client, oldTable, changeColumns);
            }
        } else if (oldColumns.size() > newColumns.size()) {
            final List<String> deleteColumns = new ArrayList<>();
            for (int i = newColumns.size(); i < oldColumns.size(); i++) {
                deleteColumns.add(oldColumns.get(i).getColumnName());
            }
            deleteTableColumns(client, oldTable, deleteColumns);
        }
    }

    public static Table createTable(Client client, TableName tableName, Map<String, String> parameters, StorageDescriptor sd, List<Column> partitions, String comment) {
        final TableInput tableInput = newTableInput(tableName, client.getUserName(), parameters, sd, partitions, comment);
        final CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setCatalogName(tableName.getCatalogName());
        createTableRequest.setProjectId(client.getProjectId());
        createTableRequest.setDatabaseName(tableName.getDatabaseName());
        createTableRequest.setInput(tableInput);
        try {
            return client.createTable(createTableRequest);
        } catch (CatalogException e) {
            ExceptionUtil.throwAlreadyExistsException(e);
            throw e;
        }
    }

    private static TableInput newTableInput(TableName tableName, String owner, Map<String, String> parameters, StorageDescriptor sd, List<Column> partitions, String comment) {
        final long currentTimeMillis = System.currentTimeMillis();
        final TableInput tableInput = new TableInput();
        tableInput.setTableName(tableName.getTableName());
        tableInput.setDatabaseName(tableName.getDatabaseName());
        tableInput.setCatalogName(tableName.getCatalogName());
        tableInput.setOwner(owner);
        tableInput.setCreateTime(currentTimeMillis / 1000);
        tableInput.setLastAccessTime(currentTimeMillis / 1000);
        tableInput.setPartitionKeys(Collections.emptyList());
        tableInput.setRetention(Integer.MAX_VALUE);
        tableInput.setViewOriginalText(null);
        tableInput.setViewExpandedText(null);
        tableInput.setTableType(TableTypeInput.EXTERNAL_TABLE.toString());
        tableInput.setPartitionKeys(partitions);
        if (sd != null) {
            tableInput.setStorageDescriptor(sd);
        }
        tableInput.setParameters(adaptCommonParameters(parameters));
        tableInput.setDescription(comment);
        return tableInput;
    }

    public static Map<String, String> adaptCommonParameters(Map<String, String> parameters) {
        if (parameters == null) {
            parameters = new HashMap<String, String>();
        }
        parameters.put(CompatibleHiveConstants.COMMON_PARAM_TRANSIENT_LAST_DDL_TIME,
            String.valueOf(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())));
        return parameters;
    }

    public static Table getTable(Client client, TableName tableName) {
        return getTable(client, tableName.getCatalogName(), tableName.getDatabaseName(), tableName.getTableName());
    }

    public static Table getTable(Client client, String catalogName, String databaseName, String tableName) throws NoSuchObjectException {
        final GetTableRequest getTableRequest = new GetTableRequest(client.getProjectId(), catalogName, databaseName, tableName);
        try {
            return client.getTable(getTableRequest);
        } catch (CatalogException e) {
            ExceptionUtil.throwNoSuchObjectException(e);
            throw e;
        }
    }

    public static boolean doesPartitionExist(Client client, String catName, String dbName, String tableName,
                                             List<String> partVals) {
        try {
            final PartitionValuesInput partitionValuesInput = new PartitionValuesInput();
            partitionValuesInput.setPartitionValues(partVals);
            final DoesPartitionExistsRequest doesPartitionExistsRequest = new DoesPartitionExistsRequest(client.getProjectId(),
                    catName, dbName, tableName, partitionValuesInput);
            return client.doesPartitionExist(doesPartitionExistsRequest);
        } catch (Exception e) {
            return false;
        }
    }

    public static String makePartitionName(List<String> partitionKeys, List<String> partValues) {
        if (partitionKeys.size() != partValues.size()) {
            throw new IllegalArgumentException("The number of fields (" + partitionKeys.size() +
                    ") in the partition identifier is not equal to the partition schema length (" +
                    partValues.size() + "). The identifier might not refer to one partition.");
        }
        StringBuilder partitionFolder = new StringBuilder();
        for (int i = 0; i < partitionKeys.size(); i++) {
            partitionFolder.append(partitionKeys.get(i))
                    .append("=")
                    .append(FileUtils.escapePathName(partValues.get(i)))
                    .append("/");
        }
        return partitionFolder.substring(0, partitionFolder.length() - 1);
    }

    public static boolean createPartition(Client client, String catalogName, String dbName, String tableName,
                                          PartitionInput partitionInput) {
        return createPartitions(client, catalogName, dbName, tableName, Collections.singletonList(partitionInput));
    }

    public static boolean createPartitions(Client client, String catalogName, String dbName, String tableName,
                                           List<PartitionInput> partitionInputs) {
        AddPartitionInput addPartitionInput = new AddPartitionInput();
        addPartitionInput.setPartitions(partitionInputs.toArray(new PartitionInput[0]));
        AddPartitionRequest request = new AddPartitionRequest(client.getProjectId(), catalogName,
                dbName, tableName, addPartitionInput);
        client.addPartition(request);
        return true;
    }

    public static <V> Map<String, V> cleanMemoryParams(Map<String, V> properties, List<String> removeKeys) {
        if (MapUtils.isEmpty(properties) || CollectionUtils.isEmpty(removeKeys)) {
            return properties;
        }
        properties = new LinkedHashMap<>(properties);
        for (String key: removeKeys) {
            properties.remove(key);
        }
        return properties;
    }

    public static Partition getPartition(Client client, String catalogName, String dbName, String tableName,
                                         List<String> partitionKeys, List<String> partValues) {
        GetPartitionRequest request = new GetPartitionRequest(client.getProjectId(), catalogName, dbName, tableName,
                makePartitionName(partitionKeys, partValues));
        return client.getPartition(request);
    }

    public static List<Partition> getPartitionsByFilter(Client client, TableName tableName, String filter) {
        final PartitionFilterInput partitionFilterInput = new PartitionFilterInput();
        partitionFilterInput.setFilter(filter);
        ListPartitionByFilterRequest listPartitionByFilterRequest = new ListPartitionByFilterRequest(
                client.getProjectId(), tableName.getCatalogName(), tableName.getDatabaseName(), tableName.getTableName(),
                partitionFilterInput);
        return client.getPartitionsByFilter(listPartitionByFilterRequest);
    }

    public static boolean alterPartition(Client client, String catalogName, String dbName, String tableName,
                                         Partition newPart, List<String> oldPartValues) {
        AlterPartitionInput input = new AlterPartitionInput();
        List<PartitionAlterContext> partitionContexts = new ArrayList<>();
        PartitionAlterContext context = convertToContext(newPart, oldPartValues);
        partitionContexts.add(context);
        input.setPartitionContexts(partitionContexts.toArray(new PartitionAlterContext[0]));
        AlterPartitionRequest request = new AlterPartitionRequest(client.getProjectId(), catalogName,
                dbName, tableName, input);
        client.alterPartitions(request);
        return true;
    }

    private static PartitionAlterContext convertToContext(Partition partition,
                                                   List<String> oldPartValues) {
        PartitionAlterContext context = new PartitionAlterContext();
        context.setOldValues(oldPartValues);
        context.setNewValues(partition.getPartitionValues());
        context.setParameters(partition.getParameters());
        context.setInputFormat(partition.getStorageDescriptor().getInputFormat());
        context.setOutputFormat(partition.getStorageDescriptor().getOutputFormat());
        context.setLocation(partition.getStorageDescriptor().getLocation());
        return context;
    }

    public static boolean dropPartition(Client client, String catalogName, String dbName, String tableName, List<String> partitionKeys, List<String> partValues) {
        List<String> partNames = new ArrayList<>();
        partNames.add(makePartitionName(partitionKeys, partValues));
        return dropPartitions(client, catalogName, dbName, tableName, partNames);
    }

    public static boolean dropPartitions(Client client, String catalogName, String dbName, String tableName, List<String> partNames) {
        DropPartitionInput dropPartitionInput = new DropPartitionInput();
        dropPartitionInput.setPartitionNames(partNames);
        DropPartitionRequest request = new DropPartitionRequest(client.getProjectId(), catalogName,
            dbName, tableName, dropPartitionInput);
        client.dropPartition(request);
        return true;
    }

    public static List<String> listPartitionNames(Client client, String catalogName, String dbName, String tableName, PartitionFilterInput input) {
        if (input == null) {
            throw new NullPointerException("Table listPartitionNames input is null.");
        }
        ListTablePartitionsRequest request = new ListTablePartitionsRequest(client.getProjectId(), catalogName,
                dbName, tableName, input);
        return client.listPartitionNames(request);
    }

    public static List<Partition> listPartitionsWithValues(Client client, TableName tableName, List<String> values) {
        if (values == null) {
            throw new NullPointerException("values is null.");
        }

        final GetPartitionsWithAuthInput input = new GetPartitionsWithAuthInput();
        input.setValues(values);
        final ListPartitionsWithAuthRequest request = new ListPartitionsWithAuthRequest(client.getProjectId(), tableName.getCatalogName(),
                tableName.getDatabaseName(), tableName.getTableName(), input);
        return client.listPartitionsPsWithAuth(request);
    }

    public static List<Partition> getPartitionsByNames(Client client, TableName tableName, String[] partNames) {
        if (partNames == null) {
            throw new NullPointerException("Table listPartitionNames input is null.");
        }
        PartitionFilterInput filterInput = new PartitionFilterInput();
        filterInput.setPartNames(partNames);
        GetTablePartitionsByNamesRequest request = new GetTablePartitionsByNamesRequest(client.getProjectId(), tableName.getCatalogName(),
            tableName.getDatabaseName(), tableName.getTableName(), filterInput);
        return client.getPartitionsByNames(request);
    }

    public static Catalog getCatalog(Client client, String catalogName) {
        final GetCatalogRequest getCatalogRequest = new GetCatalogRequest(client.getProjectId(), catalogName);
        return client.getCatalog(getCatalogRequest);
    }

    public static Database createDatabase(Client client, String catalogName, String databaseName, String comment, String location, String owner,  Map<String, String> parameters) {
        final DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setCatalogName(catalogName);
        databaseInput.setDatabaseName(databaseName);
        databaseInput.setOwner(client.getUserName());
        databaseInput.setCreateTime(System.currentTimeMillis());
        databaseInput.setLocationUri(location);
        databaseInput.setDescription(comment);
        databaseInput.setParameters(parameters);
        final CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
        createDatabaseRequest.setInput(databaseInput);
        createDatabaseRequest.setCatalogName(catalogName);
        createDatabaseRequest.setProjectId(client.getProjectId());
        try {
            return client.createDatabase(createDatabaseRequest);
        } catch (CatalogException e) {
            ExceptionUtil.throwAlreadyExistsException(e);
            throw e;
        }

    }

    public static Database[] listDatabases(Client client, String catalogName) {
        final ListDatabasesRequest listDatabasesRequest = new ListDatabasesRequest(client.getProjectId(), catalogName);
        listDatabasesRequest.setMaxResults(Integer.MAX_VALUE);
        return client.listDatabases(listDatabasesRequest).getObjects();
    }

    public static Database getDatabase(Client client, String catalogName, String databaseName) {
        GetDatabaseRequest request = new GetDatabaseRequest();
        request.setCatalogName(catalogName);
        request.setDatabaseName(databaseName);
        request.setProjectId(client.getProjectId());
        try {
            return client.getDatabase(request);
        } catch (CatalogException e) {
            ExceptionUtil.throwNoSuchObjectException(e);
            throw e;
        }

    }

    public static void dropDatabase(Client client, String catalogName, String databaseName) {
        final DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest(client.getProjectId(), catalogName, databaseName);
        try {
            client.deleteDatabase(deleteDatabaseRequest);
        } catch (CatalogException e) {
            ExceptionUtil.throwNoSuchObjectException(e);
            ExceptionUtil.throwInvalidOperationException(e);
            throw e;
        }

    }

    public static void alterDatabase(Client client, Database database, String comment, String location, String owner, Map<String, String> parameters) {
        final DatabaseInput databaseInput = toDatabaseInput(database, comment, location, owner, parameters);
        final AlterDatabaseRequest alterDatabaseRequest = new AlterDatabaseRequest(client.getProjectId(), database.getCatalogName(), database.getDatabaseName(), databaseInput);
        client.alterDatabase(alterDatabaseRequest);
    }

    private static DatabaseInput toDatabaseInput(Database database, String comment, String location, String owner, Map<String, String> parameters) {
        final DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setCatalogName(database.getCatalogName());
        databaseInput.setDatabaseName(database.getDatabaseName());
        databaseInput.setOwner(database.getOwner());
        final Map<String, String> oldParameters = database.getParameters();
        databaseInput.setParameters(oldParameters);
        if (StringUtils.isNotEmpty(comment)) {
            databaseInput.setDescription(comment);
        } else {
            databaseInput.setDescription(database.getDescription());
        }
        if (StringUtils.isNotEmpty(location)) {
            databaseInput.setLocationUri(location);
        } else {
            databaseInput.setLocationUri(database.getLocationUri());
        }
        if (StringUtils.isNotEmpty(owner)) {
            databaseInput.setOwner(owner);
        } else {
            databaseInput.setOwner(database.getOwner());
        }
        if (parameters != null) {
            oldParameters.putAll(parameters);
        }
        return databaseInput;
    }

    public static void dropTable(Client client, String catalogName, String databaseName, String tableName) {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
        deleteTableRequest.setCatalogName(catalogName);
        deleteTableRequest.setDatabaseName(databaseName);
        deleteTableRequest.setTableName(tableName);
        deleteTableRequest.setProjectId(client.getProjectId());
        deleteTableRequest.setPurgeFlag(false);
        try {
            client.deleteTable(deleteTableRequest);
        } catch (CatalogException e) {
            ExceptionUtil.throwNoSuchObjectException(e);
            throw e;
        }
    }

    public static void renameTable(Client client, Table table, String newDatabase, String newTableName) {
        final TableInput tableInput = toTableInput(table);
        tableInput.setTableName(newTableName);
        tableInput.setDatabaseName(newDatabase);
        final AlterTableInput alterTableInput = new AlterTableInput();
        alterTableInput.setTable(tableInput);
        final AlterTableRequest alterTableRequest = new AlterTableRequest();
        alterTableRequest.setInput(alterTableInput);
        alterTableRequest.setTableName(table.getTableName());
        alterTableRequest.setDatabaseName(table.getDatabaseName());
        alterTableRequest.setCatalogName(table.getCatalogName());
        alterTableRequest.setProjectId(client.getProjectId());
        try {
            client.alterTable(alterTableRequest);
        } catch (CatalogException e) {
            ExceptionUtil.throwNoSuchObjectException(e);
            ExceptionUtil.throwAlreadyExistsException(e);
            throw e;
        }

    }

    private static TableInput toTableInput(Table table) {
        final TableInput tableInput = new TableInput();
        tableInput.setTableName(table.getTableName());
        tableInput.setDatabaseName(table.getDatabaseName());
        tableInput.setCatalogName(table.getCatalogName());
        tableInput.setTableType(table.getTableType());
        tableInput.setOwner(table.getOwner());
        tableInput.setStorageDescriptor(table.getStorageDescriptor());
        tableInput.setRetention(table.getRetention().intValue());
        tableInput.setParameters(table.getParameters());
        tableInput.setPartitionKeys(table.getPartitionKeys());
        tableInput.setViewOriginalText(table.getViewOriginalText());
        tableInput.setViewExpandedText(table.getViewExpandedText());
        final long currentTimeMillis = System.currentTimeMillis();
        tableInput.setLastAccessTime(currentTimeMillis);
        tableInput.setUpdateTime(currentTimeMillis);
        tableInput.setDescription(table.getDescription());
        return tableInput;
    }

    public static void addTableColumns(Client client, Table table, List<Column> addColumns) {
        final ColumnChangeInput columnChangeInput = new ColumnChangeInput();
        columnChangeInput.setChangeType(Operation.ADD_COLUMN);
        columnChangeInput.setColumnList(addColumns);
        final AlterColumnRequest alterColumnRequest = new AlterColumnRequest(client.getProjectId(), table.getCatalogName(), table.getDatabaseName(), table.getTableName(), columnChangeInput);
        client.alterColumn(alterColumnRequest);
    }

    public static void renameTableColumns(Client client, Table table, Map<String, String> renameColumns) {
        final ColumnChangeInput columnChangeInput = new ColumnChangeInput();
        columnChangeInput.setChangeType(Operation.RENAME_COLUMN);
        columnChangeInput.setRenameColumnMap(renameColumns);
        final AlterColumnRequest alterColumnRequest = new AlterColumnRequest(client.getProjectId(), table.getCatalogName(), table.getDatabaseName(), table.getTableName(), columnChangeInput);
        client.alterColumn(alterColumnRequest);
    }

    public static void changeTableColumns(Client client, Table table, Map<String, Column> changeColumns) {
        final ColumnChangeInput columnChangeInput = new ColumnChangeInput();
        columnChangeInput.setChangeType(Operation.CHANGE_COLUMN);
        columnChangeInput.setChangeColumnMap(changeColumns);
        final AlterColumnRequest alterColumnRequest = new AlterColumnRequest(client.getProjectId(), table.getCatalogName(), table.getDatabaseName(), table.getTableName(), columnChangeInput);
        client.alterColumn(alterColumnRequest);
    }

    public static void deleteTableColumns(Client client, Table table, List<String> deleteColumns) {
        final ColumnChangeInput columnChangeInput = new ColumnChangeInput();
        columnChangeInput.setChangeType(Operation.DROP_COLUMN);
        columnChangeInput.setDropColumnList(deleteColumns);
        final AlterColumnRequest alterColumnRequest = new AlterColumnRequest(client.getProjectId(), table.getCatalogName(), table.getDatabaseName(), table.getTableName(), columnChangeInput);
        client.alterColumn(alterColumnRequest);
    }

    public static void alterTable(Client client, Table oldTable, Map<String, String> parameters, String location, String comment, String owner) {
        final TableInput tableInput = toTableInput(oldTable);
        tableInput.getParameters().putAll(adaptCommonParameters(parameters));
        if (StringUtils.isNotEmpty(location)) {
            tableInput.getStorageDescriptor().setLocation(location);
        }
        if (StringUtils.isNotEmpty(comment)) {
            tableInput.setDescription(comment);
        }
        if (StringUtils.isNotEmpty(owner)) {
            tableInput.setOwner(owner);
        }
        final AlterTableInput alterTableInput = new AlterTableInput(tableInput, parameters);
        final AlterTableRequest alterTableRequest = new AlterTableRequest();
        alterTableRequest.setInput(alterTableInput);
        client.alterTable(alterTableRequest);
    }
}
