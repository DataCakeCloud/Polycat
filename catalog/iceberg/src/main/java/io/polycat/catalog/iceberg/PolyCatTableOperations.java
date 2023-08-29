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
package io.polycat.catalog.iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableOperationType;
import io.polycat.catalog.common.plugin.request.CreateTableRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.SetTablePropertyRequest;
import io.polycat.catalog.common.plugin.request.input.SetTablePropertyInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.model.Column;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

public class PolyCatTableOperations extends BaseMetastoreTableOperations {

    private final PolyCatClient client;
    private final FileIO fileIO;
    private final String catalogName;
    private final String databaseName;
    private final String tableName;

  public PolyCatTableOperations(PolyCatClient client, FileIO fileIO, String catalogName, String databaseName,
      String tableName) {
        this.client = client;
        this.fileIO = fileIO;
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    protected String tableName() {
        return catalogName + "." + databaseName + "." + tableName;
    }

    @Override
    public FileIO io() {
        return fileIO;
    }

    @Override
    protected void doRefresh() {
        Table table = getLmsTable();
        String metadataLocation = null;
        if (table != null) {
            metadataLocation = table.getParameters().get("metadata_location");
        } else if (this.currentMetadataLocation() != null) {
            throw new NoSuchTableException("Not found table %s.%s.%s", catalogName, databaseName,
                tableName);
        }
        refreshFromMetadataLocation(metadataLocation);
    }

    @Override
    protected void doCommit(TableMetadata base, TableMetadata metadata) {
        String newMetadataLocation = this.writeNewMetadata(metadata, this.currentVersion() + 1);
        Map<String, String> properties = prepareProperties(newMetadataLocation, metadata.properties());
        Table lmsTable = getLmsTable();
        if (lmsTable == null) {
            TableInput input = buildTableInput(metadata);
            input.setParameters(properties);
            CreateTableRequest request = new CreateTableRequest();
            request.setInput(input);
            request.setCatalogName(catalogName);
            request.setDatabaseName(databaseName);
            request.setProjectId(client.getProjectId());
            client.createTable(request);
        } else {
            initOperation(properties, metadata);
            SetTablePropertyInput input = new SetTablePropertyInput();
            input.setSetProperties(properties);
            SetTablePropertyRequest request = new SetTablePropertyRequest();
            request.setInput(input);
            request.setProjectId(client.getProjectId());
            request.setCatalogName(lmsTable.getCatalogName());
            request.setDatabaseName(lmsTable.getDatabaseName());
            request.setTableName(lmsTable.getTableName());
            client.setTableProperty(request);
        }
    }

    private void initOperation(Map<String, String> properties, TableMetadata metadata) {
        if (metadata.currentSnapshot() == null) {
            return;
        }
        String operation = metadata.currentSnapshot().operation();
        if (StringUtils.isEmpty(operation)) {
            return;
        }
        String operationType = "";
        if (operation.equals(DataOperations.APPEND)) {
            operationType = TableOperationType.DML_INSERT.name();
        } else if (operation.equals(DataOperations.DELETE)) {
            operationType = TableOperationType.DML_UPDATE.name();
        } else if (operation.equals(DataOperations.OVERWRITE) || operation.equals(DataOperations.REPLACE)) {
            operationType = TableOperationType.DML_INSERT_OVERWRITE.name();
        }
        properties.put("polycat_operation", operationType);
    }

    private Map<String, String> prepareProperties(String newMetadataLocation, Map<String, String> tableProperties) {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("table_type", "iceberg".toUpperCase(Locale.ENGLISH));
        properties.put("metadata_location", newMetadataLocation);
        if (this.currentMetadataLocation() != null && !this.currentMetadataLocation().isEmpty()) {
            properties.put("previous_metadata_location", this.currentMetadataLocation());
        }
        properties.putAll(tableProperties);
        return properties;
    }

    private TableInput buildTableInput(TableMetadata metadata) {
        TableInput tableInput = new TableInput();
        List<Types.NestedField> columns = metadata.schema().columns();
        List<Column> columnInputs = new ArrayList<>(columns.size());
        for (Types.NestedField column : columns) {
          columnInputs.add(new Column(column.name(), column.type().toString()));
        }
        tableInput.setTableName(tableName);
        tableInput.setTableType("EXTERNAL_TABLE");
        tableInput.setParameters(metadata.properties());
        StorageDescriptor storageInput = new StorageDescriptor();
        storageInput.setLocation(metadata.location());
        storageInput.setSourceShortName("iceberg");
        storageInput.setColumns(columnInputs);
        tableInput.setStorageDescriptor(storageInput);
        tableInput.setOwner(client.getUserName());
        return tableInput;
    }

  private Table getLmsTable() {
        GetTableRequest request = new GetTableRequest(client.getProjectId(), catalogName, databaseName,
            tableName);
        try {
            return client.getTable(request);
        } catch (CatalogException catalogException) {
            if (catalogException.getStatusCode() == 404) {
                return null;
            } else {
                throw catalogException;
            }
        }
    }

}
