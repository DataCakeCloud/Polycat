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
package io.polycat.catalog.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.Share;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.plugin.CatalogContext;
import io.polycat.catalog.common.plugin.request.CreateTableRequest;
import io.polycat.catalog.common.plugin.request.DeleteTableRequest;
import io.polycat.catalog.common.plugin.request.GetShareRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.ListTablesRequest;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.model.Column;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SparkHelper;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.execution.datasources.v2.carbon.CarbonFileDataSourceV2;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;

/**
 * A TableCatalog implementation that interact with PolyCat catalog server
 */
public class SparkCatalog implements TableCatalog, SupportsNamespaces {

    private String name;

    private PolyCatClient client;

    private SQLConf sqlConf;

    public SparkCatalog() {
    }

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.name = name;
        sqlConf = new SQLConf();
        sqlConf.setConfString("spark.sql.sources.useV1SourceList", "carbondata");
        initClientIfNeeded(options);
    }

    private void initClientIfNeeded(CaseInsensitiveStringMap options) {
        String userName = options.get("username");
        if (StringUtils.isNotBlank(userName)) {
            String projectId = options.get("projectid");
            String tenant = options.get("tenant");
            String token = options.get("token");
            client = PolyCatClient.getInstance(getConfiguration(), false);
            client.setContext(new CatalogContext(projectId, userName, tenant, token));
        }
    }

    private Configuration getConfiguration() {
        Option<SparkSession> activeSession = SparkSession.getActiveSession();
        if (activeSession.isDefined()) {
            return activeSession.get().sessionState().newHadoopConf();
        } else {
            return new Configuration();
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        if (namespace == null || namespace.length != 1) {
            throw new NoSuchNamespaceException(namespace);
        }
        ListTablesRequest listTablesRequest = new ListTablesRequest();
        listTablesRequest.setCatalogName(this.name);
        listTablesRequest.setDatabaseName(namespace[0].toLowerCase());
        listTablesRequest.setProjectId(getClient().getProjectId());
        String pageToken = null;
        List<Identifier> listIdentifiers = new LinkedList<>();
        do {
            if (pageToken != null) {
                listTablesRequest.setNextToken(pageToken);
            }
            PagedList<io.polycat.catalog.common.model.Table> tablePagedList = getClient().listTables(listTablesRequest);
            pageToken = tablePagedList.getNextMarker();
            for (io.polycat.catalog.common.model.Table table : tablePagedList.getObjects()) {
                listIdentifiers.add(SparkHelper.createIdentifier(namespace, table.getTableName()));
            }
        } while (pageToken != null);
        return listIdentifiers.toArray(new Identifier[0]);
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        String[] namespace = ident.namespace();
        if (namespace == null || (namespace.length != 1 && namespace.length != 3)) {
            throw new NoSuchTableException(ident);
        }
        String projectId = namespace.length == 1 ? getClient().getProjectId() : namespace[0];
        GetTableRequest getTableRequest = buildGetTableRequest(ident);
        io.polycat.catalog.common.model.Table table = getLmsTable(getTableRequest);
        if (table == null) {
            return null;
        }
        return convertToSparkTable(projectId, table, namespace.length == 3 ? namespace[1] : null);
    }

    private Table convertToSparkTable(String projectId, io.polycat.catalog.common.model.Table table, String shareName) {
        Map<String, String> properties = new HashMap<>(table.getParameters());
        properties.put("path", table.getStorageDescriptor().getLocation());
        properties.put("catalog_name", table.getCatalogName());
        properties.put("database_name", table.getDatabaseName());
        properties.put("table_name", table.getTableName());
        String sourceShortName = table.getStorageDescriptor().getSourceShortName();
        // TODO remove this if code block in the future
        if ("carbon".equalsIgnoreCase(sourceShortName)) {
            Table carbonTable = new CarbonFileDataSourceV2().getTable(
                convertSchema(table.getFields(), table.getPartitionKeys()),
                buildPartitioning(table.getPartitionKeys()), properties);
            SparkHelper.setPartitionInfo(getClient(), projectId, table, carbonTable, shareName);
            return carbonTable;
        }
        Option<TableProvider> providerOption = DataSource.lookupDataSourceV2(sourceShortName, sqlConf);
        if (providerOption.isEmpty()) {
            throw new CatalogException("Spark DataSource not found for source: " + sourceShortName);
        }
        Table sparkTable = providerOption.get().getTable(
            convertSchema(table.getFields(), table.getPartitionKeys()),
            buildPartitioning(table.getPartitionKeys()), properties);
        SparkHelper.setPartitionInfo(getClient(), projectId, table, sparkTable, shareName);
        return sparkTable;
    }

    private io.polycat.catalog.common.model.Table getLmsTable(GetTableRequest getTableRequest) {
        io.polycat.catalog.common.model.Table table;
        try {
            getTableRequest.initContext(getClient().getContext());
            table = getClient().getTable(getTableRequest);
        } catch (CatalogException catalogException) {
            if (catalogException.getStatusCode() == 404) {
                return null;
            }
            throw catalogException;
        }
        return table;
    }

    private GetTableRequest buildGetTableRequest(Identifier ident) {
        String[] namespace = ident.namespace();
        // table
        GetTableRequest getTableRequest = null;
        if (namespace.length == 1) {
            getTableRequest = new GetTableRequest(getClient().getProjectId(),
                name, ident.namespace()[0].toLowerCase(), ident.name().toLowerCase());
        }
        // share
        if (namespace.length == 3) {
            GetShareRequest request = new GetShareRequest();
            request.setProjectId(namespace[0]);
            request.setShareName(namespace[1]);
            Share share = getClient().getShare(request);
            getTableRequest = new GetTableRequest(namespace[0],
                share.getCatalogName(), ident.namespace()[2].toLowerCase(), ident.name().toLowerCase()
            );
            getTableRequest.setShareName(namespace[1]);
        }
        return getTableRequest;
    }

    private Transform[] buildPartitioning(List<Column> partitionColumns) {
        if (partitionColumns == null || partitionColumns.isEmpty()) {
            return new Transform[0];
        }
        return partitionColumns.stream()
            .map(field -> SparkHelper.createTransform(field.getColumnName()))
            .toArray(Transform[]::new);
    }

    private StructType convertSchema(List<Column> columns, List<Column> partitionColumns) {
        int columnNum = columns.size();
        int partitionColumnNum = 0;
        if (partitionColumns != null && partitionColumns != null) {
            partitionColumnNum = partitionColumns.size();
        }
        StructField[] structFields = new StructField[columnNum + partitionColumnNum];
        int i = 0;
        for (Column schemaField : columns) {
            structFields[i++] = SparkHelper.convertField(schemaField);
        }
        if (partitionColumnNum > 0) {
            for (Column schemaField : partitionColumns) {
                structFields[i++] = SparkHelper.convertField(schemaField);
            }
        }
        return new StructType(structFields);
    }

    @Override
    public Table createTable(Identifier ident, StructType schema,
        Transform[] partitions, Map<String, String> properties)
        throws TableAlreadyExistsException, NoSuchNamespaceException {
        TableInput input = buildTableInput(ident, schema, properties, partitions);
        CreateTableRequest request = new CreateTableRequest();
        request.setInput(input);
        request.setCatalogName(name);
        request.setDatabaseName(ident.namespace()[0]);
        request.setProjectId(getClient().getProjectId());
        getClient().createTable(request);
        GetTableRequest getTableRequest = buildGetTableRequest(ident);
        io.polycat.catalog.common.model.Table table = getLmsTable(getTableRequest);
        createPathIfNeeded(table.getStorageDescriptor().getLocation());
        return convertToSparkTable(getClient().getProjectId(), table, null);
    }

    private void createPathIfNeeded(String location) {
        Path path = new Path(location);
        try {
            path.getFileSystem(new Configuration()).mkdirs(path);
        } catch (IOException e) {
            throw new CatalogException("Failed to create table path");
        }
    }

    private TableInput buildTableInput(Identifier ident, StructType schema, Map<String, String> properties,
        Transform[] partitions) {
        TableInput tableInput = new TableInput();
        StructField[] fields = schema.fields();
        Set<String> partitionColumnNames = Arrays.stream(partitions)
            .map(Object::toString)
            .collect(Collectors.toSet());
        List<Column> columnInputs = new ArrayList<>(fields.length);
        List<Column> partitionColumnInputs = new ArrayList<>(fields.length);
        for (StructField field : fields) {
            Column columnInput = new Column(field.name(), field.dataType().simpleString());
            if (partitionColumnNames.contains(field.name())) {
                partitionColumnInputs.add(columnInput);
            } else {
                columnInputs.add(columnInput);
            }
        }
        tableInput.setPartitionKeys(partitionColumnInputs);
        tableInput.setTableName(ident.name().toLowerCase());
        tableInput.setParameters(properties);
        tableInput.setTableType("EXTERNAL_TABLE");
        StorageDescriptor storageInput = new StorageDescriptor();
        String fileFormat = properties.get("provider");
        if (fileFormat == null) {
            fileFormat = properties.get("hive.stored-as");
        }
        if (fileFormat != null) {
            storageInput.setSourceShortName(fileFormat);
            storageInput.setFileFormat(fileFormat);
        }
        storageInput.setColumns(columnInputs);
        tableInput.setStorageDescriptor(storageInput);
        tableInput.setOwner(getClient().getUserName());
        return tableInput;
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        throw new UnsupportedOperationException("alterTable");
    }

    @Override
    public boolean dropTable(Identifier ident) {
        //throw new UnsupportedOperationException("dropTable");
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
        deleteTableRequest.setCatalogName(name);
        deleteTableRequest.setDatabaseName(ident.namespace()[0]);
        deleteTableRequest.setTableName(ident.name());
        deleteTableRequest.setProjectId(getClient().getProjectId());
        getClient().deleteTable(deleteTableRequest);
        return true;
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
        throws NoSuchTableException, TableAlreadyExistsException {
        throw new UnsupportedOperationException("renameTable");
    }

    @Override
    public String[][] listNamespaces() throws NoSuchNamespaceException {
        return listNamespaces(new String[0]);
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        assert namespace.length == 0;
        return PolyCatClientHelper.listDatabases(getClient(), name);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
        assert namespace.length == 1;
        return PolyCatClientHelper.getDatabaseProperties(name, namespace[0], getClient());
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
        throws NamespaceAlreadyExistsException {
        assert namespace.length == 1;
        PolyCatClientHelper.createDataBase(name, namespace[0], getClient());
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {

    }

    @Override
    public boolean dropNamespace(String[] namespace) throws NoSuchNamespaceException {
        return false;
    }

    public PolyCatClient getClient() {
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    client = PolyCatClient.getInstance(getConfiguration());
                }
            }
        }
        return client;
    }
}
