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

import io.polycat.catalog.CatalogRouter;
import io.polycat.catalog.common.model.SerDeInfo;
import io.polycat.catalog.common.options.PolyCatOptions;
import io.polycat.catalog.client.Client;
import io.polycat.catalog.client.PolyCatClientBuilder;
import io.polycat.catalog.client.PolyCatClientHelper;
import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.exception.NoSuchObjectException;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.base.PartitionInput;
import io.polycat.catalog.common.plugin.request.ListTablesRequest;
import io.polycat.catalog.common.plugin.request.input.PartitionFilterInput;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.spark.PathIdentifier;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.PolyCatPartitionTable;
import org.apache.spark.sql.connector.catalog.PolyCatStagedTable;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SparkHelper;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.internal.HiveSerDe;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class PolyCatClientSparkHelper {
    public static final Map<String, String> CATALOG_NAME_MAPPING = new HashMap<>();

    public static final String SPARK_POLYCAT_CATALOG_MAPPING = "spark.polycat.catalog.mapping";

    public static final String DEFAULT_DATABASE = "default";

    public final static List<String> DATABASE_PARAMS_REMOVE = Arrays.asList(SupportsNamespaces.PROP_OWNER, SupportsNamespaces.PROP_LOCATION, SupportsNamespaces.PROP_COMMENT);
    public final static List<String> TABLE_PARAMS_REMOVE = Arrays.asList(TableCatalog.PROP_LOCATION, TableCatalog.PROP_OWNER, TableCatalog.PROP_COMMENT);

    public final static List<String> PARTITION_PARAMS_REMOVE = Arrays.asList(Constants.LOCATION, Constants.PARTITION_OVERWRITE, Constants.OWNER_PARAM);

    public final static String TEXT_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
    public final static String TEXT_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";

    private static final HashMap<String, String> formatToSerializationLibMap = new HashMap() {
        {
            put("textfile", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
            put("text", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
            put("csv", "org.apache.hadoop.hive.serde2.OpenCSVSerde");
            put("json", "org.openx.data.jsonserde.JsonSerDe");

        }
    };

    public static volatile Client client;

    private static final SparkSession sparkSession = SparkSession.active();

    private static Configuration getConfiguration() {
        Option<SparkSession> activeSession = SparkSession.getActiveSession();
        if (activeSession.isDefined()) {
            return activeSession.get().sessionState().newHadoopConf();
        } else {
            return new Configuration();
        }
    }

    public static void initCatalogNameMapping(SparkConf conf) {
        String catalogNameMappingStr = conf.get(SPARK_POLYCAT_CATALOG_MAPPING, "");
        try {
            if (catalogNameMappingStr != null && !catalogNameMappingStr.isEmpty()) {
                String[] catalogs = catalogNameMappingStr.split(",");
                for (String catalog : catalogs) {
                    if (!catalog.isEmpty()) {
                        String[] kvMap = catalog.split(":");
                        if (kvMap.length != 2) {
                            throw new IllegalArgumentException("Param illegal: " + SPARK_POLYCAT_CATALOG_MAPPING + ", e.g: hive:alias1|alias2,hive1:alias3|alias4");
                        }
                        if (!kvMap[1].isEmpty()) {
                            String[] sparkCatalogNames = kvMap[1].split("\\|");
                            for (String sparkCatalogName : sparkCatalogNames) {
                                if (CATALOG_NAME_MAPPING.containsKey(sparkCatalogName)) {
                                    throw new IllegalArgumentException("Param illegal: " + SPARK_POLYCAT_CATALOG_MAPPING + ", The mapping relationship is duplicated: " + catalogNameMappingStr);
                                }
                                CATALOG_NAME_MAPPING.put(sparkCatalogName, kvMap[0]);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Param config error: " + SPARK_POLYCAT_CATALOG_MAPPING + " = " + catalogNameMappingStr, e);
        }
    }

    public static String getCatalogMappingName(String sparkCatalogName) {
        return CATALOG_NAME_MAPPING.getOrDefault(sparkCatalogName, sparkCatalogName);
    }

    public static Client getClient() {
        if (client == null) {
            synchronized (PolyCatClientSparkHelper.class) {
                if (client == null) {
                    Configuration conf = getConfiguration();
                    client = PolyCatClientBuilder.buildWithHadoopConf(conf);
                }
            }
        }
        return client;
    }

    public static Identifier[] listTables(Client client, String catalogName, String[] namespace, String[] defaultNamespace) {
        if (namespace == null || namespace.length < 1) {
            namespace = defaultNamespace;
            log.info("Use default namespace: {}", namespace);
        }
        List<Identifier> listIdentifiers = new LinkedList<>();
        ListTablesRequest listTablesRequest = new ListTablesRequest();
        listTablesRequest.setCatalogName(catalogName);
        listTablesRequest.setDatabaseName(namespace[0].toLowerCase());
        listTablesRequest.setProjectId(getClient().getProjectId());
        String pageToken = null;
        do {
            if (pageToken != null) {
                listTablesRequest.setNextToken(pageToken);
            }
            PagedList<Table> tablePagedList = client.listTables(listTablesRequest);
            pageToken = tablePagedList.getNextMarker();
            for (Table table : tablePagedList.getObjects()) {
                listIdentifiers.add(SparkHelper.createIdentifier(namespace, table.getTableName()));
            }
        } while (pageToken != null);
        return listIdentifiers.toArray(new Identifier[0]);
    }

    public static void createDatabase(Client client, String catalogName, String databaseName, Map<String, String> properties) {
        String comment = properties.get(SupportsNamespaces.PROP_COMMENT);
        String owner = properties.get(SupportsNamespaces.PROP_OWNER);
        String location = properties.get(SupportsNamespaces.PROP_LOCATION);
        if (StringUtils.isEmpty(location)) {
            final Catalog catalog = PolyCatClientHelper.getCatalog(client, catalogName);
            location = catalog.getLocation();
        }
        if (StringUtils.isEmpty(location)) {
            location = PolyCatOptions.getValueViaPrefixKey(properties, PolyCatOptions.DEFAULT_WAREHOUSE);
        }
        PolyCatClientHelper.createDatabase(client, catalogName, databaseName, comment, location, owner, PolyCatClientHelper.cleanMemoryParams(properties, DATABASE_PARAMS_REMOVE));
    }

    public static boolean databaseExists(Client client, String catalogName, String databaseName) {
        try {
            Database polyCatDatabase = getPolyCatDatabase(client, catalogName, databaseName);
            if (polyCatDatabase != null) {
                return true;
            }
        } catch (NoSuchNamespaceException e) {
            return false;
        }
        return false;
    }

    public static String[][] listDatabases(Client client, String catalogName) {
        return Arrays.stream(PolyCatClientHelper.listDatabases(client, catalogName))
                .map(database -> new String[]{database.getDatabaseName()})
                .toArray(String[][]::new);
    }

    public static Map<String, String> getDatabaseProperties(Client client, String catalogName, String databaseName)
        throws NoSuchNamespaceException {
        Database polyCatDatabase = getPolyCatDatabase(client, catalogName, databaseName);
        final Map<String, String> parameters = polyCatDatabase.getParameters();
        parameters.put(SupportsNamespaces.PROP_COMMENT, polyCatDatabase.getDescription());
        parameters.put(SupportsNamespaces.PROP_OWNER, polyCatDatabase.getOwner());
        parameters.put(SupportsNamespaces.PROP_LOCATION, polyCatDatabase.getLocationUri());
        return parameters;
    }

    public static Database getPolyCatDatabase(Client client, String catalogName, String databaseName) throws NoSuchNamespaceException {
        try {
            return PolyCatClientHelper.getDatabase(client, catalogName, databaseName);
        } catch (NoSuchObjectException e) {
            throw new NoSuchNamespaceException(new String[]{databaseName});
        }
    }

    public static void alterDatabase(Client client, String catalogName, String databaseName, NamespaceChange[] namespaceChanges) throws NoSuchNamespaceException {
        Database database;
        try {
            database = PolyCatClientHelper.getDatabase(client, catalogName, databaseName);
        } catch (NoSuchObjectException e) {
            throw new NoSuchNamespaceException(new String[]{databaseName});
        }
        String comment = null;
        String owner = null;
        String location = null;
        Map<String, String> parameters = new HashMap<>();
        for (NamespaceChange change: namespaceChanges) {
            if (change instanceof NamespaceChange.SetProperty) {
                final NamespaceChange.SetProperty setProperty = (NamespaceChange.SetProperty) change;
                final String property = setProperty.property();
                switch (property) {
                    case SupportsNamespaces.PROP_COMMENT:
                        comment = setProperty.value();
                        break;
                    case SupportsNamespaces.PROP_OWNER:
                        owner = setProperty.value();
                        break;
                    case SupportsNamespaces.PROP_LOCATION:
                        location = setProperty.value();
                        break;
                    default:
                        parameters.put(property, setProperty.value());
                        break;
                }
            } else if (change instanceof NamespaceChange.RemoveProperty) {
                throw new UnsupportedOperationException("Unsupport remove properties!");
            }
        }
        PolyCatClientHelper.alterDatabase(client, database, comment, location, owner, parameters);
    }

    public static boolean dropDatabase(Client client, String catalogName, String databaseName) throws NoSuchNamespaceException {
        try {
            PolyCatClientHelper.dropDatabase(client, catalogName, databaseName);
        } catch (NoSuchObjectException e) {
            throw new NoSuchNamespaceException(new String[]{databaseName});
        }
        return true;
    }

    public static StagedTable createStagedTable(Client client, TableName tableName, StructType schema,
                                                Transform[] partitions, Map<String, String> properties) {
        List<Column> columns = new ArrayList<>();
        List<Column> partitionColumns = new ArrayList<>();
        prepareColumns(columns, partitionColumns, schema, partitions);
        final StorageDescriptor storageDescriptor = buildStorageDescriptor(properties, columns);
        return newStagedTable(tableName, properties, storageDescriptor, partitionColumns);
    }

    private static StagedTable newStagedTable(TableName tableName, Map<String, String> properties, StorageDescriptor storageDescriptor, List<Column> partitionColumns) {
        CaseInsensitiveStringMap caseInsensitiveStringMap = new CaseInsensitiveStringMap(properties);
        return new PolyCatStagedTable(client,
                new TableName(client.getProjectId(), tableName.getCatalogName(), tableName.getDatabaseName(), tableName.getTableName()),
                convertSchema(storageDescriptor.getColumns(), partitionColumns),
                convertSchema(partitionColumns),
                sparkSession,
                getFileFormat(storageDescriptor.getSourceShortName(), properties),
                SparkHelper.getPaths(caseInsensitiveStringMap),
                SparkHelper.getOptionsWithoutPaths(caseInsensitiveStringMap),
                Option.apply(convertSchema(storageDescriptor.getColumns())));
    }

    private static org.apache.spark.sql.connector.catalog.Table convertToSparkTable(Table table, Client client) {
        final Map<String, String> parameters = table.getParameters();
        parameters.put("path", table.getStorageDescriptor().getLocation());
        parameters.put(TableCatalog.PROP_LOCATION, table.getStorageDescriptor().getLocation());
        parameters.put(TableCatalog.PROP_OWNER, table.getOwner());
        parameters.put(TableCatalog.PROP_COMMENT, table.getDescription());

        CaseInsensitiveStringMap caseInsensitiveStringMap = new CaseInsensitiveStringMap(parameters);
        return new PolyCatPartitionTable(client,
                new TableName(client.getProjectId(), table.getCatalogName(), table.getDatabaseName(), table.getTableName()),
                convertSchema(table.getFields(), table.getPartitionKeys()),
                convertSchema(table.getPartitionKeys()),
                sparkSession,
                getFileFormat(table.getStorageDescriptor().getSourceShortName(), parameters),
                SparkHelper.getPaths(caseInsensitiveStringMap),
                SparkHelper.getOptionsWithoutPaths(caseInsensitiveStringMap),
                Option.apply(convertSchema(table.getFields(), table.getPartitionKeys())));
    }

    private static String getFileFormat(String storageShortName, Map<String, String> parameters) {
        if (storageShortName == null || storageShortName.isEmpty()) {
            return parameters.getOrDefault(CatalogRouter.SPARK_SOURCE_PROVIDER, sparkSession.sessionState().conf().defaultDataSourceName());
        }
        // Compatible with historical data.
        if (formatToSerializationLibMap.containsValue(storageShortName)) {
            for (Entry<String, String> entry: formatToSerializationLibMap.entrySet()) {
                if (storageShortName.equals(entry.getValue())) {
                    return entry.getKey();
                }
            }
        }
        return storageShortName;
    }

    public static CaseInsensitiveStringMap getOptionsWithoutPaths(CaseInsensitiveStringMap map) {
        Map<String, String> withoutPath = new HashMap<>();
        for (Map.Entry<String, String> entry : map.asCaseSensitiveMap().entrySet()) {
            String key = entry.getKey();
            if (!"path".equalsIgnoreCase(key) && !"paths".equalsIgnoreCase(key)) {
                withoutPath.put(key, entry.getValue());
            }
        }

        return new CaseInsensitiveStringMap(withoutPath);
    }

    public static org.apache.spark.sql.connector.catalog.Table getSparkTable(Client client, TableName tableName) {
        Table table = PolyCatClientHelper.getTable(client, tableName);
        return convertToSparkTable(table, client);
    }

    public static org.apache.spark.sql.connector.catalog.Table createTable(Client client, TableName tableName, StructType schema,
                                                                    Transform[] partitions, Map<String, String> properties) {
        log.info("createTable properties: {}", properties);
        List<Column> columns = new ArrayList<>();
        List<Column> partitionColumns = new ArrayList<>();
        prepareColumns(columns, partitionColumns, schema, partitions);
        final StorageDescriptor storageDescriptor = buildStorageDescriptor(properties, columns);
        String comment = properties.get(SupportsNamespaces.PROP_COMMENT);
        final Table polyCatTable =
                PolyCatClientHelper.createTable(client, tableName, PolyCatClientHelper.cleanMemoryParams(properties, TABLE_PARAMS_REMOVE), storageDescriptor, partitionColumns, comment);
        return convertToSparkTable(polyCatTable, client);
    }

    public static org.apache.spark.sql.connector.catalog.Table alterTable(Client client, TableName tableName, TableChange[] changes) {
        final Table table = PolyCatClientHelper.getTable(client, tableName);
        final List<Column> oldColumns = table.getStorageDescriptor().getColumns();
        List<Column> addColumns = new ArrayList<>();
        Map<String, String> renameColumns = new HashMap<>();
        Map<String, Column> changeColumns = new HashMap<>();
        List<String> deleteColumns = new ArrayList<>();
        final Map<String, String> parameters = new HashMap<>();
        String location = null;
        String comment = null;
        String owner = null;
        for (TableChange change : changes) {
            if (change instanceof TableChange.ColumnChange) {
                prepareColumnChanges(change, oldColumns, addColumns, renameColumns, changeColumns, deleteColumns);
            } else if (change instanceof TableChange.SetProperty){
                final TableChange.SetProperty setProperty = (TableChange.SetProperty) change;
                final String property = setProperty.property();
                switch (property) {
                    case TableCatalog.PROP_LOCATION:
                        location = setProperty.value();
                        break;
                    case TableCatalog.PROP_COMMENT:
                        comment = setProperty.value();
                        break;
                    case TableCatalog.PROP_OWNER:
                        owner = setProperty.value();
                        break;
                    default:
                        parameters.put(setProperty.property(), setProperty.value());
                        break;
                }
            } else if (change instanceof TableChange.RemoveProperty) {
                throw new UnsupportedOperationException("Unsupport remove properties!");
            } else {
                throw new UnsupportedOperationException("Unsupported table change operation: " + change);
            }
        }
        PolyCatClientHelper.adaptCommonParameters(parameters);
        if (!addColumns.isEmpty()) {
            PolyCatClientHelper.addTableColumns(client, table, addColumns);
        }
        if (!renameColumns.isEmpty()) {
            PolyCatClientHelper.renameTableColumns(client, table, renameColumns);
        }
        if (!changeColumns.isEmpty()) {
            PolyCatClientHelper.changeTableColumns(client, table, changeColumns);
        }
        if (!deleteColumns.isEmpty()) {
            PolyCatClientHelper.deleteTableColumns(client, table, deleteColumns);
        }
        if (!parameters.isEmpty() || location != null || comment != null || owner != null) {
            PolyCatClientHelper.alterTable(client, table, parameters, location, comment, owner);
        }
        return getSparkTable(client, tableName);
    }

    private static void prepareColumnChanges(TableChange change,
        List<Column> oldColumns,
        List<Column> addColumns,
        Map<String, String> renameColumns,
        Map<String, Column> changeColumns,
        List<String> deleteColumns) {
        if (change instanceof TableChange.AddColumn) {
            final TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
            final Column column = new Column(addColumn.fieldNames()[0], addColumn.dataType().typeName(), addColumn.comment());
            addColumns.add(column);
        } else if (change instanceof TableChange.RenameColumn) {
            final TableChange.RenameColumn renameColumn = (TableChange.RenameColumn) change;
            renameColumns.put(renameColumn.fieldNames()[0], renameColumn.newName());
        } else if (change instanceof TableChange.UpdateColumnType) {
            final TableChange.UpdateColumnType updateColumnType = (TableChange.UpdateColumnType) change;
            final String fieldName = updateColumnType.fieldNames()[0];
            if (changeColumns.containsKey(fieldName)) {
                final Column column = changeColumns.get(fieldName);
                column.setColType(updateColumnType.newDataType().typeName());
            } else {
                final Column column = oldColumns.stream().filter(col -> col.getColumnName().equals(fieldName)).findFirst().get();
                column.setColType(updateColumnType.newDataType().typeName());
                changeColumns.put(fieldName, column);
            }
        } else if (change instanceof TableChange.UpdateColumnComment) {
            final TableChange.UpdateColumnComment updateColumnComment = (TableChange.UpdateColumnComment) change;
            final String fieldName = updateColumnComment.fieldNames()[0];
            if (changeColumns.containsKey(fieldName)) {
                final Column column = changeColumns.get(fieldName);
                column.setComment(updateColumnComment.newComment());
            } else {
                final Column column = oldColumns.stream().filter(col -> col.getColumnName().equals(fieldName)).findFirst().get();
                column.setComment(updateColumnComment.newComment());
                changeColumns.put(fieldName, column);
            }
        } else if (change instanceof TableChange.DeleteColumn) {
            final TableChange.DeleteColumn deleteColumn = (TableChange.DeleteColumn) change;
            deleteColumns.add(deleteColumn.fieldNames()[0]);
        } else {
            throw new UnsupportedOperationException("Unsupported column change operation: " + change);
        }
    }

    public static StorageDescriptor buildStorageDescriptor(Map<String, String> properties, List<Column> columnInputs) {
        StorageDescriptor storageInput = new StorageDescriptor();
        String location = properties.get(SupportsNamespaces.PROP_LOCATION);
        storageInput.setLocation(location);
        String fileFormat = properties.get("provider");
        if (fileFormat == null) {
            fileFormat = properties.get("hive.stored-as");
        }

        if (fileFormat != null) {
            storageInput.setSourceShortName(fileFormat);
            storageInput.setFileFormat(fileFormat);
        }
        final Option<HiveSerDe> hiveSerDeOption = HiveSerDe.sourceToSerDe(fileFormat);
        String serde;
        String inputFormat;
        String outputFormat;
        if (hiveSerDeOption.isDefined()) {
            final HiveSerDe hiveSerDe = hiveSerDeOption.get();
            inputFormat = hiveSerDe.inputFormat().get();
            outputFormat = hiveSerDe.outputFormat().get();
            serde = hiveSerDe.serde().get();
        } else {
            inputFormat = TEXT_INPUT_FORMAT;
            outputFormat = TEXT_OUTPUT_FORMAT;
            serde = formatToSerializationLibMap.get(fileFormat);
        }
        storageInput.setInputFormat(inputFormat);
        storageInput.setOutputFormat(outputFormat);
        final SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setSerializationLibrary(serde);
        storageInput.setSerdeInfo(serDeInfo);

        storageInput.setColumns(columnInputs);
        return storageInput;
    }

    private static void prepareColumns(List<Column> columns, List<Column> partitionColumns, StructType schema, Transform[] partitions) {
        StructField[] fields = schema.fields();
        Set<String> partitionColumnNames = Arrays.stream(partitions)
                .map(Object::toString)
                .collect(Collectors.toSet());
        for (StructField field : fields) {
            Column columnInput = new Column(field.name(), field.dataType().simpleString());
            if (partitionColumnNames.contains(field.name())) {
                partitionColumns.add(columnInput);
            } else {
                columns.add(columnInput);
            }
        }
    }
    private static Transform[] buildPartitioning(List<Column> partitionColumns) {
        if (partitionColumns == null || partitionColumns.isEmpty()) {
            return new Transform[0];
        }
        return partitionColumns.stream()
                .map(field -> SparkHelper.createTransform(field.getColumnName()))
                .toArray(Transform[]::new);
    }

    private static StructType convertSchema(List<Column> columns, List<Column> partitionColumns) {
        int columnNum = columns.size();
        int partitionColumnNum = 0;
        if (partitionColumns != null && !partitionColumns.isEmpty()) {
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

    private static StructType convertSchema(List<Column> columns) {
        int columnNum = columns.size();
        StructField[] structFields = new StructField[columnNum];
        int i = 0;
        for (Column schemaField : columns) {
            structFields[i++] = SparkHelper.convertField(schemaField);
        }
        return new StructType(structFields);
    }


    public static boolean isPathIdentifier(Identifier ident) {
        return ident instanceof PathIdentifier;
    }

    public static TableName getTableNameViaQualifiedName(String projectId, String qualifiedName) {
        String[] tableSplits = qualifiedName.split("\\.");
        if (tableSplits.length == 3) {
            return new TableName(projectId, tableSplits[0], tableSplits[1], tableSplits[2]);
        }
        throw new IllegalArgumentException("Table name: " + qualifiedName);
    }

    public static TableName getTableName(String projectId, String catalogName, Identifier ident) {
        String[] tableSplits = CatalogV2Implicits.IdentifierHelper(ident).quoted().split("\\.");
        if (tableSplits.length == 2) {
            return new TableName(projectId, catalogName, tableSplits[0], tableSplits[1]);
        }
        return new TableName(projectId, catalogName, DEFAULT_DATABASE, tableSplits[1]);
    }

    public static boolean doesPartitionExist(Client client, TableName tableName, List<String> partVals) {
        return PolyCatClientHelper.doesPartitionExist(client, tableName.getCatalogName(), tableName.getDatabaseName(),
                tableName.getTableName(), partVals);
    }

    public static boolean createPartitions(Client client, TableName tableName, StructType partitionSchema,
                                           Tuple2<List<String>, Map<String, String>>[] partValsAndProperties,
                                           String tablePath) {
        final List<PartitionInput> partitionInputs = Arrays.stream(partValsAndProperties)
                .map(tuple -> buildPartitionInput(tableName, partitionSchema, tuple._1, tuple._2, tablePath))
                .collect(Collectors.toList());
        return PolyCatClientHelper.createPartitions(client, tableName.getCatalogName(),
                tableName.getDatabaseName(), tableName.getTableName(), partitionInputs);
    }

    public static boolean createPartition(Client client, TableName tableName, StructType partitionSchema,
                                          List<String> partVals, Map<String, String> properties, String tablePath) {
        return PolyCatClientHelper.createPartition(client, tableName.getCatalogName(), tableName.getDatabaseName(),
                tableName.getTableName(), buildPartitionInput(tableName, partitionSchema, partVals, properties, tablePath));
    }

    private static PartitionInput buildPartitionInput(TableName tableName, StructType partitionSchema,
                                                      List<String> partVals, Map<String, String> properties, String tablePath) {
        PartitionInput partitionInput = new PartitionInput();
        partitionInput.setCatalogName(tableName.getCatalogName());
        partitionInput.setDatabaseName(tableName.getDatabaseName());
        partitionInput.setTableName(tableName.getTableName());
        partitionInput.setPartitionValues(partVals);
        partitionInput.setCreateTime(System.currentTimeMillis());
        StorageDescriptor sd = new StorageDescriptor();
        String location = properties.get(Constants.LOCATION);
        if (location == null) {
            location = String.format("%s/%s", tablePath, PolyCatClientHelper.makePartitionName(getPartitionKeys(partitionSchema), partVals));
        }
        sd.setLocation(location);
        partitionInput.setParameters(PolyCatClientHelper.cleanMemoryParams(properties, PARTITION_PARAMS_REMOVE));
        partitionInput.setStorageDescriptor(sd);
        return partitionInput;
    }

    public static Partition getPartition(Client client, TableName tableName, StructType partitionSchema,
                                         List<String> partVals) {
        final Partition partition = PolyCatClientHelper.getPartition(client, tableName.getCatalogName(), tableName.getDatabaseName(),
                tableName.getTableName(), getPartitionKeys(partitionSchema), partVals);
        if (partition != null) {
            partition.getParameters().put(Constants.LOCATION, partition.getStorageDescriptor().getLocation());
        }
        return partition;
    }

    public static void alterPartitionMetadata(Client client, TableName tableName, StructType partitionSchema,
                                              List<String> partVals, Map<String, String> newProperties) {
        Partition partition = getPartition(client, tableName, partitionSchema, partVals);
        if (partition != null) {
            if (newProperties.containsKey(Constants.LOCATION)) {
                partition.getStorageDescriptor().setLocation(newProperties.get(Constants.LOCATION));
            }
            PolyCatClientHelper.cleanMemoryParams(newProperties, PARTITION_PARAMS_REMOVE);
            partition.setParameters(newProperties);
        }
        PolyCatClientHelper.alterPartition(client, tableName.getCatalogName(), tableName.getDatabaseName(),
                tableName.getTableName(), partition, partVals);
    }

    public static List<String> getPartitionKeys(StructType partitionSchema) {
        return Arrays.asList(partitionSchema.fieldNames());
    }

    public static boolean dropPartition(Client client, TableName tableName, StructType partitionSchema,
                                        List<String> partVals) {
        return PolyCatClientHelper.dropPartition(client, tableName.getCatalogName(), tableName.getDatabaseName(),
                tableName.getTableName(), getPartitionKeys(partitionSchema), partVals);
    }

    public static boolean dropPartitions(Client client, TableName tableName, StructType partitionSchema,
        List<List<String>> partValsList) {
        List<String> partitionKeys = getPartitionKeys(partitionSchema);
        return PolyCatClientHelper.dropPartitions(client, tableName.getCatalogName(), tableName.getDatabaseName(),
            tableName.getTableName(), partValsList.stream().map(partVals -> PolyCatClientHelper.makePartitionName(partitionKeys, partVals)).collect(
                Collectors.toList()));
    }

    public static List<String> listPartitionNames(Client client, TableName tableName, List<String> partValuesFilter) {
        PartitionFilterInput input = new PartitionFilterInput();
        input.setValues(partValuesFilter.toArray(new String[]{}));
        return PolyCatClientHelper.listPartitionNames(client, tableName.getCatalogName(), tableName.getDatabaseName(),
                tableName.getTableName(), input);
    }

    public static boolean dropTable(Client client, TableName tableName) {
        PolyCatClientHelper.dropTable(client, tableName.getCatalogName(), tableName.getDatabaseName(),
                tableName.getTableName());
        return true;
    }
    
    public static void printLog(String key, Object o) {
        log.info(key + ": {}", o);
    }
}
