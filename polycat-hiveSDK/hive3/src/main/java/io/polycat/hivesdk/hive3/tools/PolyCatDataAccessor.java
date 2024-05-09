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
package io.polycat.hivesdk.hive3.tools;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.constants.CompatibleHiveConstants;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.Constraint;
import io.polycat.catalog.common.model.ConstraintType;
import io.polycat.catalog.common.model.ForeignKey;
import io.polycat.catalog.common.model.Order;
import io.polycat.catalog.common.model.PartitionAlterContext;
import io.polycat.catalog.common.model.PrimaryKey;
import io.polycat.catalog.common.model.SkewedInfo;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.base.PartitionInput;
import io.polycat.catalog.common.model.stats.BinaryColumnStatsData;
import io.polycat.catalog.common.model.stats.BooleanColumnStatsData;
import io.polycat.catalog.common.model.stats.ColumnStatistics;
import io.polycat.catalog.common.model.stats.ColumnStatisticsDesc;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import io.polycat.catalog.common.model.stats.DateColumnStatsData;
import io.polycat.catalog.common.model.stats.Decimal;
import io.polycat.catalog.common.model.stats.DecimalColumnStatsData;
import io.polycat.catalog.common.model.stats.DoubleColumnStatsData;
import io.polycat.catalog.common.model.stats.LongColumnStatsData;
import io.polycat.catalog.common.model.stats.StringColumnStatsData;
import io.polycat.catalog.common.plugin.request.input.*;

import java.io.IOException;
import java.util.*;
import io.polycat.catalog.common.plugin.request.input.AddPartitionInput;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.EnvironmentContextInput;
import io.polycat.catalog.common.plugin.request.input.FileInput;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.common.plugin.request.input.FunctionResourceUri;
import io.polycat.catalog.common.plugin.request.input.TableInput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.polycat.catalog.common.utils.GsonUtil;
import io.polycat.catalog.common.utils.TableUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.util.Strings;
import org.apache.thrift.TException;
@Slf4j
public class PolyCatDataAccessor {

    private static final Logger LOG = Logger.getLogger(PolyCatDataAccessor.class);

    // columns statistic
    @FunctionalInterface
    interface ThrowingFunction<T, R> {

        R apply(T var1) throws TException;
    }

    private static HashMap<String,String> serializationLibToSourceNameMap = new HashMap() {
        {
            put("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "parquet");
            put("org.apache.hadoop.hive.ql.io.orc.OrcSerde", "ORC");
            put("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", "textfile");
            put("org.apache.hadoop.hive.serde2.OpenCSVSerde", "csv");
            put("org.apache.carbondata.hive.CarbonHiveSerDe", "carbondata");
        }
    };

    static private final Map<String, ThrowingFunction<Object, Object>> statsMap = new HashMap<String, ThrowingFunction<Object, Object>>() {{
        put("binaryStats", PolyCatDataAccessor::convertToBinaryData);
        put("booleanStats", PolyCatDataAccessor::convertToBooleanData);
        put("dateStats", PolyCatDataAccessor::convertToDateData);
        put("decimalStats", PolyCatDataAccessor::convertToDecimalData);
        put("doubleStats", PolyCatDataAccessor::convertToDoubleData);
        put("longStats", PolyCatDataAccessor::convertToLongData);
        put("stringStats", PolyCatDataAccessor::convertToStringData);
    }};

    private static Object convertToBinaryData(Object statsData) throws TException {
        if (!(statsData instanceof org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData)) {
            throw new TException("transform illegal");
        }

        org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData hiveBinaryData =
            (org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData) statsData;
        BinaryColumnStatsData polyCatBinaryData = new BinaryColumnStatsData();
        polyCatBinaryData.setMaxColLen(hiveBinaryData.getMaxColLen());
        polyCatBinaryData.setAvgColLen(hiveBinaryData.getAvgColLen());
        polyCatBinaryData.setNumNulls(hiveBinaryData.getNumNulls());
        polyCatBinaryData.setBitVectors(hiveBinaryData.getBitVectors());
        return polyCatBinaryData;
    }

    private static Object convertToBooleanData(Object statsData) throws TException {
        if (!(statsData instanceof org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData)) {
            throw new TException("transform illegal");
        }

        org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData hiveBooleanData =
            (org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData) statsData;
        BooleanColumnStatsData polyCatBooleanData = new BooleanColumnStatsData();
        polyCatBooleanData.setNumNulls(hiveBooleanData.getNumNulls());
        polyCatBooleanData.setNumTrues(hiveBooleanData.getNumTrues());
        polyCatBooleanData.setNumFalses(hiveBooleanData.getNumFalses());
        polyCatBooleanData.setBitVectors(hiveBooleanData.getBitVectors());
        return polyCatBooleanData;
    }


    private static Object convertToDateData(Object statsData) throws TException {
        if (!(statsData instanceof org.apache.hadoop.hive.metastore.api.DateColumnStatsData)) {
            throw new TException("transform illegal");
        }

        org.apache.hadoop.hive.metastore.api.DateColumnStatsData hiveDateData =
            (org.apache.hadoop.hive.metastore.api.DateColumnStatsData) statsData;
        DateColumnStatsData polyCatDateData = new DateColumnStatsData();
        polyCatDateData.setHighValue(hiveDateData.getHighValue().getDaysSinceEpoch());
        polyCatDateData.setLowValue(hiveDateData.getLowValue().getDaysSinceEpoch());
        polyCatDateData.setNumDVs(hiveDateData.getNumDVs());
        polyCatDateData.setNumNulls(hiveDateData.getNumNulls());
        polyCatDateData.setBitVectors(hiveDateData.getBitVectors());
        return polyCatDateData;
    }

    private static Object convertToDecimalData(Object statsData) throws TException {
        if (!(statsData instanceof org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData)) {
            throw new TException("transform illegal");
        }

        org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData hiveDecimalData =
            (org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData) statsData;
        DecimalColumnStatsData polyCatDecimalData = new DecimalColumnStatsData();
        if (hiveDecimalData.getHighValue() != null) {
            polyCatDecimalData.setHighValue(
                    new Decimal(hiveDecimalData.getHighValue().getScale(), hiveDecimalData.getHighValue().getUnscaled()));
        } else {
            polyCatDecimalData.setHighValue(new Decimal((short) 0, new byte[]{0}));
        }
        if (hiveDecimalData.getLowValue() != null) {
            polyCatDecimalData.setLowValue(
                    new Decimal(hiveDecimalData.getLowValue().getScale(), hiveDecimalData.getLowValue().getUnscaled()));
        } else {
            polyCatDecimalData.setLowValue(new Decimal((short) 0, new byte[]{0}));
        }
        polyCatDecimalData.setNumDVs(hiveDecimalData.getNumDVs());
        polyCatDecimalData.setNumNulls(hiveDecimalData.getNumNulls());
        polyCatDecimalData.setBitVectors(hiveDecimalData.getBitVectors());
        return polyCatDecimalData;
    }

    private static Object convertToDoubleData(Object statsData) throws TException {
        if (!(statsData instanceof org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData)) {
            throw new TException("transform illegal");
        }

        org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData hiveDoubleData =
            (org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData) statsData;
        DoubleColumnStatsData polyCatDoubleData = new DoubleColumnStatsData();
        polyCatDoubleData.setHighValue(hiveDoubleData.getHighValue());
        polyCatDoubleData.setLowValue(hiveDoubleData.getLowValue());
        polyCatDoubleData.setNumDVs(hiveDoubleData.getNumDVs());
        polyCatDoubleData.setNumNulls(hiveDoubleData.getNumNulls());
        polyCatDoubleData.setBitVectors(hiveDoubleData.getBitVectors());
        return polyCatDoubleData;
    }

    private static Object convertToLongData(Object statsData) throws TException {
        if (!(statsData instanceof org.apache.hadoop.hive.metastore.api.LongColumnStatsData)) {
            throw new TException("transform illegal");
        }

        org.apache.hadoop.hive.metastore.api.LongColumnStatsData hiveLongData =
            (org.apache.hadoop.hive.metastore.api.LongColumnStatsData) statsData;
        LongColumnStatsData polyCatLongData = new LongColumnStatsData();
        polyCatLongData.setHighValue(hiveLongData.getHighValue());
        polyCatLongData.setLowValue(hiveLongData.getLowValue());
        polyCatLongData.setNumNulls(hiveLongData.getNumNulls());
        polyCatLongData.setNumDVs(hiveLongData.getNumDVs());
        polyCatLongData.setBitVectors(hiveLongData.getBitVectors());
        return polyCatLongData;
    }

    private static Object convertToStringData(Object statsData) throws TException {
        if (!(statsData instanceof org.apache.hadoop.hive.metastore.api.StringColumnStatsData)) {
            throw new TException("transform illegal");
        }

        org.apache.hadoop.hive.metastore.api.StringColumnStatsData hiveStringData =
            (org.apache.hadoop.hive.metastore.api.StringColumnStatsData) statsData;
        StringColumnStatsData polyCatStringData = new StringColumnStatsData();
        polyCatStringData.setAvgColLen(hiveStringData.getAvgColLen());
        polyCatStringData.setMaxColLen(hiveStringData.getMaxColLen());
        polyCatStringData.setNumNulls(hiveStringData.getNumNulls());
        polyCatStringData.setNumDVs(hiveStringData.getNumDVs());
        polyCatStringData.setBitVectors(hiveStringData.getBitVectors());
        return polyCatStringData;
    }

    public static ColumnStatisticsObj toColumnStatisticsObj(
            org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj hiveStats) throws TException {
        ColumnStatisticsObj polyCatStatsObj = new ColumnStatisticsObj();
        polyCatStatsObj.setColName(hiveStats.getColName());
        polyCatStatsObj.setColType(hiveStats.getColType());
        polyCatStatsObj.setDataType(hiveStats.getStatsData().getSetField().getFieldName());
        polyCatStatsObj.setDataValue(statsMap.get(hiveStats.getStatsData().getSetField().getFieldName())
                .apply(hiveStats.getStatsData().getFieldValue()));
        return polyCatStatsObj;
    }

    public static ColumnStatistics toColumnStatistics(org.apache.hadoop.hive.metastore.api.ColumnStatistics hiveStats)
        throws TException {
        org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc hiveDesc = hiveStats.getStatsDesc();
        ColumnStatisticsDesc lsmDesc = new ColumnStatisticsDesc(hiveDesc.isIsTblLevel(), hiveDesc.getCatName(),
            hiveDesc.getDbName(), hiveDesc.getTableName());
        lsmDesc.setPartName(hiveDesc.getPartName());
        lsmDesc.setLastAnalyzed(hiveDesc.getLastAnalyzed());
        List<ColumnStatisticsObj> lcObjs = new ArrayList<>();
        for (org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj hiveObjs : hiveStats.getStatsObj()) {
            lcObjs.add(toColumnStatisticsObj(hiveObjs));
        }
        return new ColumnStatistics(lsmDesc, lcObjs);
    }

    public static DatabaseInput toDatabaseInput(Database database, String accountId, String authSourceType) {
        DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setDatabaseName(database.getName());
        databaseInput.setLocationUri(database.getLocationUri());
        databaseInput.setParameters(database.getParameters());
        databaseInput.setCatalogName(database.getCatalogName());
        databaseInput.setDescription(database.getDescription());
        databaseInput.setOwner(getUserId(database.getOwnerName()));
        databaseInput.setOwnerType(database.isSetOwnerType() ? database.getOwnerType().name() : null);
        databaseInput.setAccountId(accountId);
        databaseInput.setAuthSourceType(authSourceType);
        return databaseInput;
    }

    public static CatalogInput toCatalogInput(Catalog catalog) {
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setCatalogName(catalog.getName());
        catalogInput.setDescription(catalog.getDescription());
        catalogInput.setLocation(catalog.getLocationUri());
        catalogInput.setOwner(getUserId());
        return catalogInput;
    }

    private static StorageDescriptor convertToStorageDescriptor(
        org.apache.hadoop.hive.metastore.api.StorageDescriptor sd) {
        if (sd == null) {
            return null;
        }
        StorageDescriptor tblStorage = new StorageDescriptor();
        tblStorage.setLocation(sd.getLocation());
        // source Shot Name;
        // file Format
        tblStorage.setInputFormat(sd.getInputFormat());
        tblStorage.setOutputFormat(sd.getOutputFormat());
        tblStorage.setParameters(sd.getParameters());
        tblStorage.setColumns(convertToColumns(sd.getCols()));
        tblStorage.setCompressed(sd.isCompressed());
        tblStorage.setNumberOfBuckets(sd.getNumBuckets());
        tblStorage.setBucketColumns(sd.getBucketCols());
        tblStorage.setSerdeInfo(fillSerDeInfo(sd.getSerdeInfo()));
        tblStorage.setSortColumns(fillOrderList(sd.getSortCols()));
        tblStorage.setSkewedInfo(convertToSkewedInfo(sd.getSkewedInfo()));
        tblStorage.setStoredAsSubDirectories(sd.isStoredAsSubDirectories());
        // tblStorage.setSourceShortName();
        // tblStorage.setFileFormat();
        return tblStorage;
    }

    private static List<Order> fillOrderList(List<org.apache.hadoop.hive.metastore.api.Order> sortCols) {
        if (sortCols == null) {
            return null;
        }
        return sortCols.stream()
            .map(hiveSortCol -> new Order(hiveSortCol.getCol(), hiveSortCol.getOrder()))
            .collect(Collectors.toList());
    }

    private static SkewedInfo convertToSkewedInfo(org.apache.hadoop.hive.metastore.api.SkewedInfo hiveSkewedInfo) {
        if (hiveSkewedInfo == null) {
            return null;
        }
        SkewedInfo lmsSkewedInfo = new SkewedInfo();
        lmsSkewedInfo.setSkewedColumnNames(hiveSkewedInfo.getSkewedColNames());
        lmsSkewedInfo.setSkewedColumnValues(hiveSkewedInfo.getSkewedColValues());
        Map<String, String> skewedMaps = new HashMap<>(hiveSkewedInfo.getSkewedColValueLocationMapsSize());
        Map<List<String>, String> hiveSkewedMaps = hiveSkewedInfo.getSkewedColValueLocationMaps();
        for (List<String> values : hiveSkewedMaps.keySet()) {
            skewedMaps.put(convertToValueString(values), hiveSkewedMaps.get(values));
        }
        lmsSkewedInfo.setSkewedColumnValueLocationMaps(skewedMaps);
        return lmsSkewedInfo;
    }

    private static String convertToValueString(List<String> values) {
        if (values == null) {
            return Strings.EMPTY;
        }

        StringBuilder valStr = new StringBuilder();
        for (String cur : values) {
            valStr.append(cur.length()).append("$").append(cur);
        }

        return valStr.toString();
    }

    public static TableInput toTableInput(Table table) {
        return toTableInput(table, null);
    }

    public static TableInput toTableInput(Table table, String defaultUser) {
        TableInput tableInput = new TableInput();
        List<Column> partitions = null;
        if (table.getPartitionKeys() != null) {
            partitions = table.getPartitionKeys().stream()
                    .map(PolyCatDataAccessor::convertToColumnInput).collect(Collectors.toList());
        } else {
            partitions = Collections.emptyList();
        }
        tableInput.setLmsMvcc(false);

        tableInput.setTableName(table.getTableName());
        tableInput.setPartitionKeys(partitions);
        tableInput.setRetention(table.getRetention());
        tableInput.setOwner(getTableOwner(table, defaultUser));
        tableInput.setStorageDescriptor(fillInStorageDescriptor(table, partitions));
        tableInput.getParameters().putAll(table.getParameters());
        setTableTypeParams(tableInput, table);
        return tableInput;
    }

    private static String getTableOwner(Table table, String defaultUser) {
        String owner = table.getOwner();
        if (table.getParameters() != null &&
                TableUtil.isIcebergTableByParams(table.getParameters()) &&
                table.getParameters().containsKey(Constants.OWNER_PARAM)) {
            owner = getUserId(table.getParameters().get(Constants.OWNER_PARAM), defaultUser);
            table.getParameters().put(Constants.OWNER_PARAM, owner);
        } else {
            owner = getUserId(owner, defaultUser);
        }
        return owner;
    }

    private static void setTableTypeParams(TableInput tableInput, Table newt) {
        // If the table has property EXTERNAL set, update table type
        // accordingly
        String tableType = newt.getTableType();
        try {
            if (StringUtils.isNotEmpty(tableType)) {
                boolean isExternal = "TRUE".equals(newt.getParameters().get("EXTERNAL"));
                switch (TableTypeInput.valueOf(tableType)) {
                    case VIRTUAL_VIEW:
                        tableInput.setViewExpandedText(newt.getViewExpandedText());
                        tableInput.setViewOriginalText(newt.getViewOriginalText());
                        break;
                    case MANAGED_TABLE:
                        if (isExternal) {
                            tableType = TableTypeInput.EXTERNAL_TABLE.toString();
                        }
                        break;
                    case EXTERNAL_TABLE:
                    /*
                    // Do you still need to keep it in cloud native? For the time being:EXTERNAL_TABLE
                    if (!isExternal) {
                        tableType = TableTypeInput.MANAGED_TABLE.toString();
                    }*/
                        break;
                    case INDEX_TABLE:
                        break;
                    case MATERIALIZED_VIEW:
                        setMaterializedView(tableInput, newt);
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            //Special engine type support
            log.warn("Table: {}.{} type={} unresolved", newt.getDbName(), newt.getTableName(), tableType, e);
        }
        tableInput.setTableType(tableType);
    }

    private static void setMaterializedView(TableInput tableInput, Table newt) {
        CreationMetadata hiveCreationMetadata = newt.getCreationMetadata();
        if (hiveCreationMetadata != null) {
            Map<String, String> map = new HashMap<>();
            if (tableInput.getParameters().containsKey(CompatibleHiveConstants.V3_PARAM)) {
                map = GsonUtil.fromJson(tableInput.getParameters().get(CompatibleHiveConstants.V3_PARAM), Map.class);
            }
            map.put(CompatibleHiveConstants.V3_TABLE_CREATION_METADATA, GsonUtil.toJson(hiveCreationMetadata, false));
            tableInput.getParameters().put(CompatibleHiveConstants.V3_PARAM, GsonUtil.toJson(map));
        }
    }

    private static StorageDescriptor fillInStorageDescriptor(Table newt, List<Column> partitions) {
        org.apache.hadoop.hive.metastore.api.StorageDescriptor sd = newt.getSd();
        if (sd == null) {
            return null;
        }

        StorageDescriptor storageInput = new StorageDescriptor();
        storageInput.setLocation(sd.getLocation());
        storageInput.setCompressed(sd.isCompressed());
        String serializationLib = sd.getSerdeInfo().getSerializationLib();
        String sourceName = serializationLibToSourceNameMap.getOrDefault(serializationLib, serializationLib);
        if (newt.getParameters().get("spark.sql.sources.provider") != null) {
            sourceName = newt.getParameters().get("spark.sql.sources.provider");
        }

        storageInput.setSourceShortName(sourceName);
        storageInput.setInputFormat(sd.getInputFormat());
        storageInput.setOutputFormat(sd.getOutputFormat());
        storageInput.setNumberOfBuckets(sd.getNumBuckets());
        storageInput.setBucketColumns(sd.getBucketCols());
        storageInput.setSortColumns(fillOrderList(sd.getSortCols()));
        storageInput.setParameters(sd.getParameters());
        storageInput.setSerdeInfo(fillSerDeInfo(sd.getSerdeInfo()));
        storageInput.setColumns(buildColumnInputs(newt, partitions));
        storageInput.setSkewedInfo(convertToLmsSkewedInfo(sd.getSkewedInfo()));
        storageInput.setStoredAsSubDirectories(sd.isStoredAsSubDirectories());
        return storageInput;
    }

    private static List<Column> buildColumnInputs(Table table, List<Column> partitions) {
        List<Column> columnInputs;
        if (table.getParameters().containsKey("spark.sql.sources.provider") &&
                "csv".equalsIgnoreCase(table.getParameters().get("spark.sql.sources.provider"))) {
            int partNum = Integer.parseInt(table.getParameters().get("spark.sql.sources.schema.numParts"));
            columnInputs = new ArrayList<>();
            for (int i = 0; i < partNum; i++) {
                columnInputs.addAll(getColumnInputs(table, i));
            }
            removePartitionColumns(columnInputs, partitions);
        } else {
            columnInputs = table.getSd().getCols().stream()
                    .map(PolyCatDataAccessor::convertToColumnInput).collect(Collectors.toList());
        }
        return columnInputs;
    }

    private static void removePartitionColumns(List<Column> columnList, List<Column> partitions) {
        if (partitions == null || partitions.isEmpty()) {
            return;
        }
        for (int i = partitions.size() - 1; i >= 0; i = i - 1) {
            String partColumnName = partitions.get(i).getColumnName();
            int lastIndex = columnList.size() - 1;
            if (partColumnName.equals(columnList.get(lastIndex).getColumnName())) {
                columnList.remove(lastIndex);
            }
        }
    }

    private static io.polycat.catalog.common.model.SerDeInfo fillSerDeInfo(
            org.apache.hadoop.hive.metastore.api.SerDeInfo hiveSerDe) {
        if (hiveSerDe != null) {
            io.polycat.catalog.common.model.SerDeInfo serDeInfo = new io.polycat.catalog.common.model.SerDeInfo();
            serDeInfo.setName(hiveSerDe.getName());
            serDeInfo.setSerializationLibrary(hiveSerDe.getSerializationLib());
            serDeInfo.setParameters(hiveSerDe.getParameters());
            return serDeInfo;
        }
        return null;
    }

    private static List<Column> getColumnInputs(Table table, int location) {
        String struct = table.getParameters().get("spark.sql.sources.schema.part." + location);
        JSONObject structJson = JSONObject.parseObject(struct);
        JSONArray schemas = structJson.getJSONArray("fields");
        Schema[] fieldSchemas = schemas.toJavaObject(Schema[].class);
        return Arrays.stream(fieldSchemas).map(PolyCatDataAccessor::convertToTableInput).collect(Collectors.toList());
    }

    private static Column convertToTableInput(Schema fieldSchema) {
        Column columnInput = new Column();
        columnInput.setColumnName(fieldSchema.getName());
        columnInput.setColType(fieldSchema.getType());
        String comment = ((JSONObject) fieldSchema.getMetadata()).getString("comment");
        columnInput.setComment(comment);
        return columnInput;
    }

    public static Column convertToColumnInput(FieldSchema fieldSchema) {
        Column columnInput = new Column();
        columnInput.setColumnName(fieldSchema.getName());
        columnInput.setColType(fieldSchema.getType());
        columnInput.setComment(fieldSchema.getComment());
        return columnInput;
    }

    private static SkewedInfo convertToLmsSkewedInfo(org.apache.hadoop.hive.metastore.api.SkewedInfo hiveSkewedInfo) {
        if (hiveSkewedInfo == null) {
            return null;
        }
        SkewedInfo lmsSkewedInfo = new SkewedInfo();
        lmsSkewedInfo.setSkewedColumnNames(hiveSkewedInfo.getSkewedColNames());
        lmsSkewedInfo.setSkewedColumnValues(hiveSkewedInfo.getSkewedColValues());
        Map<String, String> skewedMaps = new HashMap<>(hiveSkewedInfo.getSkewedColValueLocationMapsSize());
        Map<List<String>, String> hiveSkewedMaps = hiveSkewedInfo.getSkewedColValueLocationMaps();
        for (List<String> values : hiveSkewedMaps.keySet()) {
            skewedMaps.put(convertToValueString(values), hiveSkewedMaps.get(values));
        }
        lmsSkewedInfo.setSkewedColumnValueLocationMaps(skewedMaps);
        return lmsSkewedInfo;
    }

    private static List<Column> convertToColumns(List<FieldSchema> partitionKeys) {
        if (Objects.isNull(partitionKeys)) {
            return Collections.emptyList();
        }
        return partitionKeys.stream()
            .map(hiveSch -> {
                Column lsmSch = new Column();
                lsmSch.setColumnName(hiveSch.getName());
                lsmSch.setColType(hiveSch.getType());
                lsmSch.setComment(hiveSch.getComment());
                return lsmSch;
            }).collect(Collectors.toList());
    }

    public static EnvironmentContextInput toEnvironmentInput(EnvironmentContext environmentContext) {
        if (environmentContext != null) {
            EnvironmentContextInput input = new EnvironmentContextInput();
            input.setEnvContext(environmentContext.getProperties());
            return input;
        }
        return null;
    }

    public static Map<String, String> toAlterTableParams(EnvironmentContext environmentContext) {
        return environmentContext != null ? environmentContext.getProperties() : null;
    }

    // partition
    public static PartitionInput toPartitionBaseInput(org.apache.hadoop.hive.metastore.api.Partition hivePartition) {
        PartitionInput partitionBase = new PartitionInput();
        partitionBase.setPartitionValues(hivePartition.getValues());
        partitionBase.setCatalogName(hivePartition.getCatName());
        partitionBase.setDatabaseName(hivePartition.getDbName());
        partitionBase.setTableName(hivePartition.getTableName());
        partitionBase.setCreateTime((long) hivePartition.getCreateTime());
        partitionBase.setLastAccessTime((long) hivePartition.getLastAccessTime());
        StorageDescriptor lmPartSd = new StorageDescriptor();
        org.apache.hadoop.hive.metastore.api.StorageDescriptor partSd = hivePartition.getSd();
        if (partSd != null) {
            lmPartSd.setParameters(partSd.getParameters());
            if (partSd.getCols() != null) {
                lmPartSd.setColumns(partSd.getCols().stream().map(PolyCatDataAccessor::convertToColumnInput).collect(Collectors.toList()));
            }
            lmPartSd.setSerdeInfo(convertToLmsSerDeInfo(partSd.getSerdeInfo()));
            lmPartSd.setSerdeInfo(convertToLmsSerDeInfo(partSd.getSerdeInfo()));
            lmPartSd.setSkewedInfo(convertToLmsSkewedInfo(partSd.getSkewedInfo()));
            if (!StringUtils.isBlank(partSd.getLocation())) {
                lmPartSd.setLocation(partSd.getLocation());
            }
            lmPartSd.setInputFormat(partSd.getInputFormat());
            lmPartSd.setOutputFormat(partSd.getOutputFormat());
        }
        partitionBase.setParameters(hivePartition.getParameters());
        partitionBase.setStorageDescriptor(lmPartSd);
        partitionBase.setFiles(new FileInput[]{});
        partitionBase.setIndex(new FileStatsInput[]{});
        return partitionBase;
    }

    public static io.polycat.catalog.common.model.SerDeInfo convertToLmsSerDeInfo(SerDeInfo serdeInfo) {
        io.polycat.catalog.common.model.SerDeInfo lmsSerDeInfo = new io.polycat.catalog.common.model.SerDeInfo();
        if (serdeInfo != null) {
            lmsSerDeInfo.setName(serdeInfo.getName());
            lmsSerDeInfo.setParameters(serdeInfo.getParameters());
            lmsSerDeInfo.setSerializationLibrary(serdeInfo.getSerializationLib());
        }
        return lmsSerDeInfo;
    }

    public static String getPartitionName(Partition partition, Table table) throws MetaException {
        return Warehouse.makePartName(table.getPartitionKeys(), partition.getValues());
    }

    public static String getPartitionName(List<String> values, Table table) throws MetaException {
        if (Objects.isNull(values) || Objects.isNull(table)) {
            throw new MetaException("values can't be null");
        }
        return Warehouse.makePartName(table.getPartitionKeys(), values);
    }

    public static AddPartitionInput toPartitionInput(Partition partition) {
        AddPartitionInput partInput = new AddPartitionInput();
        PartitionInput baseInput = toPartitionBaseInput(partition);
        PartitionInput[] baseInputs = new PartitionInput[1];
        baseInputs[0] = baseInput;
        partInput.setOverwrite(false);
        partInput.setBasedVersion(null);
        partInput.setFileFormat(null);
        partInput.setNeedResult(false);
        partInput.setIfNotExist(false);
        partInput.setPartitions(baseInputs);
        return partInput;
    }

    public static AddPartitionInput toPartitionInput(List<Partition> partitions) {
        AddPartitionInput partInput = new AddPartitionInput();
        int partSize = partitions.size();
        PartitionInput[] baseInputs = new PartitionInput[partSize];
        for (int i = 0; i < partSize; i++) {
            baseInputs[i] = toPartitionBaseInput(partitions.get(i));
        }
        partInput.setOverwrite(false);
        partInput.setBasedVersion(null);
        partInput.setFileFormat(null);
        partInput.setNeedResult(false);
        partInput.setIfNotExist(false);
        partInput.setPartitions(baseInputs);
        return partInput;
    }

    public static List<Pair<Integer, byte[]>> toPartitonExprs(List<ObjectPair<Integer, byte[]>> partExprs) {
        return partExprs.stream()
            .map(hiveExprs -> new ImmutablePair<>(hiveExprs.getFirst(), hiveExprs.getSecond()))
            .collect(Collectors.toList());
    }

    public static PartitionAlterContext toPartitionAlterContext(Partition hivePart) {
        PartitionAlterContext ctx = new PartitionAlterContext();
        org.apache.hadoop.hive.metastore.api.StorageDescriptor hiveSd = hivePart.getSd();
        if (Objects.nonNull(hiveSd)) {
            ctx.setLocation(hiveSd.getLocation());
            ctx.setInputFormat(hiveSd.getInputFormat());
            ctx.setOutputFormat(hiveSd.getOutputFormat());
        }
        ctx.setOldValues(hivePart.getValues());
        ctx.setNewValues(hivePart.getValues());
        ctx.setParameters(hivePart.getParameters());
        ctx.setCreateTime(hivePart.getCreateTime());
        ctx.setLastAccessTime(hivePart.getLastAccessTime());
        return ctx;
    }

    public static FunctionInput toFunctionInput(Function hiveFunc) {
        FunctionInput functionInput = new FunctionInput();
        functionInput.setFunctionName(hiveFunc.getFunctionName());
        functionInput.setClassName(hiveFunc.getClassName());
        if (hiveFunc.getOwnerName() != null) {
            functionInput.setOwner(hiveFunc.getOwnerName());
        } else {
            functionInput.setOwner(getUserId());
        }
        functionInput.setOwnerType(hiveFunc.isSetOwnerType() ? hiveFunc.getOwnerType().name() : null);
        functionInput.setFuncType(hiveFunc.isSetFunctionType() ? hiveFunc.getFunctionType().name() : null);
        functionInput.setCatalogName(hiveFunc.getCatName());
        functionInput.setDatabaseName(hiveFunc.getDbName());
        functionInput.setCreateTime(hiveFunc.getCreateTime());
        if (hiveFunc.getResourceUris() != null) {
            List<FunctionResourceUri> rUris = new ArrayList<>();
            for (ResourceUri ru : hiveFunc.getResourceUris()) {
                FunctionResourceUri rUri = new FunctionResourceUri(ru.getResourceType().name(), ru.getUri());
                rUris.add(rUri);
            }
            functionInput.setResourceUris(rUris);
        }
        return functionInput;
    }

    // constraint beg
    public static PrimaryKey toPrimaryKey(SQLPrimaryKey hivePKey) {
        PrimaryKey lsmPKey = new PrimaryKey();
        lsmPKey.setCatName(hivePKey.getCatName());
        lsmPKey.setDbName(hivePKey.getTable_db());
        lsmPKey.setTableName(hivePKey.getTable_name());
        lsmPKey.setColumnName(hivePKey.getColumn_name());
        lsmPKey.setPkName(hivePKey.getPk_name());
        lsmPKey.setEnable_cstr(hivePKey.isEnable_cstr());
        lsmPKey.setRely_cstr(hivePKey.isRely_cstr());
        lsmPKey.setValidate_cstr(hivePKey.isValidate_cstr());
        lsmPKey.setKeySeq(hivePKey.getKey_seq());
        return lsmPKey;
    }


    public static ForeignKey toForeignKey(SQLForeignKey hiveFKey) {
        ForeignKey lsmFKey = new ForeignKey();
        lsmFKey.setCatName(hiveFKey.getCatName());
        lsmFKey.setFkTableDb(hiveFKey.getFktable_db());
        lsmFKey.setPkTableDb(hiveFKey.getPktable_db());
        lsmFKey.setFkTableName(hiveFKey.getFktable_name());
        lsmFKey.setPkTableName(hiveFKey.getPktable_name());
        lsmFKey.setPkColumnName(hiveFKey.getPkcolumn_name());
        lsmFKey.setFkColumnName(hiveFKey.getFkcolumn_name());
        lsmFKey.setPkName(hiveFKey.getPk_name());
        lsmFKey.setFkName(hiveFKey.getFk_name());
        lsmFKey.setEnable_cstr(hiveFKey.isEnable_cstr());
        lsmFKey.setRely_cstr(hiveFKey.isRely_cstr());
        lsmFKey.setValidate_cstr(hiveFKey.isValidate_cstr());
        lsmFKey.setKeySeq(hiveFKey.getKey_seq());
        lsmFKey.setDeleteRule(hiveFKey.getDelete_rule());
        lsmFKey.setUpdateRule(hiveFKey.getUpdate_rule());
        return lsmFKey;
    }


    public static List<Constraint> toDefaultConstraint(List<SQLDefaultConstraint> defaultConstraints) {
        return defaultConstraints.stream().map(hiveCstr -> {
            Constraint lsmCstr = new Constraint();
            lsmCstr.setCstr_type(ConstraintType.DEFAULT_CSTR);
            lsmCstr.setCatName(hiveCstr.getCatName());
            lsmCstr.setDbName(hiveCstr.getTable_db());
            lsmCstr.setTable_name(hiveCstr.getTable_name());
            lsmCstr.setColumn_name(hiveCstr.getColumn_name());
            lsmCstr.setValidate_cstr(hiveCstr.isValidate_cstr());
            lsmCstr.setEnable_cstr(hiveCstr.isEnable_cstr());
            lsmCstr.setRely_cstr(hiveCstr.isRely_cstr());
            lsmCstr.setCstr_name(hiveCstr.getDc_name());
            lsmCstr.setCstr_info(hiveCstr.getDefault_value());
            return lsmCstr;
        }).collect(Collectors.toList());
    }

    public static List<Constraint> toUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraints) {
        return uniqueConstraints.stream().map(hiveCstr -> {
            Constraint lsmCstr = new Constraint();
            lsmCstr.setCstr_type(ConstraintType.UNIQUE_CSTR);
            lsmCstr.setCatName(hiveCstr.getCatName());
            lsmCstr.setDbName(hiveCstr.getTable_db());
            lsmCstr.setTable_name(hiveCstr.getTable_name());
            lsmCstr.setColumn_name(hiveCstr.getColumn_name());
            lsmCstr.setValidate_cstr(hiveCstr.isValidate_cstr());
            lsmCstr.setEnable_cstr(hiveCstr.isEnable_cstr());
            lsmCstr.setRely_cstr(hiveCstr.isRely_cstr());
            lsmCstr.setCstr_name(hiveCstr.getUk_name());
            lsmCstr.setCstr_info(String.valueOf(hiveCstr.getKey_seq()));
            return lsmCstr;
        }).collect(Collectors.toList());
    }

    public static List<Constraint> toNotNullConstraint(List<SQLNotNullConstraint> notNullConstraints) {
        return notNullConstraints.stream().map(hiveCstr -> {
            Constraint lsmCstr = new Constraint();
            lsmCstr.setCstr_type(ConstraintType.NOT_NULL_CSTR);
            lsmCstr.setCatName(hiveCstr.getCatName());
            lsmCstr.setDbName(hiveCstr.getTable_db());
            lsmCstr.setTable_name(hiveCstr.getTable_name());
            lsmCstr.setColumn_name(hiveCstr.getColumn_name());
            lsmCstr.setValidate_cstr(hiveCstr.isValidate_cstr());
            lsmCstr.setEnable_cstr(hiveCstr.isEnable_cstr());
            lsmCstr.setRely_cstr(hiveCstr.isRely_cstr());
            lsmCstr.setCstr_name(hiveCstr.getNn_name());
            lsmCstr.setCstr_info("");
            return lsmCstr;
        }).collect(Collectors.toList());
    }

    public static List<Constraint> toCheckConstraint(List<SQLCheckConstraint> checkConstraints) {
        return checkConstraints.stream().map(hiveCstr -> {
            Constraint lsmCstr = new Constraint();
            lsmCstr.setCstr_type(ConstraintType.CHECK_CSTR);
            lsmCstr.setCatName(hiveCstr.getCatName());
            lsmCstr.setDbName(hiveCstr.getTable_db());
            lsmCstr.setTable_name(hiveCstr.getTable_name());
            lsmCstr.setColumn_name(hiveCstr.getColumn_name());
            lsmCstr.setValidate_cstr(hiveCstr.isValidate_cstr());
            lsmCstr.setEnable_cstr(hiveCstr.isEnable_cstr());
            lsmCstr.setRely_cstr(hiveCstr.isRely_cstr());
            lsmCstr.setCstr_name(hiveCstr.getDc_name());
            lsmCstr.setCstr_info(hiveCstr.getCheck_expression());
            return lsmCstr;
        }).collect(Collectors.toList());
    }
    // constraint end

    public static String getProjectId() {
        return getProjectId(null);
    }

    public static String getProjectId(String defaultProjectId) {
        String projectId = defaultProjectId;
        try {
            String ugiInfo = UserGroupInformation.getCurrentUser().getUserName();
            String[] ugiInfos = ugiInfo.split("#");
            if (ugiInfos.length == 2) {
                projectId = ugiInfos[0];
            }
        } catch (IOException e) {
            log.warn("Cannot get ugi user, return the default projectId", e);
        }
        return projectId;
    }

    public static String getUserId(String owner, String defaultUserId) {
        String userId = getUserId(owner);
        if (userId == null) {
            return defaultUserId;
        }
        return userId;
    }

    public static String getUserId() {
        return getUserId(null);
    }

    public static String getUserId(String userId) {
        if (userId == null) {
            try {
                String userName = UserGroupInformation.getCurrentUser().getUserName();
                if (userName != null) {
                    return getUserId(userName);
                }
                return null;
            } catch (IOException e) {
                e.printStackTrace();
                log.warn("Get UGI user info error, {}", e.getMessage());
                return null;
            }
        }
        String[] split = userId.split("#");
        if (split.length == 2) {
            return split[1];
        } else {
            return userId;
        }
    }
}
