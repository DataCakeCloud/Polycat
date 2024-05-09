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
package io.polycat.hiveService.data.accesser;


import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.Constraint;
import io.polycat.catalog.common.model.ConstraintType;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.ForeignKey;
import io.polycat.catalog.common.model.KerberosToken;
import io.polycat.catalog.common.model.Order;
import io.polycat.catalog.common.model.PrimaryKey;
import io.polycat.catalog.common.model.SerDeInfo;
import io.polycat.catalog.common.model.SkewedInfo;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableBrief;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.stats.AggrStatisticData;
import io.polycat.catalog.common.model.stats.BinaryColumnStatsData;
import io.polycat.catalog.common.model.stats.BooleanColumnStatsData;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import io.polycat.catalog.common.model.stats.DateColumnStatsData;
import io.polycat.catalog.common.model.stats.Decimal;
import io.polycat.catalog.common.model.stats.DecimalColumnStatsData;
import io.polycat.catalog.common.model.stats.DoubleColumnStatsData;
import io.polycat.catalog.common.model.stats.LongColumnStatsData;
import io.polycat.catalog.common.model.stats.StringColumnStatsData;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.common.plugin.request.input.FunctionResourceUri;
import io.polycat.catalog.common.utils.PartitionUtil;

import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.logging.log4j.util.Strings;


public class PolyCatDataAccessor {

    private static final String MOCK_CATALOG_ID = "hiveCatalogId";
    private static final String MOCK_PROJECT_ID = "hiveProjectId";
    private static final String MOCK_DATABASE_ID = "hiveDatabaseId";
    private static final String MOCK_CREATE_TIME = "hiveCreateTime";
    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final int MILLIS_IN_SECOND = 1000;

    static private final Map<String, Function<Object, Object>> statsMap = new HashMap<String, Function<Object, Object>>() {{
        put("binaryStats", PolyCatDataAccessor::convertToBinaryData);
        put("booleanStats", PolyCatDataAccessor::convertToBooleanData);
        put("dateStats", PolyCatDataAccessor::convertToDateData);
        put("decimalStats", PolyCatDataAccessor::convertToDecimalData);
        put("doubleStats", PolyCatDataAccessor::convertToDoubleData);
        put("longStats", PolyCatDataAccessor::convertToLongData);
        put("stringStats", PolyCatDataAccessor::convertToStringData);
    }};
    public static final String EMPTRY_STRING = "";
    public static final String MOCK_AUTH_SOURCE_TYPE = "mock_auth_source_type";
    public static final String MOCK_ACCOUNT = "mock_account";

    public static Catalog toCatalog(org.apache.hadoop.hive.metastore.api.Catalog hiveCatalog) {
        Catalog catalog = new Catalog();
        catalog.setCatalogName(hiveCatalog.getName());
        catalog.setDescription(hiveCatalog.getDescription());
        catalog.setLocation(hiveCatalog.getLocationUri());
        return catalog;
    }

    public static Catalog toCatalog(String CatalogName) {
        Catalog catalog = new Catalog();
        catalog.setCatalogName(CatalogName);
        return catalog;
    }


    public static Database toDatabase(org.apache.hadoop.hive.metastore.api.Database database) {
        Database polyCatDB = new Database();
        polyCatDB.setCatalogName(database.getCatalogName());
        polyCatDB.setDatabaseName(database.getName());
        polyCatDB.setDescription(database.getDescription());
        polyCatDB.setLocationUri(database.getLocationUri());
        polyCatDB.setOwner(database.getOwnerName());
        polyCatDB.setParameters(database.getParameters());
        polyCatDB.setOwnerType(convertOwnerType(database.getOwnerType()));
        polyCatDB.setAccountId(MOCK_ACCOUNT);
        polyCatDB.setAuthSourceType(MOCK_AUTH_SOURCE_TYPE);
        // privilege
        return polyCatDB;
    }

    private static String convertOwnerType(org.apache.hadoop.hive.metastore.api.PrincipalType ownerType) {
        return ownerType == null ? null : ownerType.name();
    }

    public static Table toTable(org.apache.hadoop.hive.metastore.api.Table table) {
        Table polyCatTable = new Table();
        polyCatTable.setTableName(table.getTableName());
        polyCatTable.setDatabaseName(table.getDbName());
        polyCatTable.setCatalogName(table.getCatName());
        polyCatTable.setPartitionKeys(convertToColumnOutput(table.getPartitionKeys()));
        polyCatTable.setTableType(table.getTableType());
        if (table.isSetSd()) {
            polyCatTable.setStorageDescriptor(convertToStorageDescriptor(table.getSd()));
        }
        // tableStats
        polyCatTable.setCreateTime((long) table.getCreateTime());
        polyCatTable.setLastAccessTime((long) table.getLastAccessTime());
        polyCatTable.setAccountId(Strings.EMPTY);
        polyCatTable.setAuthSourceType(Strings.EMPTY);
        polyCatTable.setOwner(table.getOwner());
        polyCatTable.setOwnerType(table.getOwnerType().name());
        polyCatTable.setLmsMvcc(false);
        polyCatTable.setRetention((long) table.getRetention());
        polyCatTable.setViewOriginalText(table.getViewOriginalText());
        polyCatTable.setViewExpandedText(table.getViewExpandedText());
        Map<String, String> params = new HashMap<>(table.getParameters());
        polyCatTable.setParameters(params);
        return polyCatTable;
    }

    private static void fillProperties(Map<String, String> parameters, Map<String, String> properties) {
        parameters.keySet().forEach(key -> properties.put(key, parameters.get(key)));
    }

    private static String convertToDateStr(int createTime) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        return dateFormat.format((long) createTime * MILLIS_IN_SECOND);
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
        tblStorage.setColumns(convertToColumnOutput(sd.getCols()));
        tblStorage.setCompressed(sd.isCompressed());
        tblStorage.setNumberOfBuckets(sd.getNumBuckets());
        tblStorage.setBucketColumns(sd.getBucketCols());
        tblStorage.setSerdeInfo(fillSerDeInfo(sd.getSerdeInfo()));
        tblStorage.setSortColumns(fillOrderList(sd.getSortCols()));
        tblStorage.setSkewedInfo(convertToSkewedInfo(sd.getSkewedInfo()));
        tblStorage.setStoredAsSubDirectories(sd.isStoredAsSubDirectories());
        return tblStorage;
    }

    private static SkewedInfo convertToSkewedInfo(org.apache.hadoop.hive.metastore.api.SkewedInfo hiveSkewedInfo) {
        if (hiveSkewedInfo == null) {
            return null;
        }
        SkewedInfo lmsSkewedInfo = new SkewedInfo();
        lmsSkewedInfo.setSkewedColumnNames(hiveSkewedInfo.getSkewedColNames());
        lmsSkewedInfo.setSkewedColumnValues(hiveSkewedInfo.getSkewedColValues());
        List<List<String>> mapKeys = new ArrayList<>(hiveSkewedInfo.getSkewedColValueLocationMapsSize());
        List<String> mapValues = new ArrayList<>(hiveSkewedInfo.getSkewedColValueLocationMapsSize());
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

    private static List<Order> fillOrderList(List<org.apache.hadoop.hive.metastore.api.Order> sortCols) {
        if (sortCols == null) {
            return null;
        }
        return sortCols.stream()
            .map(hiveSortCol -> new Order(hiveSortCol.getCol(), hiveSortCol.getOrder()))
            .collect(Collectors.toList());
    }

    private static SerDeInfo fillSerDeInfo(org.apache.hadoop.hive.metastore.api.SerDeInfo hiveSerDe) {
        if (hiveSerDe == null) {
            return null;
        }
        SerDeInfo lmsSerDe = new SerDeInfo();
        lmsSerDe.setName(hiveSerDe.getName());
        lmsSerDe.setSerializationLibrary(hiveSerDe.getSerializationLib());
        lmsSerDe.setParameters(hiveSerDe.getParameters());
        return lmsSerDe;
    }

    private static List<Column> convertToColumnOutput(List<FieldSchema> schemas) {
        return schemas.stream().map(x -> {
            Column column = new Column(x.getName(), x.getType());
            column.setComment(x.getComment());
            return column;
        }).collect(Collectors.toList());
    }

    public static ColumnStatisticsObj toColumnStatisticsObj(
        org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj hiveStats) {
        ColumnStatisticsObj polyCatStatsObj = new ColumnStatisticsObj();
        polyCatStatsObj.setColName(hiveStats.getColName());
        polyCatStatsObj.setColType(hiveStats.getColType());
        polyCatStatsObj.setDataType(hiveStats.getStatsData().getSetField().getFieldName());
        polyCatStatsObj.setDataValue(statsMap.get(hiveStats.getStatsData().getSetField().getFieldName())
            .apply(hiveStats.getStatsData().getFieldValue()));
        return polyCatStatsObj;
    }


    private static Object convertToBinaryData(Object statsData) {
        if (!(statsData instanceof org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData)) {
            throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR);
        }

        org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData hiveBinaryData =
            (org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData) statsData;
        BinaryColumnStatsData polyCatBinaryData = new BinaryColumnStatsData();
        polyCatBinaryData.setMaxColLen(hiveBinaryData.getMaxColLen());
        polyCatBinaryData.setAvgColLen(hiveBinaryData.getAvgColLen());
        polyCatBinaryData.setNumNulls(hiveBinaryData.getNumNulls());
        polyCatBinaryData.setBitVectors(getBitVerctor(hiveBinaryData.bufferForBitVectors()));
        return polyCatBinaryData;
    }

    private static Object convertToBooleanData(Object statsData) {
        if (!(statsData instanceof org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData)) {
            throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR);
        }

        org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData hiveBooleanData =
            (org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData) statsData;
        BooleanColumnStatsData polyCatBooleanData = new BooleanColumnStatsData();
        polyCatBooleanData.setNumNulls(hiveBooleanData.getNumNulls());
        polyCatBooleanData.setNumTrues(hiveBooleanData.getNumTrues());
        polyCatBooleanData.setNumFalses(hiveBooleanData.getNumFalses());
        polyCatBooleanData.setBitVectors(getBitVerctor(hiveBooleanData.bufferForBitVectors()));
        return polyCatBooleanData;
    }

    private static byte[] getBitVerctor(ByteBuffer bitVectors) {
        return bitVectors == null ? null : bitVectors.array();
    }


    private static Object convertToDateData(Object statsData) {
        if (!(statsData instanceof org.apache.hadoop.hive.metastore.api.DateColumnStatsData)) {
            throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR);
        }

        org.apache.hadoop.hive.metastore.api.DateColumnStatsData hiveDateData =
            (org.apache.hadoop.hive.metastore.api.DateColumnStatsData) statsData;
        DateColumnStatsData polyCatDateData = new DateColumnStatsData();
        polyCatDateData.setHighValue(hiveDateData.getHighValue().getDaysSinceEpoch());
        polyCatDateData.setLowValue(hiveDateData.getLowValue().getDaysSinceEpoch());
        polyCatDateData.setNumDVs(hiveDateData.getNumDVs());
        polyCatDateData.setNumNulls(hiveDateData.getNumNulls());
        polyCatDateData.setBitVectors(getBitVerctor(hiveDateData.bufferForBitVectors()));
        return polyCatDateData;
    }

    private static Object convertToDecimalData(Object statsData) {
        if (!(statsData instanceof org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData)) {
            throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR);
        }

        org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData hiveDecimalData =
            (org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData) statsData;
        DecimalColumnStatsData polyCatDecimalData = new DecimalColumnStatsData();
        polyCatDecimalData.setHighValue(
            new Decimal(hiveDecimalData.getHighValue().getScale(), hiveDecimalData.getHighValue().getUnscaled()));
        polyCatDecimalData.setLowValue(
            new Decimal(hiveDecimalData.getLowValue().getScale(), hiveDecimalData.getLowValue().getUnscaled()));
        polyCatDecimalData.setNumDVs(hiveDecimalData.getNumDVs());
        polyCatDecimalData.setNumNulls(hiveDecimalData.getNumNulls());
        polyCatDecimalData.setBitVectors(getBitVerctor(hiveDecimalData.bufferForBitVectors()));
        return polyCatDecimalData;
    }

    private static Object convertToDoubleData(Object statsData) {
        if (!(statsData instanceof org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData)) {
            throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR);
        }

        org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData hiveDoubleData =
            (org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData) statsData;
        DoubleColumnStatsData polyCatDoubleData = new DoubleColumnStatsData();
        polyCatDoubleData.setHighValue(hiveDoubleData.getHighValue());
        polyCatDoubleData.setLowValue(hiveDoubleData.getLowValue());
        polyCatDoubleData.setNumDVs(hiveDoubleData.getNumDVs());
        polyCatDoubleData.setNumNulls(hiveDoubleData.getNumNulls());
        polyCatDoubleData.setBitVectors(getBitVerctor(hiveDoubleData.bufferForBitVectors()));
        return polyCatDoubleData;
    }

    private static Object convertToLongData(Object statsData) {
        if (!(statsData instanceof org.apache.hadoop.hive.metastore.api.LongColumnStatsData)) {
            throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR);
        }

        org.apache.hadoop.hive.metastore.api.LongColumnStatsData hiveLongData =
            (org.apache.hadoop.hive.metastore.api.LongColumnStatsData) statsData;
        LongColumnStatsData polyCatLongData = new LongColumnStatsData();
        polyCatLongData.setHighValue(hiveLongData.getHighValue());
        polyCatLongData.setLowValue(hiveLongData.getLowValue());
        polyCatLongData.setNumNulls(hiveLongData.getNumNulls());
        polyCatLongData.setNumDVs(hiveLongData.getNumDVs());
        polyCatLongData.setBitVectors(getBitVerctor(hiveLongData.bufferForBitVectors()));
        return polyCatLongData;
    }

    private static Object convertToStringData(Object statsData) {
        if (!(statsData instanceof org.apache.hadoop.hive.metastore.api.StringColumnStatsData)) {
            throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR);
        }

        org.apache.hadoop.hive.metastore.api.StringColumnStatsData hiveStringData =
            (org.apache.hadoop.hive.metastore.api.StringColumnStatsData) statsData;
        StringColumnStatsData polyCatStringData = new StringColumnStatsData();
        polyCatStringData.setAvgColLen(hiveStringData.getAvgColLen());
        polyCatStringData.setMaxColLen(hiveStringData.getMaxColLen());
        polyCatStringData.setNumNulls(hiveStringData.getNumNulls());
        polyCatStringData.setNumDVs(hiveStringData.getNumDVs());
        polyCatStringData.setBitVectors(getBitVerctor(hiveStringData.bufferForBitVectors()));
        return polyCatStringData;
    }

    public static FunctionInput toFunctionBase(org.apache.hadoop.hive.metastore.api.Function hiveFunc) {
        FunctionInput lmsFunc = new FunctionInput();
        lmsFunc.setCatalogName(hiveFunc.getCatName());
        lmsFunc.setDatabaseName(hiveFunc.getDbName());
        lmsFunc.setClassName(hiveFunc.getClassName());
        lmsFunc.setFunctionName(hiveFunc.getFunctionName());
        lmsFunc.setOwner(hiveFunc.getOwnerName());
        lmsFunc.setOwnerType(hiveFunc.getOwnerType().name());
        lmsFunc.setCreateTime(hiveFunc.getCreateTime());
        lmsFunc.setResourceUris(hiveFunc.getResourceUris().stream().map(
            lsmResource -> new FunctionResourceUri(lsmResource.getResourceType().name(), lsmResource.getUri())
        ).collect(Collectors.toList()));

        return lmsFunc;
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
            lsmCstr.setCstr_info(EMPTRY_STRING);
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

    public static List<ForeignKey> toForeignKeys(List<SQLForeignKey> foreignKeys) {
        return foreignKeys.stream().map(hiveFKey -> {
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
        }).collect(Collectors.toList());

    }

    public static List<PrimaryKey> toPrimaryKeys(List<SQLPrimaryKey> primaryKeys) {
        return primaryKeys.stream().map(hivePKey -> {
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
        }).collect(Collectors.toList());
    }

    public static Partition toPartition(org.apache.hadoop.hive.metastore.api.Partition hivePartition) {
        Partition lsmPartition = new Partition();
        lsmPartition.setFileIndexUrl(null);
        lsmPartition.setStorageDescriptor(convertToStorageDescriptor(hivePartition.getSd()));
        lsmPartition.setParameters(hivePartition.getParameters());
        lsmPartition.setCreateTime((long) hivePartition.getCreateTime());
        lsmPartition.setLastAccessTime((long) hivePartition.getLastAccessTime());
        lsmPartition.setPartitionValues(hivePartition.getValues());
        lsmPartition.setCatalogName(hivePartition.getCatName());
        lsmPartition.setDatabaseName(hivePartition.getDbName());
        lsmPartition.setTableName(hivePartition.getTableName());

        return lsmPartition;
    }

    private static String convertToPartitionName(org.apache.hadoop.hive.metastore.api.Partition hivePartition,
        org.apache.hadoop.hive.metastore.api.Table table) {
        List<String> partitionKeys = table.getPartitionKeys().stream().map(FieldSchema::getName)
            .collect(Collectors.toList());
        return PartitionUtil.makePartitionName(partitionKeys, hivePartition.getValues());
    }

    private static List<Column> convertToTableColumns(List<FieldSchema> cols) {
        if (cols == null) {
            return Collections.emptyList();
        }
        return cols.stream().map(col -> new Column(col.getName(), col.getType(), col.getComment()))
            .collect(Collectors.toList());
    }

    public static AggrStatisticData toAggrStatisticResult(AggrStats hiveAggrStat) {
        AggrStatisticData lsmAggrStat = new AggrStatisticData();
        lsmAggrStat.setPartsFound(hiveAggrStat.getPartsFound());
        lsmAggrStat.setColumnStatistics(
            hiveAggrStat.getColStats().stream().map(PolyCatDataAccessor::toColumnStatisticsObj).collect(
                Collectors.toList()));
        return lsmAggrStat;
    }

    public static Catalog toCatalogRecord(org.apache.hadoop.hive.metastore.api.Catalog hiveCatalog) {
        Catalog catalog = new Catalog();
        catalog.setCreateTime(System.currentTimeMillis() / 1000);
        catalog.setCatalogName(hiveCatalog.getName());
        catalog.setDescription(hiveCatalog.getDescription());
        catalog.setLocation(hiveCatalog.getLocationUri());

        return catalog;
    }

    public static KerberosToken toMRSToken(String tokenId, String token) {
        KerberosToken lsmToken = new KerberosToken();
        lsmToken.setMRSToken(tokenId, token);
        return lsmToken;
    }

    public static KerberosToken toMRSToken(String token) {
        return toMRSToken("", token);
    }

    public static KerberosToken toMRSToken(long retention) {
        KerberosToken lsmToken = new KerberosToken();
        lsmToken.setRetention(retention);
        return lsmToken;
    }

    public static TableBrief toTableBrief(String projectId, TableMeta tableMeta) {
        return new TableBrief(tableMeta.getCatName(),
            tableMeta.getDbName(), tableMeta.getTableName(), tableMeta.getTableType(), tableMeta.getComments());
    }

}
