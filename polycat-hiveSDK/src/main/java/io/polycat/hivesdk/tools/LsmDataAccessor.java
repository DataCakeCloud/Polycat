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
package io.polycat.hivesdk.tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.Constraint;
import io.polycat.catalog.common.model.ConstraintType;
import io.polycat.catalog.common.model.ForeignKey;
import io.polycat.catalog.common.model.Order;
import io.polycat.catalog.common.model.PartitionAlterContext;
import io.polycat.catalog.common.model.PrimaryKey;
import io.polycat.catalog.common.model.SerDeInfo;
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
import io.polycat.catalog.common.plugin.CatalogContext;
import io.polycat.catalog.common.plugin.request.input.AddPartitionInput;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.EnvironmentContextInput;
import io.polycat.catalog.common.plugin.request.input.FileInput;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.common.plugin.request.input.FunctionResourceUri;
import io.polycat.catalog.common.plugin.request.input.TableInput;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.logging.log4j.util.Strings;
import org.apache.thrift.TException;

public class LsmDataAccessor {

    // columns statistic
    @FunctionalInterface
    interface ThrowingFunction<T, R> {

        R apply(T var1) throws TException;
    }

    static private final Map<String, ThrowingFunction<Object, Object>> statsMap = new HashMap<String, ThrowingFunction<Object, Object>>() {{
        put("binaryStats", LsmDataAccessor::convertToBinaryData);
        put("booleanStats", LsmDataAccessor::convertToBooleanData);
        put("dateStats", LsmDataAccessor::convertToDateData);
        put("decimalStats", LsmDataAccessor::convertToDecimalData);
        put("doubleStats", LsmDataAccessor::convertToDoubleData);
        put("longStats", LsmDataAccessor::convertToLongData);
        put("stringStats", LsmDataAccessor::convertToStringData);
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
        polyCatBinaryData.setBitVectors(hiveBinaryData.bufferForBitVectors());
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
        polyCatBooleanData.setBitVectors(hiveBooleanData.bufferForBitVectors());
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
        polyCatDateData.setBitVectors(hiveDateData.bufferForBitVectors());
        return polyCatDateData;
    }

    private static Object convertToDecimalData(Object statsData) throws TException {
        if (!(statsData instanceof org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData)) {
            throw new TException("transform illegal");
        }

        org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData hiveDecimalData =
            (org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData) statsData;
        DecimalColumnStatsData polyCatDecimalData = new DecimalColumnStatsData();
        polyCatDecimalData.setHighValue(
            new Decimal(hiveDecimalData.getHighValue().getScale(), hiveDecimalData.getHighValue().bufferForUnscaled()));
        polyCatDecimalData.setLowValue(
            new Decimal(hiveDecimalData.getLowValue().getScale(), hiveDecimalData.getLowValue().bufferForUnscaled()));
        polyCatDecimalData.setNumDVs(hiveDecimalData.getNumDVs());
        polyCatDecimalData.setNumNulls(hiveDecimalData.getNumNulls());
        polyCatDecimalData.setBitVectors(hiveDecimalData.bufferForBitVectors());
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
        polyCatDoubleData.setBitVectors(hiveDoubleData.bufferForBitVectors());
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
        polyCatLongData.setBitVectors(hiveLongData.bufferForBitVectors());
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
        polyCatStringData.setBitVectors(hiveStringData.bufferForBitVectors());
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
        List<ColumnStatisticsObj> lsmObjs = new ArrayList<>();
        for (org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj hiveObjs : hiveStats.getStatsObj()) {
            lsmObjs.add(toColumnStatisticsObj(hiveObjs));
        }

        return new ColumnStatistics(lsmDesc, lsmObjs);
    }
    // column statistic end

    public static DatabaseInput toDatabaseInput(Database database, CatalogContext context) {
        DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setDatabaseName(database.getName());
        databaseInput.setLocationUri(database.getLocationUri());
        databaseInput.setParameters(database.getParameters());
        databaseInput.setCatalogName(database.getCatalogName());
        databaseInput.setDescription(database.getDescription());
        databaseInput.setOwner(database.getOwnerName());
        databaseInput.setOwnerType(database.isSetOwnerType() ? database.getOwnerType().name() : null);
        databaseInput.setAccountId(context.getTenantName());
        databaseInput.setAuthSourceType(context.getAuthSourceType());
        return databaseInput;
    }

    public static CatalogInput toCatalogInput(Catalog catalog, CatalogContext context) {
        CatalogInput input = new CatalogInput();
        input.setOwner(context.getUserName());
        input.setDescription(catalog.getDescription());
        input.setCatalogName(catalog.getName());
        input.setLocation(catalog.getLocationUri());
        return input;
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
        return tblStorage;
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
        TableInput input = new TableInput();
        input.setCatalogName(table.getCatName());
        input.setDatabaseName(table.getDbName());
        input.setTableName(table.getTableName());
        input.setStorageDescriptor(convertToStorageDescriptor(table.getSd()));
        input.setAccountId(Strings.EMPTY);
        input.setAuthSourceType(Strings.EMPTY);
        input.setOwner(table.getOwner());
        input.setOwnerType(table.getOwnerType().name());
        input.setParameters(table.getParameters());
        input.setTableType(table.getTableType());
        input.setPartitionKeys(convertToColumns(table.getPartitionKeys()));
        input.setRetention(table.getRetention());
        input.setViewExpandedText(table.getViewExpandedText());
        input.setViewOriginalText(table.getViewOriginalText());
        input.setLastAccessTime((long) table.getLastAccessTime());
        input.setCreateTime((long) table.getCreateTime());
        return input;
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
    private static PartitionInput convertToPartitionBaseInputPartial(Partition hivePartition) {
        PartitionInput lsmPartition = new PartitionInput();
        lsmPartition.setFiles(new FileInput[0]);
        lsmPartition.setIndex(null);
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
        PartitionInput baseInput = convertToPartitionBaseInputPartial(partition);
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
            baseInputs[i] = convertToPartitionBaseInputPartial(partitions.get(i));
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
        FunctionInput lmsFunc = new FunctionInput(
            hiveFunc.getFunctionName(),
            hiveFunc.getClassName(),
            hiveFunc.getOwnerName(),
            hiveFunc.isSetOwnerType() ? hiveFunc.getOwnerType().name() : null,
            hiveFunc.isSetFunctionType() ? hiveFunc.getFunctionType().name() : null,
            hiveFunc.getCreateTime(),
            hiveFunc.getDbName(),
            hiveFunc.getCatName(),
            null
        );

        if (hiveFunc.isSetResourceUris()) {
            lmsFunc.setResourceUris(hiveFunc.getResourceUris().stream().map(
                lsmResource -> new FunctionResourceUri(lsmResource.getResourceType().name(), lsmResource.getUri())
            ).collect(Collectors.toList()));
        }

        return lmsFunc;
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
}
