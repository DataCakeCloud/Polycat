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
package io.polycat.hivesdk.hive2.tools;

import com.google.common.collect.Lists;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.ForeignKey;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.PrimaryKey;
import io.polycat.catalog.common.model.TableBrief;
import io.polycat.catalog.common.model.stats.AggrStatisticData;
import io.polycat.catalog.common.model.stats.PartitionStatisticData;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.common.utils.DataTypeUtil;
import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;

public class HiveDataAccessor {

    public static final String CASCADE = "CASCADE";
    public static final String TRUE = "true";
    // column statistic

    static Map<String, Function<Object, ColumnStatisticsData>> statsMap = new HashMap<String, Function<Object, ColumnStatisticsData>>() {{
        put("binaryStats", HiveDataAccessor::convertToHiveBinaryData);
        put("booleanStats", HiveDataAccessor::convertToHiveBooleanData);
        put("dateStats", HiveDataAccessor::convertToHiveDateData);
        put("decimalStats", HiveDataAccessor::convertToHiveDecimalData);
        put("doubleStats", HiveDataAccessor::convertToHiveDoubleData);
        put("longStats", HiveDataAccessor::convertToHiveLongData);
        put("stringStats", HiveDataAccessor::convertToHiveStringData);
    }};

    public static ColumnStatisticsObj convertToHiveStatsObj(
        io.polycat.catalog.common.model.stats.ColumnStatisticsObj polyCatObj) {
        return new ColumnStatisticsObj(polyCatObj.getColName(), polyCatObj.getColType(),
            statsMap.get(polyCatObj.getDataType()).apply(polyCatObj.getDataValue()));
    }

    // convert statistics data
    private static ColumnStatisticsData convertToHiveBinaryData(Object dataValue) {
        ColumnStatisticsData hiveStats = new ColumnStatisticsData();
        hiveStats.setBinaryStats(convertToHiveBinaryStatsInner(dataValue));
        return hiveStats;
    }

    private static BinaryColumnStatsData convertToHiveBinaryStatsInner(Object dataValue) {
        io.polycat.catalog.common.model.stats.BinaryColumnStatsData polyCatBinaryData =
            mapToStruct((Map<String, ? extends Object>) dataValue,
                io.polycat.catalog.common.model.stats.BinaryColumnStatsData.class);

        BinaryColumnStatsData binaryData = new BinaryColumnStatsData(polyCatBinaryData.getMaxColLen(),
            polyCatBinaryData.getAvgColLen(), polyCatBinaryData.getNumNulls());
        binaryData.setBitVectors(DataTypeUtil.getBitVectors(polyCatBinaryData.getBitVectors()));
        return binaryData;
    }

    private static ColumnStatisticsData convertToHiveBooleanData(Object dataValue) {
        ColumnStatisticsData hiveStats = new ColumnStatisticsData();
        hiveStats.setBooleanStats(convertToHiveBooleanStatsInner(dataValue));
        return hiveStats;
    }

    private static BooleanColumnStatsData convertToHiveBooleanStatsInner(Object dataValue) {
        io.polycat.catalog.common.model.stats.BooleanColumnStatsData polyCatBooleanData =
            mapToStruct((Map<String, ? extends Object>) dataValue,
                io.polycat.catalog.common.model.stats.BooleanColumnStatsData.class);

        BooleanColumnStatsData booleanData = new BooleanColumnStatsData(polyCatBooleanData.getNumTrues(),
            polyCatBooleanData.getNumFalses(), polyCatBooleanData.getNumNulls());
        booleanData.setBitVectors(DataTypeUtil.getBitVectors(polyCatBooleanData.getBitVectors()));
        return booleanData;
    }

    private static ColumnStatisticsData convertToHiveDateData(Object dataValue) {
        ColumnStatisticsData hiveStats = new ColumnStatisticsData();
        hiveStats.setDateStats(convertToHiveDateStatsInner(dataValue));
        return hiveStats;
    }

    private static DateColumnStatsData convertToHiveDateStatsInner(Object dataValue) {
        io.polycat.catalog.common.model.stats.DateColumnStatsData polyCatDateData =
            mapToStruct((Map<String, ? extends Object>) dataValue,
                io.polycat.catalog.common.model.stats.DateColumnStatsData.class);

        DateColumnStatsData dateData = new DateColumnStatsData(polyCatDateData.getNumNulls(),
            polyCatDateData.getNumDVs());
        dateData.setHighValue(new Date(polyCatDateData.getHighValue()));
        dateData.setLowValue(new Date(polyCatDateData.getLowValue()));
        dateData.setBitVectors(DataTypeUtil.getBitVectors(polyCatDateData.getBitVectors()));
        return dateData;
    }

    private static ColumnStatisticsData convertToHiveDecimalData(Object dataValue) {
        ColumnStatisticsData hiveStats = new ColumnStatisticsData();
        hiveStats.setDecimalStats(convertToHiveDecimalStatsInner(dataValue));
        return hiveStats;
    }

    private static DecimalColumnStatsData convertToHiveDecimalStatsInner(Object dataValue) {
        io.polycat.catalog.common.model.stats.DecimalColumnStatsData polyCatDecimalData =
                DataTypeUtil.mapToColumnStatsData(dataValue,
                io.polycat.catalog.common.model.stats.DecimalColumnStatsData.class);
        DecimalColumnStatsData decimalData = new DecimalColumnStatsData(polyCatDecimalData.getNumNulls(),
            polyCatDecimalData.getNumDVs());
        decimalData.setHighValue(
            new Decimal(ByteBuffer.wrap(polyCatDecimalData.getHighValue().getUnscaled()), polyCatDecimalData.getHighValue().getScale()));
        decimalData.setLowValue(
            new Decimal(ByteBuffer.wrap(polyCatDecimalData.getLowValue().getUnscaled()), polyCatDecimalData.getLowValue().getScale()));
        decimalData.setBitVectors(DataTypeUtil.getBitVectors(polyCatDecimalData.getBitVectors()));
        return decimalData;
    }

    private static ColumnStatisticsData convertToHiveDoubleData(Object dataValue) {
        ColumnStatisticsData hiveStats = new ColumnStatisticsData();
        hiveStats.setDoubleStats(convertToHiveDoubleStatsInner(dataValue));
        return hiveStats;
    }

    private static DoubleColumnStatsData convertToHiveDoubleStatsInner(Object dataValue) {
        io.polycat.catalog.common.model.stats.DoubleColumnStatsData polyCatDoubleData =
            mapToStruct((Map<String, ? extends Object>) dataValue,
                io.polycat.catalog.common.model.stats.DoubleColumnStatsData.class);

        DoubleColumnStatsData doubleData = new DoubleColumnStatsData(polyCatDoubleData.getNumNulls(),
            polyCatDoubleData.getNumDVs());
        doubleData.setHighValue(polyCatDoubleData.getHighValue());
        doubleData.setLowValue(polyCatDoubleData.getLowValue());
        doubleData.setBitVectors(DataTypeUtil.getBitVectors(polyCatDoubleData.getBitVectors()));
        return doubleData;
    }

    private static ColumnStatisticsData convertToHiveLongData(Object dataValue) {
        ColumnStatisticsData hiveStats = new ColumnStatisticsData();
        hiveStats.setLongStats(convertToHiveLongStatsInner(dataValue));
        return hiveStats;
    }

    private static LongColumnStatsData convertToHiveLongStatsInner(Object dataValue) {
        io.polycat.catalog.common.model.stats.LongColumnStatsData polyCatLongData =
            mapToStruct((Map<String, ? extends Object>) dataValue,
                io.polycat.catalog.common.model.stats.LongColumnStatsData.class);

        LongColumnStatsData longData = new LongColumnStatsData(polyCatLongData.getNumNulls(),
            polyCatLongData.getNumDVs());
        longData.setBitVectors(DataTypeUtil.getBitVectors(polyCatLongData.getBitVectors()));
        longData.setLowValue(polyCatLongData.getLowValue());
        longData.setHighValue(polyCatLongData.getHighValue());
        return longData;
    }

    private static ColumnStatisticsData convertToHiveStringData(Object dataValue) {
        ColumnStatisticsData hiveStats = new ColumnStatisticsData();
        hiveStats.setStringStats(convertToHiveStringStatsInner(dataValue));
        return hiveStats;
    }

    private static StringColumnStatsData convertToHiveStringStatsInner(Object dataValue) {
        io.polycat.catalog.common.model.stats.StringColumnStatsData polyCatStringData =
            mapToStruct((Map<String, ? extends Object>) dataValue,
                io.polycat.catalog.common.model.stats.StringColumnStatsData.class);

        StringColumnStatsData stringData = new StringColumnStatsData(polyCatStringData.getMaxColLen(),
            polyCatStringData.getAvgColLen(), polyCatStringData.getNumNulls(), polyCatStringData.getNumDVs());
        stringData.setBitVectors(DataTypeUtil.getBitVectors(polyCatStringData.getBitVectors()));
        return stringData;
    }

    private static <T> T mapToStruct(Map<String, ?> dataValue, Class<T> targetClass) {
        try {
            T target = targetClass.newInstance();
            BeanUtils.populate(target, dataValue);
            return target;
        } catch (Exception e) {
            throw new CatalogException("transform illegal");
        }
    }

    // column statistic end

    public static org.apache.hadoop.hive.metastore.api.Database toDatabase(Database polyCatDatabase) {
        org.apache.hadoop.hive.metastore.api.Database database = new org.apache.hadoop.hive.metastore.api.Database();
        database.setDescription(polyCatDatabase.getDescription());
        database.setLocationUri(polyCatDatabase.getLocationUri());
        database.setName(polyCatDatabase.getDatabaseName());
        database.setParameters(polyCatDatabase.getParameters());
        database.setOwnerName(polyCatDatabase.getOwner());
        database.setOwnerType(getOwnerType(polyCatDatabase.getOwnerType()));
        return database;
    }

    public static Table toTable(io.polycat.catalog.common.model.Table lsmTbl) {
        Table table = new Table();
        table.setDbName(lsmTbl.getDatabaseName());
        table.setTableName(lsmTbl.getTableName());
        table.setOwner(lsmTbl.getOwner());
        table.setRetention(lsmTbl.getRetention().intValue());
        table.setSd(convertToStorageDescriptor(lsmTbl.getStorageDescriptor()));
        table.setPartitionKeys(convertToColumns(lsmTbl.getPartitionKeys()));
        table.setParameters(lsmTbl.getParameters());
        table.setViewOriginalText(lsmTbl.getViewOriginalText());
        table.setViewExpandedText(lsmTbl.getViewExpandedText());
        table.setTableType(lsmTbl.getTableType());
        table.setCreateTime(toInteger(lsmTbl.getCreateTime()));
        table.setLastAccessTime(toInteger(lsmTbl.getLastAccessTime()));
        // privileges
        return table;
    }

    private static PrincipalType getOwnerType(String ownerType) {
        if (StringUtils.isNotEmpty(ownerType)) {
            try {
                return PrincipalType.valueOf(ownerType);
            } catch (Exception e) {
                return PrincipalType.USER;
            }
        }
        return PrincipalType.USER;
    }

    private static int toInteger(Long longNum) {
        if (longNum == null) {
            return 0;
        }
        return longNum.intValue();
    }

    private static StorageDescriptor convertToStorageDescriptor(
        io.polycat.catalog.common.model.StorageDescriptor lmsSd) {
        if (Objects.nonNull(lmsSd)) {
            StorageDescriptor hiveSd = new StorageDescriptor();
            hiveSd.setCols(convertToColumns(lmsSd.getColumns()));
            hiveSd.setLocation(lmsSd.getLocation());
            hiveSd.setInputFormat(lmsSd.getInputFormat());
            hiveSd.setOutputFormat(lmsSd.getOutputFormat());
            hiveSd.setCompressed(lmsSd.getCompressed());
            hiveSd.setNumBuckets(lmsSd.getNumberOfBuckets());
            hiveSd.setSerdeInfo(convertToSerdeInfo(lmsSd.getSerdeInfo()));
            hiveSd.setBucketCols(lmsSd.getBucketColumns());
            hiveSd.setSortCols(convertToSortColumns(lmsSd.getSortColumns()));
            hiveSd.setParameters(lmsSd.getParameters());
            hiveSd.setSkewedInfo(convertToSkewInfo(lmsSd.getSkewedInfo()));
            hiveSd.setStoredAsSubDirectories(lmsSd.getStoredAsSubDirectories());
            return hiveSd;
        }
        return null;
    }

    private static SkewedInfo convertToSkewInfo(io.polycat.catalog.common.model.SkewedInfo lmsSkewInfo) {
        if (Objects.nonNull(lmsSkewInfo)) {
            return new SkewedInfo(lmsSkewInfo.getSkewedColumnNames()
                , lmsSkewInfo.getSkewedColumnValues(), getSkewedValueLocationMap(lmsSkewInfo));
        }
        return null;
    }

    private static Map<List<String>, String> getSkewedValueLocationMap(
        io.polycat.catalog.common.model.SkewedInfo lmsSkewInfo) {
        Map<List<String>, String> hiveMap = new HashMap<>();
        Map<String, String> lsmSkewMap = lmsSkewInfo.getSkewedColumnValueLocationMaps();
        lsmSkewMap.keySet().forEach(valStr -> hiveMap.put(convertToValueList(valStr), lsmSkewMap.get(valStr)));
        return hiveMap;
    }

    public static List<String> convertToValueList(final String valStr) {
        boolean isValid = false;
        if (valStr == null) {
            return null;
        }
        List<String> valueList = new ArrayList<>();
        try {
            for (int i = 0; i < valStr.length(); ) {
                StringBuilder length = new StringBuilder();
                for (int j = i; j < valStr.length(); j++) {
                    if (valStr.charAt(j) != '$') {
                        length.append(valStr.charAt(j));
                        i++;
                        isValid = false;
                    } else {
                        int lengthOfString = Integer.parseInt(length.toString());
                        System.out.println(lengthOfString);
                        valueList.add(valStr.substring(j + 1, j + 1 + lengthOfString));
                        i = j + 1 + lengthOfString;
                        isValid = true;
                        break;
                    }
                }
            }
        } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
            isValid = false;
        }

        if (!isValid) {
            throw new InvalidParameterException(String.format("str %s is illegal", valStr));
        }
        return valueList;
    }

    private static List<Order> convertToSortColumns(List<io.polycat.catalog.common.model.Order> lmsSortCols) {
        if (Objects.nonNull(lmsSortCols)) {
            return lmsSortCols.stream()
                .map(lmsCol -> new Order(lmsCol.getColumn(), lmsCol.getSortOrder()))
                .collect(Collectors.toList());
        }
        return null;
    }

    private static SerDeInfo convertToSerdeInfo(io.polycat.catalog.common.model.SerDeInfo lmsSerDe) {
        if (Objects.nonNull(lmsSerDe)) {
            SerDeInfo serde = new SerDeInfo();
            serde.setName(lmsSerDe.getName());
            serde.setSerializationLib(lmsSerDe.getSerializationLibrary());
            serde.setParameters(lmsSerDe.getParameters());
            return serde;
        }
        return null;
    }

    private static List<FieldSchema> convertToColumns(List<Column> columns) {
        if (Objects.isNull(columns)) {
            return null;
        }
        return columns.stream()
            .map(column -> new FieldSchema(column.getColumnName(), column.getColType(), column.getComment()))
            .collect(Collectors.toList());
    }


    public static TableMeta toTableMeta(TableBrief lsmTblMeta) {
        TableMeta hiveTblMeta = new TableMeta(lsmTblMeta.getDatabaseName(), lsmTblMeta.getTableName(),
            lsmTblMeta.getTableType());
        return hiveTblMeta;
    }

    public static ColumnStatisticsObj toColumnStatistics(
        io.polycat.catalog.common.model.stats.ColumnStatisticsObj polyCatObj) {
        return new ColumnStatisticsObj(polyCatObj.getColName(), polyCatObj.getColType(),
            statsMap.get(polyCatObj.getDataType()).apply(polyCatObj.getDataValue()));
    }

    // partition
    public static org.apache.hadoop.hive.metastore.api.Partition toPartition(Partition partition) {
        org.apache.hadoop.hive.metastore.api.Partition hivePartition = new org.apache.hadoop.hive.metastore.api.Partition(
            partition.getPartitionValues(),
            partition.getDatabaseName(),
            partition.getTableName(),
            partition.getCreateTime() == null ? 0 : partition.getCreateTime().intValue(),
            partition.getLastAccessTime() == null ? 0: partition.getLastAccessTime().intValue(),
            convertToStorageDescriptor(partition.getStorageDescriptor()),
            partition.getParameters());
        return hivePartition;
    }

    public static List<org.apache.hadoop.hive.metastore.api.Partition> toPartitionList(List<Partition> partitions) {
        if (partitions != null) {
            return partitions.stream().map(HiveDataAccessor::toPartition).collect(Collectors.toList());
        }
        return null;
    }

    public static org.apache.hadoop.hive.metastore.api.Partition toPartition(Partition partition, String catName,
        String dbName, String tblName) {

        List<String> values = partition.getPartitionValues();
        StorageDescriptor partitionSd = new StorageDescriptor();
        partitionSd.setParameters(partition.getParameters());
        partitionSd.setCols(convertToColumns(partition.getStorageDescriptor().getColumns()));
        partitionSd.setLocation(partition.getStorageDescriptor().getLocation());
        partitionSd.setInputFormat(partition.getStorageDescriptor().getInputFormat());
        partitionSd.setOutputFormat(partition.getStorageDescriptor().getOutputFormat());
        // empty serdeinfo
        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setParameters(convertToParameters(partition.getStorageDescriptor().getSerdeInfo().getParameters()));
        serDeInfo.setSerializationLib(partition.getStorageDescriptor().getSerdeInfo().getSerializationLibrary());
        partitionSd.setSerdeInfo(serDeInfo);
        org.apache.hadoop.hive.metastore.api.Partition hivePartition = new org.apache.hadoop.hive.metastore.api.Partition(
            values, dbName, tblName, partition.getCreateTime().intValue(),
            partition.getLastAccessTime().intValue(), partitionSd, partition.getParameters());
        return hivePartition;
    }

    private static Map<String, String> convertToParameters(Map<String, String> serdeParameters) {
        return serdeParameters == null ? Collections.emptyMap() : serdeParameters;
    }

    public static AggrStats toAggrStats(AggrStatisticData lsmStats) {
        AggrStats hiveStats = new AggrStats();
        hiveStats.setPartsFound(lsmStats.getPartsFound());
        hiveStats.setColStats(lsmStats.getColumnStatistics().stream().map(HiveDataAccessor::toColumnStatistics).collect(
            Collectors.toList()));
        return hiveStats;
    }

    public static Map<String, List<ColumnStatisticsObj>> toColumnStatisticsMap(
        Map<String, List<io.polycat.catalog.common.model.stats.ColumnStatisticsObj>> lsmMap) {
        Map<String, List<ColumnStatisticsObj>> hiveMap = new HashMap<>(lsmMap.size());

        lsmMap.keySet().forEach(partName -> {
            List<ColumnStatisticsObj> hiveColStatsList =
                lsmMap.get(partName).stream().map(HiveDataAccessor::toColumnStatistics).collect(Collectors.toList());
            hiveMap.put(partName, hiveColStatsList);
        });
        return hiveMap;
    }

    public static org.apache.hadoop.hive.metastore.api.Function toFunction(FunctionInput lsmFunc) {
        org.apache.hadoop.hive.metastore.api.Function hiveFunc = new org.apache.hadoop.hive.metastore.api.Function(
            lsmFunc.getFunctionName(),
            lsmFunc.getDatabaseName(),
            lsmFunc.getClassName(),
            lsmFunc.getOwner(),
            getOwnerType(lsmFunc.getOwnerType()),
            (int) lsmFunc.getCreateTime(),
            convertToFunctionType(lsmFunc.getFuncType()),
            null
        );

        if (lsmFunc.getResourceUris() != null) {
            hiveFunc.setResourceUris(lsmFunc.getResourceUris().stream().map(
                lsmResource -> new ResourceUri(ResourceType.valueOf(lsmResource.getType().name()),
                    lsmResource.getUri())
            ).collect(Collectors.toList()));
        }

        return hiveFunc;
    }

    private static FunctionType convertToFunctionType(String funcType) {
        return funcType == null ? FunctionType.JAVA : FunctionType.valueOf(funcType);
    }

    public static SQLPrimaryKey toSQLPrimaryKey(PrimaryKey lsmPKey) {
        SQLPrimaryKey hivePKey = new SQLPrimaryKey(
            lsmPKey.getDbName(),
            lsmPKey.getTableName(),
            lsmPKey.getColumnName(),
            lsmPKey.getKeySeq(),
            lsmPKey.getPkName(),
            lsmPKey.isEnable_cstr(),
            lsmPKey.isValidate_cstr(),
            lsmPKey.isRely_cstr());
        return hivePKey;
    }

    public static SQLForeignKey toSQLForgeinKey(ForeignKey lsmFKey) {
        SQLForeignKey hiveFKey = new SQLForeignKey(lsmFKey.getPkTableDb(), lsmFKey.getPkTableName(),
            lsmFKey.getPkColumnName(), lsmFKey.getFkTableDb(), lsmFKey.getFkTableName(),
            lsmFKey.getFkColumnName(), lsmFKey.getKeySeq(), lsmFKey.getUpdateRule(), lsmFKey.getDeleteRule(),
            lsmFKey.getFkName(), lsmFKey.getPkName(), lsmFKey.isEnable_cstr(), lsmFKey.isValidate_cstr(),
            lsmFKey.isRely_cstr());
        return hiveFKey;
    }

    public static List<ColumnStatistics> toColumnStatisticsList(PartitionStatisticData result, String dbName,
            String tableName) {
        List<ColumnStatistics> list = Lists.newArrayList();
        if (result == null || result.getStatisticsResults() == null) {
            return list;
        }
        Map<String, List<io.polycat.catalog.common.model.stats.ColumnStatisticsObj>> statisticsResults = result.getStatisticsResults();
        for (Entry<String, List<io.polycat.catalog.common.model.stats.ColumnStatisticsObj>> entry: statisticsResults.entrySet()) {
            if (CollectionUtils.isNotEmpty(entry.getValue())) {
                List<ColumnStatisticsObj> statisticsObjs = entry.getValue().stream()
                        .map(HiveDataAccessor::convertToHiveStatsObj).collect(Collectors.toList());
                list.add(convertToColumnStatistics(statisticsObjs, false, dbName, tableName, entry.getKey()));
            }
        }

        return list;
    }

    private static ColumnStatistics convertToColumnStatistics(List<ColumnStatisticsObj> statisticsObjs,
            boolean isTblLevel, String dbName, String tableName, String partName) {
        ColumnStatisticsDesc statisticsDesc = new ColumnStatisticsDesc(isTblLevel, dbName, tableName);
        if (partName != null) {
            statisticsDesc.setPartName(partName);
        }
        return new ColumnStatistics(statisticsDesc, statisticsObjs);
    }
/*
    public static Object toConstraint(Constraint constraint) {
        switch (constraint.getCstr_type()) {
            case CHECK_CSTR:
                return toCheckConstraint(constraint);
            case DEFAULT_CSTR:
                return toDefaultConstraint(constraint);
            case NOT_NULL_CSTR:
                return toNotNullConstraint(constraint);
            case UNIQUE_CSTR:
                return toUniqueConstraint(constraint);
            default:
                throw new CatalogException("constraint transform illegal");
        }
    }

    public static SQLCheckConstraint toCheckConstraint(Constraint lsmCstr) {
        SQLCheckConstraint hiveCheckCstr = new SQLCheckConstraint();
        if (lsmCstr.getCstr_type() != ConstraintType.CHECK_CSTR) {
            throw new CatalogException("constraint transform illegal");
        }

        hiveCheckCstr.setCatNameIsSet(true);
        hiveCheckCstr.setCatName(lsmCstr.getCatName());
        hiveCheckCstr.setTable_db(lsmCstr.getDbName());
        hiveCheckCstr.setTable_name(lsmCstr.getTable_name());
        hiveCheckCstr.setColumn_name(lsmCstr.getColumn_name());
        hiveCheckCstr.setValidate_cstr(lsmCstr.isValidate_cstr());
        hiveCheckCstr.setEnable_cstr(lsmCstr.isEnable_cstr());
        hiveCheckCstr.setRely_cstr(lsmCstr.isRely_cstr());
        hiveCheckCstr.setDc_name(lsmCstr.getCstr_name());
        hiveCheckCstr.setCheck_expression(lsmCstr.getCstr_info());

        return hiveCheckCstr;
    }

    public static SQLNotNullConstraint toNotNullConstraint(Constraint lsmCstr) {
        SQLNotNullConstraint hiveNotNullCstr = new SQLNotNullConstraint();
        if (lsmCstr.getCstr_type() != ConstraintType.NOT_NULL_CSTR) {
            throw new CatalogException("constraint transform illegal");
        }

        hiveNotNullCstr.setCatNameIsSet(true);
        hiveNotNullCstr.setCatName(lsmCstr.getCatName());
        hiveNotNullCstr.setTable_db(lsmCstr.getDbName());
        hiveNotNullCstr.setTable_name(lsmCstr.getTable_name());
        hiveNotNullCstr.setColumn_name(lsmCstr.getColumn_name());
        hiveNotNullCstr.setValidate_cstr(lsmCstr.isValidate_cstr());
        hiveNotNullCstr.setEnable_cstr(lsmCstr.isEnable_cstr());
        hiveNotNullCstr.setRely_cstr(lsmCstr.isRely_cstr());
        hiveNotNullCstr.setNn_name(lsmCstr.getCstr_name());

        return hiveNotNullCstr;
    }

    public static SQLDefaultConstraint toDefaultConstraint(Constraint lsmCstr) {
        SQLDefaultConstraint hiveDefaultCstr = new SQLDefaultConstraint();
        if (lsmCstr.getCstr_type() != ConstraintType.DEFAULT_CSTR) {
            throw new CatalogException("constraint transform illegal");
        }

        hiveDefaultCstr.setCatNameIsSet(true);
        hiveDefaultCstr.setCatName(lsmCstr.getCatName());
        hiveDefaultCstr.setTable_db(lsmCstr.getDbName());
        hiveDefaultCstr.setTable_name(lsmCstr.getTable_name());
        hiveDefaultCstr.setColumn_name(lsmCstr.getColumn_name());
        hiveDefaultCstr.setValidate_cstr(lsmCstr.isValidate_cstr());
        hiveDefaultCstr.setEnable_cstr(lsmCstr.isEnable_cstr());
        hiveDefaultCstr.setRely_cstr(lsmCstr.isRely_cstr());
        hiveDefaultCstr.setDc_name(lsmCstr.getCstr_name());
        hiveDefaultCstr.setDefault_value(lsmCstr.getCstr_info());

        return hiveDefaultCstr;
    }

    public static SQLUniqueConstraint toUniqueConstraint(Constraint lsmCstr) {
        SQLUniqueConstraint hiveUniqueCstr = new SQLUniqueConstraint();
        if (!lsmCstr.getCstr_type().equals(ConstraintType.UNIQUE_CSTR)) {
            throw new CatalogException("constraint transform illegal");
        }
        hiveUniqueCstr.setCatNameIsSet(true);
        hiveUniqueCstr.setCatName(lsmCstr.getCatName());
        hiveUniqueCstr.setTable_db(lsmCstr.getDbName());
        hiveUniqueCstr.setTable_name(lsmCstr.getTable_name());
        hiveUniqueCstr.setColumn_name(lsmCstr.getColumn_name());
        hiveUniqueCstr.setValidate_cstr(lsmCstr.isValidate_cstr());
        hiveUniqueCstr.setEnable_cstr(lsmCstr.isEnable_cstr());
        hiveUniqueCstr.setRely_cstr(lsmCstr.isRely_cstr());
        hiveUniqueCstr.setUk_name(lsmCstr.getCstr_name());
        hiveUniqueCstr.setKey_seq(Integer.parseInt(lsmCstr.getCstr_info()));

        return hiveUniqueCstr;
    }*/
}
