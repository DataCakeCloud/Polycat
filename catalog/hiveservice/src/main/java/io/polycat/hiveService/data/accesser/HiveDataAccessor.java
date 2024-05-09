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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.Constraint;
import io.polycat.catalog.common.model.ConstraintType;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.ForeignKey;
import io.polycat.catalog.common.model.PartitionAlterContext;
import io.polycat.catalog.common.model.PrimaryKey;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.base.PartitionInput;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionByValuesInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionsByExprsInput;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.common.plugin.request.input.SetPartitionColumnStatisticsInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DoubleColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.StringColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;

public class HiveDataAccessor {

    static Map<String, Function<Object, ColumnStatisticsData>> statsMap = new HashMap<String, Function<Object, ColumnStatisticsData>>() {{
        put("binaryStats", HiveDataAccessor::convertToHiveBinaryData);
        put("booleanStats", HiveDataAccessor::convertToHiveBooleanData);
        put("dateStats", HiveDataAccessor::convertToHiveDateData);
        put("decimalStats", HiveDataAccessor::convertToHiveDecimalData);
        put("doubleStats", HiveDataAccessor::convertToHiveDoubleData);
        put("longStats", HiveDataAccessor::convertToHiveLongData);
        put("stringStats", HiveDataAccessor::convertToHiveStringData);
    }};

    public static Catalog toCatalog(CatalogInput catalogInput) throws MetaException {
        return new CatalogBuilder()
            .setName(catalogInput.getCatalogName())
            .setLocation(catalogInput.getLocation())
            .setDescription(catalogInput.getDescription())
            .build();
    }

    public static Database toDatabase(DatabaseInput dataBaseInput) {
        Database database = new org.apache.hadoop.hive.metastore.api.Database();
        database.setCatalogName(dataBaseInput.getCatalogName());
        database.setDescription(dataBaseInput.getDescription());
        database.setName(dataBaseInput.getDatabaseName());
        database.setLocationUri(dataBaseInput.getLocationUri());
        database.setParameters(dataBaseInput.getParameters());
        database.setOwnerName(dataBaseInput.getOwner());
        database.setOwnerType(convertToOwnerType(dataBaseInput.getOwnerType()));
        return database;
    }

    public static Table toTable(TableName tableName, TableInput tableInput) {
        DatabaseName dbName = new DatabaseName(tableName.getProjectId(), tableName.getCatalogName(),
            tableName.getDatabaseName());
        return toTable(dbName, tableInput);
    }

    public static Table toTable(DatabaseName databaseName, TableInput tableInput) {
        Table table = new Table();
        table.setCatName(databaseName.getCatalogName());
        table.setDbName(databaseName.getDatabaseName());
        table.setTableName(tableInput.getTableName());
        table.setOwner(tableInput.getOwner());
        table.setOwnerType(PrincipalType.valueOf(tableInput.getOwnerType()));
        table.setRetention(tableInput.getRetention());
        table.setSd(convertToStorageDescriptor(tableInput.getStorageDescriptor()));
        table.setPartitionKeys(convertToColumns(tableInput.getPartitionKeys()));
        table.setParameters(tableInput.getParameters());
        table.setTableType(tableInput.getTableType());
        table.setCreateTime(toInteger(tableInput.getCreateTime()));
        table.setLastAccessTime(toInteger(tableInput.getLastAccessTime()));
        table.setViewExpandedText(tableInput.getViewExpandedText());
        table.setViewOriginalText(tableInput.getViewOriginalText());
        // privileges
        return table;
    }

    private static org.apache.hadoop.hive.metastore.api.StorageDescriptor convertToStorageDescriptor(
        StorageDescriptor lmsSd) {
        if (Objects.nonNull(lmsSd)) {
            org.apache.hadoop.hive.metastore.api.StorageDescriptor hiveSd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor();
            hiveSd.setCols(convertToColumns(lmsSd.getColumns()));
            hiveSd.setLocation(lmsSd.getLocation());
            hiveSd.setInputFormat(lmsSd.getInputFormat());
            hiveSd.setOutputFormat(lmsSd.getOutputFormat());
            hiveSd.setCompressed(lmsSd.getCompressed());
            hiveSd.setNumBuckets(Optional.ofNullable(lmsSd.getNumberOfBuckets()).orElse(0));
            hiveSd.setSerdeInfo(convertToSerdeInfo(lmsSd.getSerdeInfo()));
            hiveSd.setBucketCols(lmsSd.getBucketColumns());
            hiveSd.setSortCols(convertToSortColumns(lmsSd.getSortColumns()));
            hiveSd.setParameters(lmsSd.getParameters().isEmpty() ? null : lmsSd.getParameters());
            hiveSd.setSkewedInfo(convertToSkewInfo(lmsSd.getSkewedInfo()));
            hiveSd.setStoredAsSubDirectories(lmsSd.getStoredAsSubDirectories());
            return hiveSd;
        }
        return null;
    }

    private static SkewedInfo convertToSkewInfo(io.polycat.catalog.common.model.SkewedInfo lmsSkewInfo) {
        if (Objects.nonNull(lmsSkewInfo)) {
            Map<List<String>, String> hiveMap = getSkewedValueLocationMap(lmsSkewInfo);
            return new SkewedInfo(lmsSkewInfo.getSkewedColumnNames()
                , lmsSkewInfo.getSkewedColumnValues(), hiveMap);
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
            throw new CatalogServerException(ErrorCode.ARGUMENT_ILLEGAL, valStr);
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

    public static ColumnStatistics toColumnStatistics(
        io.polycat.catalog.common.model.stats.ColumnStatistics stats) {
        io.polycat.catalog.common.model.stats.ColumnStatisticsDesc polyCatStats = stats.getColumnStatisticsDesc();
        ColumnStatisticsDesc hiveDesc = new ColumnStatisticsDesc(polyCatStats.isTableLevel(), polyCatStats.getDbName(),
            polyCatStats.getTableName());
        hiveDesc.setCatName(polyCatStats.getCatName());
        hiveDesc.setLastAnalyzed(polyCatStats.getLastAnalyzed());
        hiveDesc.setPartName(polyCatStats.getPartName());
        List<ColumnStatisticsObj> hiveStatsObjs = new ArrayList<>();
        stats.getColumnStatisticsObjs()
            .forEach(polyCatStatsObj -> hiveStatsObjs.add(convertToHiveStatsObj(polyCatStatsObj)));
        return new ColumnStatistics(hiveDesc, hiveStatsObjs);
    }

    public static org.apache.hadoop.hive.metastore.api.Function toFunction(FunctionInput lsmFunc) {
        org.apache.hadoop.hive.metastore.api.Function hiveFunc = new org.apache.hadoop.hive.metastore.api.Function(
            lsmFunc.getFunctionName(),
            lsmFunc.getDatabaseName(),
            lsmFunc.getClassName(),
            lsmFunc.getOwner(),
            convertToOwnerType(lsmFunc.getOwnerType()),
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
        hiveFunc.setCatName(lsmFunc.getCatalogName());
        return hiveFunc;
    }

    private static FunctionType convertToFunctionType(String funcType) {
        return funcType == null ? null : FunctionType.valueOf(funcType);
    }

    public static List<SQLCheckConstraint> toCheckConstraint(List<Constraint> constraint) {
        return constraint.stream().map(lsmCstr -> {
            SQLCheckConstraint hiveCheckCstr = new SQLCheckConstraint();
            if (lsmCstr.getCstr_type() != ConstraintType.CHECK_CSTR) {
                throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR);
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
        }).collect(Collectors.toList());
    }

    public static List<SQLDefaultConstraint> toDefaultConstraint(List<Constraint> constraint) {
        return constraint.stream().map(lsmCstr -> {
            SQLDefaultConstraint hiveDefaultCstr = new SQLDefaultConstraint();
            if (lsmCstr.getCstr_type() != ConstraintType.DEFAULT_CSTR) {
                throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR);
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
        }).collect(Collectors.toList());
    }

    public static List<SQLNotNullConstraint> toNotNullConstraint(List<Constraint> constraints) {
        return constraints.stream().map(lsmCstr -> {
            SQLNotNullConstraint hiveNotNullCstr = new SQLNotNullConstraint();
            if (lsmCstr.getCstr_type() != ConstraintType.NOT_NULL_CSTR) {
                throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR);
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
        }).collect(Collectors.toList());
    }

    public static List<SQLUniqueConstraint> toUniqueConstraint(List<Constraint> constraint) {
        return constraint.stream().map(lsmCstr -> {
            SQLUniqueConstraint hiveUniqueCstr = new SQLUniqueConstraint();
            if (lsmCstr.getCstr_type() != ConstraintType.UNIQUE_CSTR) {
                throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR);
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
        }).collect(Collectors.toList());
    }

    public static UniqueConstraintsRequest toUniqueCtrReq(TableName tableName) {
        return new UniqueConstraintsRequest(tableName.getCatalogName(),
            tableName.getDatabaseName(),
            tableName.getTableName());
    }

    public static DefaultConstraintsRequest toDefaultCtrReq(TableName tableName) {
        return new DefaultConstraintsRequest(tableName.getCatalogName(),
            tableName.getDatabaseName(),
            tableName.getTableName());
    }

    public static NotNullConstraintsRequest toNotNullCtrReq(TableName tableName) {
        return new NotNullConstraintsRequest(tableName.getCatalogName(),
            tableName.getDatabaseName(),
            tableName.getTableName());
    }

    public static CheckConstraintsRequest toCheckCtrReq(TableName tableName) {
        return new CheckConstraintsRequest(tableName.getCatalogName(),
            tableName.getDatabaseName(),
            tableName.getTableName());
    }

    public static ForeignKeysRequest toForeignKeyRequest(TableName parentTblName, TableName foreignTableName) {
        ForeignKeysRequest request = new ForeignKeysRequest();
        request.setCatName(foreignTableName.getCatalogName());
        request.setForeign_db_name(foreignTableName.getDatabaseName());
        request.setForeign_tbl_name(foreignTableName.getTableName());
        request.setParent_db_name(parentTblName.getDatabaseName());
        request.setParent_tbl_name(parentTblName.getTableName());
        return request;
    }

    public static List<SQLForeignKey> toForeignKeys(List<ForeignKey> foreignKeys) {
        return foreignKeys.stream().map(
            lsmFKey -> {
                SQLForeignKey hiveFKey = new SQLForeignKey(lsmFKey.getPkTableDb(), lsmFKey.getPkTableName(),
                    lsmFKey.getPkColumnName(), lsmFKey.getFkTableDb(), lsmFKey.getFkTableName(),
                    lsmFKey.getFkColumnName(), lsmFKey.getKeySeq(), lsmFKey.getUpdateRule(), lsmFKey.getDeleteRule(),
                    lsmFKey.getFkName(), lsmFKey.getPkName(), lsmFKey.isEnable_cstr(), lsmFKey.isValidate_cstr(),
                    lsmFKey.isRely_cstr());
                hiveFKey.setCatName(lsmFKey.getCatName());
                return hiveFKey;
            }).collect(Collectors.toList());
    }

    public static List<SQLPrimaryKey> toPrimaryKeys(List<PrimaryKey> primaryKeys) {
        return primaryKeys.stream().map(
                lsmPKey -> {
                    SQLPrimaryKey hivePKey = new SQLPrimaryKey(lsmPKey.getDbName(), lsmPKey.getTableName(),
                        lsmPKey.getColumnName(), lsmPKey.getKeySeq(), lsmPKey.getPkName(),
                        lsmPKey.isEnable_cstr(), lsmPKey.isValidate_cstr(), lsmPKey.isRely_cstr());
                    hivePKey.setCatName(lsmPKey.getCatName());
                    return hivePKey;
                })
            .collect(Collectors.toList());
    }

    public static PrimaryKeysRequest toPrimaryKeyReq(String catName, String dbName, String tableName) {
        PrimaryKeysRequest req = new PrimaryKeysRequest();
        req.setCatName(catName);
        req.setDb_name(dbName);
        req.setTbl_name(tableName);
        return req;
    }

    public static Partition toPartition(PartitionInput partition) {
        Partition hivePartition = new Partition(partition.getPartitionValues(),
            partition.getDatabaseName(),
            partition.getTableName(),
            partition.getCreateTime().intValue(),
            partition.getLastAccessTime().intValue(),
            convertToStorageDescriptor(partition.getStorageDescriptor()),
            partition.getParameters().isEmpty() ? null : partition.getParameters());
        hivePartition.setCatName(partition.getCatalogName());
        return hivePartition;
    }

    public static List<ObjectPair<Integer, byte[]>> toExprsList(List<Pair<Integer, byte[]>> exprs) {
        return exprs.stream().map(pair -> new ObjectPair<>(pair.getKey(), pair.getValue()))
            .collect(Collectors.toList());
    }

    public static PartitionDropOptions toPartitionDropOptions(DropPartitionByValuesInput dropPartitionByValuesInput) {
        return new PartitionDropOptions()
            .deleteData(dropPartitionByValuesInput.deleteData)
            .ifExists(dropPartitionByValuesInput.ifExists)
            .purgeData(dropPartitionByValuesInput.purgeData)
            .returnResults(dropPartitionByValuesInput.returnResults);
    }

    public static PartitionDropOptions toPartitionDropOptions(DropPartitionsByExprsInput input) {
        return new PartitionDropOptions()
            .deleteData(input.deleteData)
            .ifExists(input.ifExists)
            .purgeData(input.purgeData)
            .returnResults(input.returnResults);
    }

    public static Partition toPartition(Partition oldPartition, PartitionAlterContext alterContext) {
        Partition newPart = new Partition(oldPartition);
        newPart.setValues(alterContext.getNewValues());
        newPart.setParameters(alterContext.getParameters());
        newPart.setLastAccessTime(alterContext.getLastAccessTime());
        newPart.setCreateTime(alterContext.getCreateTime());
        newPart.getSd().setInputFormat(alterContext.getInputFormat());
        newPart.getSd().setOutputFormat(alterContext.getOutputFormat());
        newPart.getSd().setLocation(alterContext.getLocation());
        return newPart;
    }

    public static SetPartitionsStatsRequest toSetPartitionsStatsRequest(SetPartitionColumnStatisticsInput input) {
        SetPartitionsStatsRequest request = new SetPartitionsStatsRequest();
        request.setColStats(
            input.getStats().stream().map(HiveDataAccessor::toColumnStatistics).collect(Collectors.toList()));
        request.setNeedMerge(input.isNeedMerge());
        return request;
    }

    private static PrincipalType convertToOwnerType(String ownerType) {
        return ownerType == null ? null : PrincipalType.valueOf(ownerType);
    }

    private static List<FieldSchema> convertToColumns(List<Column> columns) {
        if (Objects.isNull(columns) || columns.isEmpty()) {
            return Collections.emptyList();
        }
        return columns.stream()
            .map(column -> new FieldSchema(column.getColumnName(), column.getColType(), column.getComment()))
            .collect(Collectors.toList());
    }

    private static List<FieldSchema> convertTableColumnToHiveColumn(List<Column> columns) {
        return columns.stream()
            .map(column -> new FieldSchema(column.getColumnName(), column.getColType(), column.getComment()))
            .collect(Collectors.toList());
    }

    private static ColumnStatisticsObj convertToHiveStatsObj(
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
        binaryData.setBitVectors(polyCatBinaryData.getBitVectors());
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
        booleanData.setBitVectors(polyCatBooleanData.getBitVectors());
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

        DateColumnStatsData dateData = new DateColumnStatsDataInspector(polyCatDateData.getNumNulls(),
            polyCatDateData.getNumDVs());
        dateData.setHighValue(new Date(polyCatDateData.getHighValue()));
        dateData.setLowValue(new Date(polyCatDateData.getLowValue()));
        dateData.setBitVectors(polyCatDateData.getBitVectors());
        return dateData;
    }

    private static ColumnStatisticsData convertToHiveDecimalData(Object dataValue) {
        ColumnStatisticsData hiveStats = new ColumnStatisticsData();
        hiveStats.setDecimalStats(convertToHiveDecimalStatsInner(dataValue));
        return hiveStats;
    }

    private static DecimalColumnStatsData convertToHiveDecimalStatsInner(Object dataValue) {
        io.polycat.catalog.common.model.stats.DecimalColumnStatsData polyCatDecimalData =
            mapToStruct((Map<String, ? extends Object>) dataValue,
                io.polycat.catalog.common.model.stats.DecimalColumnStatsData.class);

        DecimalColumnStatsData decimalData = new DecimalColumnStatsDataInspector(polyCatDecimalData.getNumNulls(),
            polyCatDecimalData.getNumDVs());
        decimalData.setHighValue(
            new Decimal(polyCatDecimalData.getHighValue().getScale(), ByteBuffer.wrap(polyCatDecimalData.getHighValue().getUnscaled())));
        decimalData.setLowValue(
            new Decimal(polyCatDecimalData.getLowValue().getScale(), ByteBuffer.wrap(polyCatDecimalData.getLowValue().getUnscaled())));
        decimalData.setBitVectors(polyCatDecimalData.getBitVectors());
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

        DoubleColumnStatsData doubleData = new DoubleColumnStatsDataInspector(polyCatDoubleData.getNumNulls(),
            polyCatDoubleData.getNumDVs());
        doubleData.setHighValue(polyCatDoubleData.getHighValue());
        doubleData.setLowValue(polyCatDoubleData.getLowValue());
        doubleData.setBitVectors(polyCatDoubleData.getBitVectors());
        return doubleData;
    }

    private static <T> T mapToStruct(Map<String, ? extends Object> dataValue, Class<T> targetClass) {
        try {
            T target = targetClass.newInstance();
            BeanUtils.populate(target, dataValue);
            return target;
        } catch (Exception e) {
            throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR);
        }
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

        LongColumnStatsData longData = new LongColumnStatsDataInspector(polyCatLongData.getNumNulls(),
            polyCatLongData.getNumDVs());
        longData.setBitVectors(polyCatLongData.getBitVectors());
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

        StringColumnStatsData stringData = new StringColumnStatsDataInspector(polyCatStringData.getMaxColLen(),
            polyCatStringData.getAvgColLen(), polyCatStringData.getNumNulls(), polyCatStringData.getNumDVs());
        stringData.setBitVectors(polyCatStringData.getBitVectors());
        return stringData;
    }

    private static int toInteger(Long longNum) {
        if (longNum == null) {
            return 0;
        }
        return longNum.intValue();
    }
}

