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
package io.polycat.catalog.store.common;

import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.polycat.catalog.common.model.ColumnObject;
import io.polycat.catalog.common.model.DataFileObject;
import io.polycat.catalog.common.model.DataPartitionSetObject;
import io.polycat.catalog.common.model.FileStatsObject;
import io.polycat.catalog.common.model.IndexPartitionSetObject;
import io.polycat.catalog.common.model.PartitionColumnInfo;
import io.polycat.catalog.common.model.PartitionInfo;
import io.polycat.catalog.common.model.PartitionObject;
import io.polycat.catalog.common.model.TableBaseObject;
import io.polycat.catalog.common.model.TableHistoryObject;
import io.polycat.catalog.common.model.TableIndexInfoObject;
import io.polycat.catalog.common.model.TablePartitionSetType;
import io.polycat.catalog.common.model.TablePartitionType;
import io.polycat.catalog.common.model.TableSchemaObject;
import io.polycat.catalog.common.model.TableStorageObject;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.SkewedInfo;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.protos.DataPartitionSet;
import io.polycat.catalog.store.protos.IndexPartitionSet;
import io.polycat.catalog.store.protos.common.ColumnInfo;
import io.polycat.catalog.store.protos.common.DataFile;
import io.polycat.catalog.store.protos.common.FileStats;
import io.polycat.catalog.store.protos.common.Order;
import io.polycat.catalog.store.protos.common.Partition;
import io.polycat.catalog.store.protos.common.PartitionFileInfo;
import io.polycat.catalog.store.protos.common.PartitionSetType;
import io.polycat.catalog.store.protos.common.PartitionType;
import io.polycat.catalog.store.protos.common.SchemaInfo;
import io.polycat.catalog.store.protos.common.SerDeInfo;
import io.polycat.catalog.store.protos.common.StorageInfo;
import io.polycat.catalog.store.protos.common.StringList;
import io.polycat.catalog.store.protos.common.TableBaseInfo;
import io.polycat.catalog.store.protos.common.TableDataInfo;
import io.polycat.catalog.store.protos.common.TableDataPartitionSetInfo;
import io.polycat.catalog.store.protos.common.TableIndexInfo;
import io.polycat.catalog.store.protos.common.TableIndexInfoSet;
import io.polycat.catalog.store.protos.common.TableIndexPartitionSetInfo;

import static java.util.stream.Collectors.toList;

public class TableStoreConvertor {
    public static TableBaseInfo getTableBaseInfo(TableBaseObject tableBaseObject) {
        TableBaseInfo tableBaseInfo = TableBaseInfo.newBuilder()
            .setCreateTime(tableBaseObject.getCreateTime())
            .setAuthSourceType(tableBaseObject.getAuthSourceType())
            .setAccountId(tableBaseObject.getAccountId())
            .setOwnerType(tableBaseObject.getOwnerType())
            .setOwner(tableBaseObject.getOwner())
            .setDescription(tableBaseObject.getDescription())
            .setRetention(tableBaseObject.getRetention())
            .setTableType(tableBaseObject.getTableType())
            .putAllParameters(tableBaseObject.getParameters())
            .setViewOriginalText(tableBaseObject.getViewOriginalText())
            .setViewExpandedText(tableBaseObject.getViewExpandedText())
            .setLmsMvcc(tableBaseObject.isLmsMvcc())
            .build();
        return tableBaseInfo;
    }

    public static StorageInfo getTableStorageInfo(TableStorageObject tableStorageObject) {
        StorageInfo.Builder storageInfo = StorageInfo.newBuilder()
                .setSourceShortName(tableStorageObject.getSourceShortName())
                .setFileFormat(tableStorageObject.getFileFormat())
                .setInputFormat(tableStorageObject.getInputFormat())
                .setOutputFormat(tableStorageObject.getOutputFormat())
                .putAllParameters(tableStorageObject.getParameters())
                .setNumberOfBuckets(tableStorageObject.getNumberOfBuckets())
                .addAllBucketColumns(tableStorageObject.getBucketColumns())
                .setStoredAsSubDirectories(tableStorageObject.getStoredAsSubDirectories());
                //.setSkewedInfo(tableStorageObject.getSkewedInfo());
        if (tableStorageObject.getSkewedInfo() != null) {
            final SkewedInfo skewedInfo = tableStorageObject.getSkewedInfo();
            io.polycat.catalog.store.protos.common.SkewedInfo.Builder skewedInfoBuilder = io.polycat.catalog.store.protos.common.SkewedInfo.newBuilder();
            final List<String> skewedColumnNames = skewedInfo.getSkewedColumnNames();
            skewedColumnNames.forEach(skewedInfoBuilder::addSkewedColumnNames);
            final List<List<String>> skewedColumnValues = skewedInfo.getSkewedColumnValues();
            skewedColumnValues.forEach(values -> {
                final StringList.Builder stringListBuilder = StringList.newBuilder();
                stringListBuilder.addAllValues(values);
                skewedInfoBuilder.addSkewedColumnValues(stringListBuilder);
            });
            skewedInfoBuilder.putAllSkewedColumnValueLocationMaps(skewedInfo.getSkewedColumnValueLocationMaps());
            storageInfo.setSkewedInfo(skewedInfoBuilder);
        }
        if (tableStorageObject.getLocation() != null) {
            storageInfo.setLocation(tableStorageObject.getLocation());
        }
        if (tableStorageObject.getCompressed() != null) {
            storageInfo.setCompressed(tableStorageObject.getCompressed());
        }
        if (tableStorageObject.getSerdeInfo() != null) {
            SerDeInfo serDeInfo = SerDeInfo.newBuilder()
                    .setName(tableStorageObject.getSerdeInfo().getName())
                    .setSerializationLibrary(tableStorageObject.getSerdeInfo().getSerializationLibrary())
                    .putAllParameters(tableStorageObject.getSerdeInfo().getParameters())
                    .build();
            storageInfo.setSerdeInfo(serDeInfo);
        }
        if (tableStorageObject.getSortColumns() != null) {
            List<Order> orderList = new ArrayList<>(tableStorageObject.getSortColumns().size());
            tableStorageObject.getSortColumns().forEach(column -> {
                orderList.add(Order.newBuilder().setColumn(column.getColumn()).setSortOrder(column.getSortOrder()).build());
            });
            storageInfo.addAllSortColumns(orderList);
        }
        return storageInfo.build();
    }

    public static List<ColumnInfo> getColumnInfoList(List<ColumnObject> columnObjects) {
        List<ColumnInfo> columnInfoList = new ArrayList<>(columnObjects.size());
        columnObjects.forEach(columnObject -> {
            ColumnInfo columnInfo = ColumnInfo.newBuilder()
                .setName(columnObject.getName())
                .setOrdinal(columnObject.getOrdinal())
                .setType(columnObject.getDataType().toString())
                .setComment(columnObject.getComment())
                .build();
            columnInfoList.add(columnInfo);
        });

        return columnInfoList;
    }

    public static SchemaInfo getSchemaInfo(TableSchemaObject tableSchemaObject) {
        SchemaInfo schemaInfo = SchemaInfo.newBuilder()
            .addAllColumns(getColumnInfoList(tableSchemaObject.getColumns()))
            .addAllPartitionKeys(getColumnInfoList(tableSchemaObject.getPartitionKeys()))
            .build();
        return schemaInfo;
    }

    public static PartitionSetType trans2PartitionSetType(TablePartitionSetType tablePartitionSetType) {
        switch (tablePartitionSetType) {
            case INIT:
                return PartitionSetType.INIT;
            case DATA:
                return PartitionSetType.DATA;
            case INDEX:
                return PartitionSetType.INDEX;
            default:
                throw new UnsupportedOperationException("failed to convert " + tablePartitionSetType.name());
        }
    }

    public static TableDataInfo getTableDataInfo(TableHistoryObject tableHistoryObject) {
        TableDataInfo tableDataInfo = TableDataInfo.newBuilder()
            .setPartitionType(trans2PartitionSetType(tableHistoryObject.getPartitionSetType()))
            .setCurSetId(tableHistoryObject.getCurSetId())
            .addAllSetIds(tableHistoryObject.getSetIds())
            .setTableIndexUrl(tableHistoryObject.getTableIndexUrl())
            .build();

        return tableDataInfo;
    }

    private static PartitionType trans2PartitionType(TablePartitionType tablePartitionType) {
        switch (tablePartitionType) {
            case INTERNAL:
                return PartitionType.INTERNAL;
            case EXTERNAL:
                return PartitionType.EXTERNAL;
            case CACHE:
                return PartitionType.CACHE;
            default:
                throw new UnsupportedOperationException("failed to convert " + tablePartitionType.name());
        }
    }

    private static DataFile getDataFile(DataFileObject dataFileObject) {
        DataFile dataFile = DataFile.newBuilder()
            .setFileName(dataFileObject.getFileName())
            .setOffset(dataFileObject.getOffset())
            .setLength(dataFileObject.getLength())
            .setRowCount(dataFileObject.getRowCount())
            .build();

        return dataFile;
    }

    private static FileStats getFileStats(FileStatsObject fileStatsObject) {
        FileStats fileStats = FileStats.newBuilder()
            .addAllMinValue(
                fileStatsObject.getMinValue().stream().map(bytes -> ByteString.copyFrom(bytes)).collect(toList()))
            .addAllMaxValue(
                fileStatsObject.getMaxValue().stream().map(bytes -> ByteString.copyFrom(bytes)).collect(toList()))
            .build();

        return fileStats;
    }

    public static Partition getPartition(PartitionObject partitionObject) {
        Partition partition = Partition.newBuilder()
            .setName(partitionObject.getName())
            .setType(trans2PartitionType(partitionObject.getType()))
            .setPartitionId(partitionObject.getPartitionId())
            .setSchemaVersion(CodecUtil.hex2ByteString(partitionObject.getSchemaVersion()))
            .setInvisible(partitionObject.isInvisible())
            .setLocation(partitionObject.getLocation())
            .addAllColumn(getColumnInfoList(partitionObject.getColumn()))
            .addAllFile(partitionObject.getFile().stream().map(dataFileObject -> getDataFile(dataFileObject))
                .collect(toList()))
            .addAllStats(partitionObject.getStats().stream().map(fileStatsObject -> getFileStats(fileStatsObject))
                .collect(toList()))
            .setPartitionIndexUrl(partitionObject.getPartitionIndexUrl())
            .setFileFormat(partitionObject.getFileFormat())
            .setInputFormat(partitionObject.getInputFormat())
            .setOutputFormat(partitionObject.getOutputFormat())
            .setSerde(partitionObject.getSerde())
            .putAllProperties(partitionObject.getProperties())
            .setStartTime(partitionObject.getStartTime())
            .setEndTime(partitionObject.getEndTime())
            .build();

        return partition;
    }

    public static TableDataPartitionSetInfo getDataPartitionSetInfo(DataPartitionSetObject dataPartitionSetObject) {
        TableDataPartitionSetInfo tableDataPartitionSetInfo = TableDataPartitionSetInfo.newBuilder()
            .addAllDataPartitions(dataPartitionSetObject.getDataPartitions().stream()
                .map(partitionObject -> TableStoreConvertor.getPartition(partitionObject))
                .collect(toList()))
            .setPartitionsSize(dataPartitionSetObject.getPartitionsSize())
            .build();
        return tableDataPartitionSetInfo;
    }

    public static TableIndexPartitionSetInfo getIndexPartitionSetInfo(IndexPartitionSetObject indexPartitionSetObject) {
        TableIndexPartitionSetInfo tableIndexPartitionSetInfo = TableIndexPartitionSetInfo.newBuilder()
            .addAllSetIds(indexPartitionSetObject.getSetIds())
            .setCurSetId(indexPartitionSetObject.getCurSetId())
            .build();
        return tableIndexPartitionSetInfo;
    }

    public static TableIndexInfo getTableIndexInfo(TableIndexInfoObject tableIndexInfo) {
        return TableIndexInfo.newBuilder()
            .setDatabaseId(tableIndexInfo.getDatabaseId())
            .setIndexId(tableIndexInfo.getIndexId())
            .setIsMV(tableIndexInfo.isMV())
            .build();
    }

    public static TableIndexInfoSet getTableIndexInfoSet(List<TableIndexInfoObject> tableIndexInfo) {
        return TableIndexInfoSet.newBuilder().addAllIndexInfos(
            tableIndexInfo.stream().map(tableIndexInfoObject -> getTableIndexInfo(tableIndexInfoObject))
                .collect(toList())).build();
    }

    public static DataPartitionSet getDataPartitionSet(DataPartitionSetObject dataPartitionSetObject) {
        DataPartitionSet dataPartitionSet = DataPartitionSet.newBuilder()
                .setDataPartitionSetInfo(TableStoreConvertor.getDataPartitionSetInfo(dataPartitionSetObject))
                .setSetId(dataPartitionSetObject.getSetId())
                .setCatalogId(dataPartitionSetObject.getCatalogId())
                .setDatabaseId(dataPartitionSetObject.getDatabaseId())
                .setTableId(dataPartitionSetObject.getTableId())
                .build();
        return dataPartitionSet;
    }

    public static IndexPartitionSet getIndexPartitionSet(IndexPartitionSetObject indexPartitionSetObject) {
        IndexPartitionSet indexPartitionSet = IndexPartitionSet.newBuilder()
                .setSetId(indexPartitionSetObject.getCurSetId())
                .setCatalogId(indexPartitionSetObject.getCatalogId())
                .setDatabaseId(indexPartitionSetObject.getDatabaseId())
                .setTableId(indexPartitionSetObject.getTableId())
                .setIndexPartitionSetInfo(TableStoreConvertor.getIndexPartitionSetInfo(indexPartitionSetObject))
                .build();

        return indexPartitionSet;
    }

    public static PartitionInfo getPartitionInfo(PartitionObject partitionObject) {
        final PartitionFileInfo partitionFileInfo = PartitionFileInfo.newBuilder()
            .addAllFile(
                partitionObject.getFile().stream().map(dataFileObject -> getDataFile(dataFileObject))
                    .collect(toList()))
            .addAllStats(
                partitionObject.getStats().stream().map(fileStatsObject -> getFileStats(fileStatsObject))
                    .collect(toList()))
            .putAllProperties(partitionObject.getProperties())
            .build();
        return new PartitionInfo(
            partitionObject.getPartitionId(),
            partitionObject.getName(),
            partitionObject.getType().getNum(),
            partitionObject.getSchemaVersion(),
            partitionObject.isInvisible(),
            partitionObject.getLocation(),
            partitionFileInfo.toByteArray(),
            partitionObject.getPartitionIndexUrl(),
            partitionObject.getFileFormat(),
            partitionObject.getInputFormat(),
            partitionObject.getOutputFormat(),
            partitionObject.getSerde(),
            partitionObject.getStartTime(),
            partitionObject.getEndTime(),
            "",
            "",
            new ArrayList<>()
        );
    }

    public static PartitionColumnInfo getPartitionColumnInfo(ColumnObject columnObject) {
        return new PartitionColumnInfo(
            UuidUtil.generateId(),
            columnObject.getName(),
            "",
            columnObject.getOrdinal(),
            columnObject.getDataType().getName(),
            columnObject.getComment(),
            "",
            "");
    }

    public static PartitionObject convertToPartitionObject(PartitionInfo partitionInfo) {
        final PartitionObject partitionObject = new PartitionObject();
        partitionObject.setSetId(partitionInfo.getSetId());
        partitionObject.setName(partitionInfo.getName());
        partitionObject.setType(TablePartitionType.values()[partitionInfo.getType()-1]);
        partitionObject.setPartitionId(partitionInfo.getId());
        partitionObject.setSchemaVersion(partitionInfo.getSchemaVersion());
        partitionObject.setInvisible(partitionInfo.isInvisible());
        partitionObject.setLocation(partitionInfo.getLocation());
        if (partitionInfo.getPartitionColumnInfos() != null) {
            partitionObject.setColumn(partitionInfo.getPartitionColumnInfos().stream().map(partitionColumnInfo -> {
                final ColumnObject columnObject = new ColumnObject();
                columnObject.setName(partitionColumnInfo.getName());
                columnObject.setComment(partitionColumnInfo.getComment());
                columnObject.setDataType(DataTypes.valueOf(partitionColumnInfo.getType()));
                columnObject.setOrdinal(partitionColumnInfo.getOrdinal());
                return columnObject;
            }).collect(toList()));
        }
        try {
            final PartitionFileInfo partitionFileInfo = PartitionFileInfo
                .parseFrom(partitionInfo.getPartitionFileInfo());
            partitionObject.setFile(partitionFileInfo.getFileList().stream().map(DataFileObject::new).collect(toList()));
            partitionObject.setStats(partitionFileInfo.getStatsList().stream().map(FileStatsObject::new).collect(toList()));
            partitionObject.setProperties(partitionFileInfo.getPropertiesMap());
        } catch (InvalidProtocolBufferException e) {
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
        partitionObject.setPartitionIndexUrl(partitionInfo.getPartitionIndexUrl());
        partitionObject.setFileFormat(partitionInfo.getFileFormat());
        partitionObject.setInputFormat(partitionInfo.getInputFormat());
        partitionObject.setOutputFormat(partitionInfo.getOutputFormat());
        partitionObject.setSerde(partitionInfo.getSerde());
        partitionObject.setStartTime(partitionInfo.getStartTime());
        partitionObject.setEndTime(partitionInfo.getEndTime());
        return partitionObject;
    }

}
