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
package io.polycat.catalog.server.service.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.polycat.catalog.server.util.TransactionFrameRunner;
import io.polycat.catalog.server.util.TransactionRunnerUtil;
import io.polycat.catalog.common.model.ColumnStatisticsAggrObject;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.ColumnStatisticsObject;
import io.polycat.catalog.common.model.stats.ColumnStatisticsDesc;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import io.polycat.catalog.common.utils.TableUtil;
import io.polycat.catalog.service.api.TableService;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import static io.polycat.catalog.common.Operation.ALTER_PARTITION;
import static io.polycat.catalog.common.Operation.DROP_PARTITION;
import static io.polycat.catalog.common.Operation.INSERT_TABLE;

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.ColumnObject;
import io.polycat.catalog.common.model.DataFile;
import io.polycat.catalog.common.model.DataFileObject;
import io.polycat.catalog.common.model.DataPartitionSetObject;
import io.polycat.catalog.common.model.FileStatsObject;
import io.polycat.catalog.common.model.OperationObject;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.PartitionAlterContext;
import io.polycat.catalog.common.model.PartitionObject;
import io.polycat.catalog.common.model.SerDeInfo;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.TableBaseObject;
import io.polycat.catalog.common.model.TableCommitObject;
import io.polycat.catalog.common.model.TableHistoryObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TableOperationType;
import io.polycat.catalog.common.model.TablePartitionSetType;
import io.polycat.catalog.common.model.TablePartitionType;
import io.polycat.catalog.common.model.TableSchemaHistoryObject;
import io.polycat.catalog.common.model.TableSchemaObject;
import io.polycat.catalog.common.model.TableStats;
import io.polycat.catalog.common.model.TableStorageObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.model.base.PartitionInput;
import io.polycat.catalog.common.model.stats.AggrStatisticData;
import io.polycat.catalog.common.model.stats.PartitionStatisticData;
import io.polycat.catalog.common.plugin.request.input.AddPartitionInput;
import io.polycat.catalog.common.plugin.request.input.AlterPartitionInput;
import io.polycat.catalog.common.plugin.request.input.ColumnStatisticsInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionByValuesInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionsByExprsInput;
import io.polycat.catalog.common.plugin.request.input.FileInput;
import io.polycat.catalog.common.plugin.request.input.FileStatsInput;
import io.polycat.catalog.common.plugin.request.input.FilterInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionWithAuthInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionsByExprInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionsWithAuthInput;
import io.polycat.catalog.common.plugin.request.input.PartitionDescriptorInput;
import io.polycat.catalog.common.plugin.request.input.PartitionFilterInput;
import io.polycat.catalog.common.plugin.request.input.PartitionValuesInput;
import io.polycat.catalog.common.plugin.request.input.SetPartitionColumnStatisticsInput;
import io.polycat.catalog.common.plugin.request.input.TableTypeInput;
import io.polycat.catalog.common.plugin.request.input.TruncatePartitionInput;
import io.polycat.catalog.common.types.DataTypes;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.PartitionUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.server.hive.HiveUtil;
import io.polycat.catalog.server.wrapper.PartitionFilterStatesWrapper;
import io.polycat.catalog.service.api.PartitionService;
import io.polycat.catalog.store.api.CatalogStore;
import io.polycat.catalog.store.api.TableDataStore;
import io.polycat.catalog.store.api.TableMetaStore;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.metrics.MethodStageDurationCollector;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import static java.util.stream.Collectors.toList;

/**
 * @author liangyouze
 * @date 2022/11/22
 */
@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class PartitionServiceImplV2 implements PartitionService {

    private static final Logger log = Logger.getLogger(PartitionServiceImplV2.class);

    @Autowired
    private TableMetaStore tableMetaStore;

    @Autowired
    private TableDataStore tableDataStore;

    @Autowired
    private TableService tableService;

    @Autowired
    private CatalogStore catalogStore;

    private static int TABLE_STORE_MAX_RETRY_NUM = 256;

    // a maximum of 2048 data partition set records can be stored in a index partition set.
    // the value is not final because the test case needs to modify it.
    private static int indexStoredMaxNumber = 2048;

    // 97280 = 95 x 1024, a data partition set can store up to 97280 bytes.
    // the value is not final because the test case needs to modify it.
    private static int dataStoredMaxSize = 97280;

    @Override
    public void addPartition(TableName tableName, AddPartitionInput partitionInput) {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        runner.run(context -> insertPartitionInternal(context, tableName, partitionInput, partitionInput.isOverwrite(),
            catalogCommitEventId)).getResultAndCheck(ret -> CatalogCommitHelper
            .catalogCommitExist(ret, catalogCommitEventId));
    }

    private boolean isNotTransactionAndPartitionTable(TableBaseObject tableBaseObject,
        TableSchemaObject tableSchemaObject) {
        return !tableBaseObject.isLmsMvcc() && tableSchemaObject.getPartitionKeys().size() == 0;
        //return !tableBaseObject.isLmsMvcc() && tableBaseObject.getPartitions().size() == 0;
    }

    private void fillPartLocationIfNotExists(AddPartitionInput partitionInput, TableStorageObject storageObject,
        TableSchemaObject tableSchemaObject) {
        List<String> partitionKeys =
            tableSchemaObject.getPartitionKeys().stream().map(ColumnObject::getName).collect(Collectors.toList());
        for (PartitionInput pb : partitionInput.getPartitions()) {
            if (pb.getStorageDescriptor() == null) {
                pb.setStorageDescriptor(new StorageDescriptor());
            }
            if (StringUtils.isBlank(pb.getStorageDescriptor().getLocation())) {
                pb.getStorageDescriptor().setLocation(storageObject.getLocation() + File.separator +
                    PartitionUtil.makePartitionName(partitionKeys, pb.getPartitionValues()));
            }

        }
    }

    private DataFileObject convert(FileInput fileInput) {
        DataFileObject dataFileObject = new DataFileObject(fileInput.getFileName(), fileInput.getOffset(),
            fileInput.getLength(), fileInput.getRowCount());
        return dataFileObject;
    }

    private FileStatsObject convert(FileStatsInput fileStatsInput) {
        List<byte[]> minValues = Arrays.stream(fileStatsInput.getMinValues())
            .map(CodecUtil::base642Bytes)
            .collect(Collectors.toList());
        List<byte[]> maxValues = Arrays.stream(fileStatsInput.getMaxValues())
            .map(CodecUtil::base642Bytes)
            .collect(Collectors.toList());

        FileStatsObject fileStatsObject = new FileStatsObject(minValues, maxValues);
        return fileStatsObject;
    }

    private PartitionObject makePartitionInfo(TableIdent tableIdent, PartitionInput partitionBase, String basedVersion,
        String fileFormat,
        List<ColumnObject> partitionKeys) {
        PartitionObject partitionObject = new PartitionObject();
        TablePartitionType partitionType =
            partitionBase.getFileIndexUrl() == null ? TablePartitionType.INTERNAL : TablePartitionType.EXTERNAL;
        partitionObject.setType(partitionType);
        if (TablePartitionType.INTERNAL == partitionType) {
            List<DataFileObject> dataFiles = Collections.emptyList();
            if (partitionBase.getFiles() != null) {
                dataFiles = Arrays.stream(partitionBase.getFiles())
                    .map(this::convert).collect(Collectors.toList());
            }

            List<FileStatsObject> fileStatsList = Collections.emptyList();
            if (partitionBase.getIndex() != null) {
                fileStatsList = Arrays.stream(partitionBase.getIndex())
                    .map(this::convert).collect(Collectors.toList());
            }

            if (!fileStatsList.isEmpty() && dataFiles.size() != fileStatsList.size()) {
                throw new CatalogServerException(ErrorCode.SEGMENT_FILE_INVALID);
            }
            partitionObject.setFile(dataFiles);
            partitionObject.setStats(fileStatsList);
            partitionObject.setProperties(partitionBase.getParameters());
            StorageDescriptor sd = partitionBase.getStorageDescriptor();
            if (sd != null) {
                partitionObject.setLocation(sd.getLocation());
                partitionObject.setInputFormat(sd.getInputFormat());
                partitionObject.setOutputFormat(sd.getOutputFormat());
                partitionObject.setColumn(convertToColumnObject(sd.getColumns()));
            }
        } else {
            partitionObject.setPartitionIndexUrl(partitionBase.getFileIndexUrl());
        }
        if (basedVersion == null) {
            partitionObject.setSchemaVersion(VersionManagerHelper.getLatestVersion(tableIdent));
        } else {
            partitionObject.setSchemaVersion(basedVersion);
        }
        if (fileFormat != null) {
            partitionObject.setFileFormat(fileFormat);
        }
        final List<String> partitionKeyNames = partitionKeys.stream().map(ColumnObject::getName)
                .collect(Collectors.toList());
        partitionObject.setPartitionKeys(partitionKeys);
        partitionObject.setName(PartitionUtil.makePartitionName(partitionKeyNames, partitionBase.getPartitionValues()));

        if (partitionBase.getCreateTime() <= 0) {
            partitionObject.setStartTime(System.currentTimeMillis());
        } else {
            partitionObject.setStartTime(partitionBase.getCreateTime());
        }
        if (partitionBase.getLastAccessTime() <= 0) {
            partitionObject.setEndTime(System.currentTimeMillis());
        } else {
            partitionObject.setEndTime(partitionBase.getLastAccessTime());
        }
        return partitionObject;
    }

    private List<ColumnObject> convertToColumnObject(List<Column> columns) {
        if (columns != null) {
            return columns.stream().map(column -> {
                final ColumnObject columnObject = new ColumnObject();
                columnObject.setName(column.getColumnName());
                columnObject.setDataType(DataTypes.valueOf(column.getColType()));
                columnObject.setComment(column.getComment());
                return columnObject;
            }).collect(Collectors.toList());
        }
        return null;
    }

    private PartitionObject buildNewPartition(TransactionContext context, TableIdent tableIdent,
        PartitionObject partition, String schemaVersion)
        throws MetaStoreException {
        PartitionObject partitionObject = new PartitionObject(partition);
        partitionObject.setPartitionId(UuidUtil.generateUUID32());

        if (!partition.getSchemaVersion().isEmpty()) {
            partitionObject.setSchemaVersion(schemaVersion);
        } else {
            partitionObject.setColumn(partition.getColumn());
        }

        return partitionObject;
    }

    private DataPartitionSetObject makeDataPartitionSet(TableIdent tableIdent, List<PartitionObject> newPartitions) {
        DataPartitionSetObject dataPartitionSetObject = new DataPartitionSetObject();
        dataPartitionSetObject.setSetId(UuidUtil.generateUUID32());
        dataPartitionSetObject.setCatalogId(tableIdent.getCatalogId());
        dataPartitionSetObject.setDatabaseId(tableIdent.getDatabaseId());
        dataPartitionSetObject.setTableId(tableIdent.getTableId());
        dataPartitionSetObject.setDataPartitions(newPartitions);

        return dataPartitionSetObject;
    }

    private void insertPartitions(TransactionContext context, TableIdent tableIdent,
        List<PartitionObject> newPartitions,
        TableHistoryObject tableHistoryObject) {
        DataPartitionSetObject dataPartitionSet = tableDataStore
            .getDataPartitionSet(context, tableIdent, tableHistoryObject.getCurSetId());
        if (dataPartitionSet == null) {
            dataPartitionSet = makeDataPartitionSet(tableIdent, newPartitions);
            tableDataStore.insertDataPartitionSet(context, tableIdent, dataPartitionSet);
        } else {
            dataPartitionSet.setDataPartitions(newPartitions);
        }
        tableDataStore.insertPartitionInfo(context, tableIdent, dataPartitionSet);
        tableHistoryObject.setPartitionSetType(TablePartitionSetType.DATA);
        tableHistoryObject.setCurSetId(dataPartitionSet.getSetId());
        String version = VersionManagerHelper.getNextVersion(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId());
        tableDataStore.insertTableHistory(context, tableIdent, version, tableHistoryObject);
    }


    private void insertTablePartition(TransactionContext context, TableIdent tableIdent, PartitionObject newPartition,
        TableHistoryObject latestDataVersion) {

        String version = VersionManagerHelper.getNextVersion(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId());
        final List<PartitionObject> partitionObjects = new ArrayList<>();
        partitionObjects.add(newPartition);
        TableHistoryObject tableHistory = new TableHistoryObject(latestDataVersion);
        insertPartitions(context, tableIdent, partitionObjects, tableHistory);
        tableDataStore.insertTableHistory(context, tableIdent, version, tableHistory);
    }

    private TableIdent insertPartitionInternal(TransactionContext context,
        TableName tableName, AddPartitionInput partitionInput, boolean overwrite, String catalogCommitEventId) {
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        TableBaseObject tableBaseObject = TableBaseHelper.getTableBase(context, tableIdent);
        TableSchemaObject tableSchemaObject = TableSchemaHelper.getTableSchema(context, tableIdent);
        TableStorageObject tableStorage = TableStorageHelper.getTableStorage(context, tableIdent);

        if (isNotTransactionAndPartitionTable(tableBaseObject, tableSchemaObject)) {
            return null;
        }

        if (!isViewTableType(tableBaseObject)) {
            fillPartLocationIfNotExists(partitionInput, tableStorage, tableSchemaObject);
        }

        String basedVersion = partitionInput.getBasedVersion();
        if (basedVersion == null) {
            basedVersion = VersionManagerHelper.getLatestVersion(tableIdent);
        }
        TableSchemaHistoryObject basedSchema = TableSchemaHelper
            .getLatestTableSchemaOrElseThrow(context, tableIdent, basedVersion);

        PartitionObject partitionObject = makePartitionInfo(tableIdent, partitionInput.getPartitions()[0],
            basedVersion, partitionInput.getFileFormat(), tableSchemaObject.getPartitionKeys());

        PartitionObject newPartition = buildNewPartition(context, tableIdent, partitionObject,
            basedSchema.getVersion());

        tableMetaStore.upsertTableReference(context, tableIdent);

        // insert TableHistory
        String currentVersionStamp = VersionManagerHelper.getLatestVersion(tableIdent);
        TableHistoryObject latestDataVersion;
        if (overwrite) {
            TableHistoryObject tableHistoryObject = new TableHistoryObject();
            tableHistoryObject.setEventId(UuidUtil.generateId());
            tableHistoryObject.setPartitionSetType(TablePartitionSetType.INIT);
            tableHistoryObject.setSetIds(Collections.emptyList());

            latestDataVersion = tableHistoryObject;
        } else {
            latestDataVersion = TableHistoryHelper
                .getLatestTableHistory(context, tableIdent, currentVersionStamp).get();
        }

        insertTablePartition(context, tableIdent, newPartition, latestDataVersion);

        // insert TableCommit
        TableCommitObject tableCommitObject = TableCommitHelper
            .getLatestTableCommit(context, tableIdent, currentVersionStamp).get();
        long commitTime = RecordStoreHelper.getCurrentTime();
        OperationObject tableOperation = new OperationObject(TableOperationType.DML_INSERT,
            newPartition.getFile().stream().map(DataFileObject::getRowCount).reduce(0L, Long::sum),
            0, 0, newPartition.getFile().size());

        tableCommitObject.setCommitTime(commitTime);
        tableCommitObject.setDroppedTime(0);
        tableCommitObject.setOperations(Collections.singletonList(tableOperation));
        // insert the drop table into TableCommit subspace
        tableMetaStore.insertTableCommit(context, tableIdent, tableCommitObject);

        // insert CatalogCommit
        StringBuilder builder = new StringBuilder();
        builder.append("database name: ").append(tableName.getDatabaseName()).append(", ")
            .append("table name: ").append(tableName.getTableName()).append(", ")
            .append("partition name: ").append(newPartition.getName());

        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId(), catalogCommitEventId, commitTime, INSERT_TABLE, builder.toString());

        return tableIdent;
    }

    private boolean isViewTableType(TableBaseObject tableBaseObject) {
        return TableTypeInput.VIRTUAL_VIEW.name().equals(tableBaseObject.getTableType());
    }

    private List<PartitionObject> buildNewPartitions(TableIdent tableIdent, List<PartitionObject> partitions,
        TransactionContext context) {
        List<PartitionObject> partitionList = new ArrayList<>();
        String lastSchemaVersion = "";
        TableSchemaHistoryObject basedSchema = null;
        for (PartitionObject partition : partitions) {
            PartitionObject partitionObjectNew = new PartitionObject(partition);
            partitionObjectNew.setPartitionId(UuidUtil.generateId());
            if (!partition.getSchemaVersion().isEmpty()) {
                String basedVersion = partition.getSchemaVersion();
                if (!basedVersion.equals(lastSchemaVersion)) {
                    String versionstamp = basedVersion;
                    basedSchema = TableSchemaHelper.getLatestTableSchemaOrElseThrow(context, tableIdent, versionstamp);
                    lastSchemaVersion = basedVersion;
                }
                partitionObjectNew.setSchemaVersion(basedSchema.getVersion());
            } else {
                partitionObjectNew.setColumn(partition.getColumn());
            }
            partitionList.add(partitionObjectNew);
        }
        return partitionList;
    }

    private OperationObject collectPartitionsRowNumAndFileCount(List<PartitionObject> newPartitions) {
        long addRowNum = 0;
        int fileCount = 0;
        for (PartitionObject partition : newPartitions) {
            addRowNum += partition.getFile().stream().map(DataFileObject::getRowCount).reduce(0L, Long::sum);
            fileCount += partition.getFile().size();
        }
        return new OperationObject(TableOperationType.DML_INSERT, addRowNum, 0, 0, fileCount);
    }

    private TableIdent addPartitionsInternal(TransactionContext context, TableName tableName,
        List<PartitionObject> newPartitions, boolean overwrite, String catalogCommitEventId) {
        MethodStageDurationCollector.Timer timer = MethodStageDurationCollector.startCollectTimer();

        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        // List<PartitionObject> newPartitions = buildNewPartitions(tableIdent, partitions, context);

        timer.observePrevDuration("TableStoreImpl.addPartitionsInternal", "buildNewPartitions");

        tableMetaStore.upsertTableReference(context, tableIdent);
        timer.observePrevDuration("TableStoreImpl.addPartitionsInternal", "saveTableReference");

        String currentVersionStamp = VersionManagerHelper.getLatestVersion(tableIdent);

        timer.observePrevDuration("TableStoreImpl.addPartitionsInternal", "getCurContextVersion");

        //tableHistory commit

        TableHistoryObject baseTableHistory = TableHistoryHelper
            .getLatestTableHistory(context, tableIdent, currentVersionStamp).get();
        timer.observePrevDuration("TableStoreImpl.addPartitionsInternal", "getLatestTableHistory");

        insertPartitions(context, tableIdent, newPartitions, baseTableHistory);

        timer.observePrevDuration("TableStoreImpl.addPartitionsInternal", "insertTableHistory");

        // insert TableCommit
        OperationObject operation = collectPartitionsRowNumAndFileCount(newPartitions);
        long commitTime = RecordStoreHelper.getCurrentTime();
        TableCommitObject tableCommitObject = TableCommitHelper
            .getLatestTableCommit(context, tableIdent, currentVersionStamp).get();

        tableCommitObject.setCommitTime(commitTime);
        tableCommitObject.setDroppedTime(0);
        tableCommitObject.setOperations(Collections.singletonList(operation));
        tableMetaStore.insertTableCommit(context, tableIdent, tableCommitObject);

        timer.observePrevDuration("TableStoreImpl.addPartitionsInternal", "insertTableCommit");

        // insert CatalogCommit
        StringBuilder builder = new StringBuilder();
        builder.append("database name: ").append(tableName.getDatabaseName()).append(", ")
            .append("table name: ").append(tableName.getTableName()).append(", ")
            .append("partition name size: ").append(newPartitions.size());
        /*newPartitions.forEach(partition -> {
            builder.append(partition.getName()).append(", ");
        });*/

        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId(), catalogCommitEventId, commitTime, INSERT_TABLE, builder.toString());

        timer.observePrevDuration("TableStoreImpl.addPartitionsInternal", "insertCatalogCommit");

        return tableIdent;
    }

    private void addPartitionsByName(TransactionContext context, TableName tableName, List<PartitionObject> partitions,
        boolean overwrite)
        throws MetaStoreException {
        MethodStageDurationCollector.Timer timer = MethodStageDurationCollector.startCollectTimer();
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        addPartitionsInternal(context, tableName, partitions, overwrite, catalogCommitEventId);
        /*TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        // addPartitionsInternal(null, tableName, partitions, overwrite, catalogCommitEventId);
         runner.run(context -> addPartitionsInternal(context, tableName, partitions, overwrite, catalogCommitEventId))
             .getResultAndCheck(ret -> CatalogCommitHelper.catalogCommitExist(ret, catalogCommitEventId));*/

        timer.observeTotalDuration("TableStoreImpl.addPartitions", "totalLatency-success");
    }

    @Override
    public void addPartitions(TableName tableName, AddPartitionInput partitionInput) {
        MethodStageDurationCollector.Timer timer = MethodStageDurationCollector.startCollectTimer();

        List<PartitionObject> partitionObjects = TransactionRunnerUtil.transactionRunThrow(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
            TableStorageObject tableStorage = TableStorageHelper.getTableStorage(context, tableIdent);
            TableBaseObject tableBaseObject = TableBaseHelper.getTableBase(context, tableIdent);
            TableSchemaObject tableSchemaObject = TableSchemaHelper.getTableSchema(context, tableIdent);

            List<PartitionObject> partitions = new ArrayList<>();
            if (isNotTransactionAndPartitionTable(tableBaseObject, tableSchemaObject)) {
                return partitions;
            }

            if (!isViewTableType(tableBaseObject)) {
                fillPartLocationIfNotExists(partitionInput, tableStorage, tableSchemaObject);
            }

            String basedVersion = partitionInput.getBasedVersion();
            if (basedVersion == null) {
                basedVersion = VersionManagerHelper.getLatestVersion(context, tableIdent);
            }
            String finalBasedVersion = basedVersion;
            TableSchemaHistoryObject basedSchema = TableSchemaHelper
                .getLatestTableSchemaOrElseThrow(context, tableIdent, finalBasedVersion);
            Arrays.stream(partitionInput.getPartitions()).forEach(partitionBase -> {
                PartitionObject builder = makePartitionInfo(tableIdent, partitionBase, finalBasedVersion,
                    partitionInput.getFileFormat(), tableSchemaObject.getPartitionKeys());
                partitions.add(buildNewPartition(context, tableIdent, builder, basedSchema.getVersion()));
            });
            addPartitionsByName(context, tableName, partitions, partitionInput.isOverwrite());

            return null;
        }).getResult();

        timer.observeTotalDuration("PartitionServiceImpl.addPartitions", "totalLatency");
    }

    @Override
    public int addPartitionsReturnCnt(TableName tableName, AddPartitionInput partitionInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "addPartitionsReturnCnt");
    }

    @Override
    public Partition appendPartition(TableName tableName, PartitionDescriptorInput descriptor) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "appendPartition");
    }

    private TableIdent alterPartitionsInternal(TransactionContext context, TableName tableName,
        AlterPartitionInput input, String catalogCommitEventId) {
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        String latestVersion = VersionManagerHelper.getLatestVersion(tableIdent);
        TableHistoryObject latestTableHistory = TableHistoryHelper
            .getLatestTableHistoryOrElseThrow(context, tableIdent, latestVersion);

        HashMap<String, PartitionAlterContext> nameMap = new HashMap<>();
        TableSchemaObject tableSchemaObject = TableSchemaHelper.getTableSchema(context, tableIdent);
        //TableStorageObject tableStorage = TableStorageHelper.getTableStorage(context, tableIdent);
        List<String> partitionKeys = tableSchemaObject.getPartitionKeys().stream()
            .map(ColumnObject::getName).collect(Collectors.toList());
        for (PartitionAlterContext alterContext : input.getPartitionContexts()) {
            String oldPartName = PartitionUtil.makePartitionName(partitionKeys, alterContext.getOldValues());
            nameMap.put(oldPartName, alterContext);
        }

        List<PartitionObject> partitionList = PartitionHelper
            .getPartitionsByPartitionNames(context, tableIdent, latestTableHistory, new ArrayList<>(nameMap.keySet()));

        List<PartitionObject> newPartitions = new ArrayList<>(partitionList.size());
        partitionList.forEach(partition -> {
            if (nameMap.containsKey(partition.getName())) {
                PartitionAlterContext alterContext = nameMap.get(partition.getName());
                String newPartName = PartitionUtil.makePartitionName(partitionKeys, alterContext.getNewValues());
                PartitionObject newPartition = new PartitionObject(partition);
                newPartition.setName(newPartName);
                newPartition.setPartitionKeys(tableSchemaObject.getPartitionKeys());
                newPartition.setInputFormat(alterContext.getInputFormat());
                newPartition.setOutputFormat(alterContext.getOutputFormat());
                newPartition.setProperties(alterContext.getParameters());
                newPartition.setLocation(alterContext.getLocation());
                newPartition.setProperties(alterContext.getParameters());
                newPartitions.add(newPartition);
            }
        });

        // update TableReference
        /*TableReferenceObject tableReference = TableReferenceHelper.getTableReferenceOrElseThrow(context, tableIdent);
        tableReference.setDataUpdateTime(System.currentTimeMillis());*/
        tableMetaStore.upsertTableReference(context, tableIdent);

        String currentVersionStamp = VersionManagerHelper.getLatestVersion(tableIdent);
        //tableHistory commit
        TableHistoryObject baseTableHistory = TableHistoryHelper
            .getLatestTableHistory(context, tableIdent, currentVersionStamp).get();
        baseTableHistory.setPartitionSetType(TablePartitionSetType.INIT);

        updatePartitionSet(context, tableIdent, baseTableHistory.getCurSetId(), new ArrayList<>(nameMap.keySet()), newPartitions);

        //tableCommit commit
        TableCommitObject tableCommitObject = TableCommitHelper
            .getLatestTableCommit(context, tableIdent, currentVersionStamp).get();
        long commitTime = RecordStoreHelper.getCurrentTime();

        OperationObject tableOperation = new OperationObject(TableOperationType.DDL_ALTER_PARTITION,
            0, 0, 0, 0);

        tableCommitObject.setCommitTime(commitTime);
        tableCommitObject.setDroppedTime(0);
        tableCommitObject.setOperations(Collections.singletonList(tableOperation));

        tableMetaStore.insertTableCommit(context, tableIdent, tableCommitObject);

        //catalogCommit commit
        StringBuilder builder = new StringBuilder();
        builder.append("database name: ").append(tableName.getDatabaseName()).append(", ")
            .append("table name: ").append(tableName.getTableName()).append(", ");

        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId(), catalogCommitEventId, commitTime, ALTER_PARTITION, builder.toString());

        return tableIdent;
    }

    private void updatePartitionSet(TransactionContext context, TableIdent tableIdent, String curSetId, List<String> partNames, List<PartitionObject> newPartitions) {
        // PartitionHelper.insertTablePartitions(context, tableIdent, baseTableHistory, newPartitions);
        tableDataStore.deletePartitionInfoByNames(context, tableIdent, curSetId,
                partNames);
        DataPartitionSetObject dataPartitionSet = tableDataStore
                .getDataPartitionSet(context, tableIdent, curSetId);
        dataPartitionSet.setDataPartitions(newPartitions);
        tableDataStore.insertPartitionInfo(context, tableIdent, dataPartitionSet);
    }

    @Override
    public void alterPartitions(TableName tableName, AlterPartitionInput alterPartitionInput) {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        TransactionRunnerUtil.transactionRunThrow(
            context -> alterPartitionsInternal(context, tableName, alterPartitionInput, catalogCommitEventId))
            .getResultAndCheck(ret -> CatalogCommitHelper.catalogCommitExist(ret, catalogCommitEventId));
    }

    @Override
    public void alterPartition(TableName tableName, AlterPartitionInput alterPartitionInput) {
        alterPartitions(tableName, alterPartitionInput);
    }

    @Override
    public void renamePartition(TableName tableName, AlterPartitionInput alterPartitionInput) {
        alterPartitions(tableName, alterPartitionInput);
    }

    private TableIdent dropPartitionInternal(TransactionContext context, TableName tableName,
        DropPartitionInput dropPartitionInput, String catalogCommitEventId) {
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        TableHistoryObject latestTableHistory = TableHistoryHelper
            .getLatestTableHistoryOrElseThrow(context, tableIdent, VersionManagerHelper.getLatestVersion(tableIdent));
        /*List<PartitionObject> partitionList = PartitionHelper
            .getAllPartitionsFromTableHistory(context, tableIdent, latestTableHistory);
        HashSet<String> names = new HashSet<>(dropPartitionInput.getPartitionNames());

        List<PartitionObject> newPartitions = new ArrayList<>(partitionList.size());
        partitionList.forEach(partition -> {
            if (!names.contains(partition.getName())) {
                newPartitions.add(partition);
            }
        });*/
        tableDataStore.deletePartitionInfoByNames(context, tableIdent, latestTableHistory.getCurSetId(),
            new ArrayList<>(dropPartitionInput.getPartitionNames()));
        deletePartitionsColumnStatistics(context, tableName, dropPartitionInput.getPartitionNames());

        // update TableReference
        /*TableReferenceObject tableReference = TableReferenceHelper.getTableReferenceOrElseThrow(context, tableIdent);
        tableReference.setDataUpdateTime(System.currentTimeMillis());*/
        tableMetaStore.upsertTableReference(context, tableIdent);

        String currentVersionStamp = VersionManagerHelper.getLatestVersion(tableIdent);
        //tableHistory commit
        TableHistoryObject baseTableHistory = TableHistoryHelper
            .getLatestTableHistory(context, tableIdent, currentVersionStamp).get();
        baseTableHistory.setPartitionSetType(TablePartitionSetType.INIT);
        // PartitionHelper.insertTablePartitions(context, tableIdent, baseTableHistory, newPartitions);

        //tableCommit commit
        TableCommitObject tableCommitObject = TableCommitHelper
            .getLatestTableCommit(context, tableIdent, currentVersionStamp).get();
        long commitTime = RecordStoreHelper.getCurrentTime();

        OperationObject tableOperation = new OperationObject(TableOperationType.DML_DROP_PARTITION,
            0, dropPartitionInput.getPartitionNames().size(), 0, 0);

        tableCommitObject.setCommitTime(commitTime);
        tableCommitObject.setDroppedTime(0);
        tableCommitObject.setOperations(Collections.singletonList(tableOperation));

        tableMetaStore.insertTableCommit(context, tableIdent, tableCommitObject);

        //catalogCommit commit
        StringBuilder builder = new StringBuilder();
        builder.append("database name: ").append(tableName.getDatabaseName()).append(", ")
            .append("table name: ").append(tableName.getTableName()).append(", ");

        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId(), catalogCommitEventId, commitTime, DROP_PARTITION, builder.toString());

        return tableIdent;
    }

    private void deletePartitionsColumnStatistics(TransactionContext context, TableName tableName, List<String> partitionNames) {
        if (CollectionUtils.isNotEmpty(partitionNames)) {
            for (String partName: partitionNames) {
                tableDataStore.deletePartitionColumnStatistics(context, tableName, partName, null);
            }
        }
    }

    private void dropPartitioByName(TableName tableName, DropPartitionInput dropPartitionInput)
        throws MetaStoreException {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        TransactionRunnerUtil.transactionRunThrow(
            context -> dropPartitionInternal(context, tableName, dropPartitionInput, catalogCommitEventId))
            .getResultAndCheck(ret -> CatalogCommitHelper.catalogCommitExist(ret, catalogCommitEventId));
    }

    @Override
    public void dropPartition(TableName tableName, DropPartitionInput dropPartitionInput) {
        dropPartitioByName(tableName, dropPartitionInput);
    }

    @Override
    public void dropPartition(TableName tableName, DropPartitionByValuesInput dropPartitionByValuesInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "dropPartitionByValues");
    }

    @Override
    public void truncatePartitions(TableName tableName, TruncatePartitionInput truncatePartitionInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "truncatePartitions");
    }

    @Override
    public Partition[] dropPartitionsByExprs(TableName tableName,
        DropPartitionsByExprsInput dropPartitionsByExprsInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "dropPartitionsByExprs");
    }

    @Override
    public Partition[] addPartitionsBackResult(TableName tableName, AddPartitionInput partitionInput) {
        // todo ： need return partitions which added in this operate
        return new Partition[0];
    }

    private List<PartitionObject> getPartitionObjectsByFilter(TableName tableName, String filter, int maxParts) {
        MethodStageDurationCollector.Timer timer = MethodStageDurationCollector.startCollectTimer();

        //TableStorageObject tableStorage;
        return TransactionRunnerUtil.transactionReadRunThrow(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(null, tableName);
            //tableStorage = tableStore.getTableStorage(context, tableIdent);
            TableHistoryObject latestTableHistory = TableHistoryHelper
                .getLatestTableHistoryOrElseThrow(null, tableIdent, VersionManagerHelper.getLatestVersion(tableIdent));
            TableSchemaObject tableSchemaObject = tableMetaStore.getTableSchema(null, tableIdent);

            timer.observePrevDuration("PartitionServiceImpl.getPartitionsByFilter", "getTableByName");

            try {
                PartitionFilterStatesWrapper partitionFilterStatesWrapper = new PartitionFilterStatesWrapper();
                final String sqlFilter = HiveUtil.generatePartitionSqlFilter(tableIdent, tableSchemaObject, filter, partitionFilterStatesWrapper);
                List<PartitionObject> partitionsByFilter;
                if (partitionFilterStatesWrapper.filterOptimizerType != null) {
                    switch (partitionFilterStatesWrapper.filterOptimizerType) {
                        case EQUALS:
                            partitionsByFilter = tableDataStore.getPartitionsByKeyValues(context, tableIdent,
                                    latestTableHistory.getCurSetId(), partitionFilterStatesWrapper.partitionKeys,
                                    partitionFilterStatesWrapper.alignPartitionValues, maxParts);
                            break;
                        default:
                            partitionsByFilter = tableDataStore
                                    .getPartitionsByFilter(context, tableIdent, latestTableHistory.getCurSetId(), sqlFilter,
                                            maxParts);

                    }
                } else {
                    partitionsByFilter = tableDataStore
                            .getPartitionsByFilter(context, tableIdent, latestTableHistory.getCurSetId(), sqlFilter,
                                    maxParts);
                }
                partitionsByFilter.forEach(partitionObject -> partitionObject.setColumn(tableSchemaObject.getColumns()));
                return partitionsByFilter;
            } catch (Exception metaException) {
                throw new CatalogServerException(ErrorCode.PARTITION_FILTER_ILLEGAL, metaException);
            }
        }).getResult();
    }

    @Override
    public Partition[] getPartitionsByFilter(TableName tableName, PartitionFilterInput filterInput) {
        final List<PartitionObject> partitionObjectsByFilter =
            getPartitionObjectsByFilter(tableName, filterInput.getFilter(),
                filterInput.getMaxParts());
        return partitionObjectsByFilter.stream()
            .map(partitionObject -> buildTablePartitionModel(tableName, partitionObject))
            .toArray(Partition[]::new);
    }

    private Partition buildTablePartitionModel(TableName tableName, PartitionObject partition) {
        return buildTablePartitionModel(tableName, partition, false);
    }

    private List<Column> convertToColumn(List<ColumnObject> columnObjs) {
        if (CollectionUtils.isEmpty(columnObjs)) {
            return null;
        }
        List<Column> columnList = new ArrayList<>(columnObjs.size());
        for (ColumnObject columnObject : columnObjs) {
            Column tableColumnModel = new Column();
            tableColumnModel.setColumnName(columnObject.getName());
            tableColumnModel.setColType(columnObject.getDataType().toString());
            columnList.add(tableColumnModel);
        }
        return columnList;
    }

    private SerDeInfo convertToSerDeInfo(PartitionObject partition) {
        return new SerDeInfo("", partition.getSerde(), new HashMap<>());
    }

    private StorageDescriptor fillStorageDescriptorByPartitionObject(PartitionObject partition) {
        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(partition.getLocation());
//        sd.setColumns(convertToColumn(partition.getColumn()));
//        sd.setInputFormat(partition.getInputFormat());
//        sd.setOutputFormat(partition.getOutputFormat());
//        sd.setSerdeInfo(convertToSerDeInfo(partition));
        return sd;
    }

    private Partition buildTablePartitionModel(TableName tableName, PartitionObject partition,
        boolean fillFileDetails) {
        Partition tablePartition = new Partition();
        tablePartition.setStorageDescriptor(fillStorageDescriptorByPartitionObject(partition));
        //tablePartition.setFileIndexUrl(partition.getPartitionIndexUrl());
        tablePartition.setPartitionValues(PartitionUtil.convertEscapePartNameToVals(partition.getName()));
        tablePartition.setTableName(tableName.getTableName());
        tablePartition.setDatabaseName(tableName.getDatabaseName());
        tablePartition.setCatalogName(tableName.getCatalogName());
        tablePartition.setCreateTime(partition.getStartTime());
        //tablePartition.setLastAccessTime(partition.getEndTime());
        tablePartition.setParameters(partition.getProperties());
        if (fillFileDetails) {
            List<DataFile> tableDataFiles = new ArrayList<>();
            for (DataFileObject dataFile : partition.getFile()) {
                DataFile tableDataFile = new DataFile();
                tableDataFile.setLength(dataFile.getLength());
                tableDataFile.setFileName(dataFile.getFileName());
                tableDataFile.setRowCount(dataFile.getRowCount());
                tableDataFile.setOffset(dataFile.getOffset());
                tableDataFiles.add(tableDataFile);
            }
            tablePartition.setDataFiles(tableDataFiles);
        }

        return tablePartition;
    }

    @Override
    public Partition[] getPartitionsByNames(TableName tableName, PartitionFilterInput filterInput) {
        List<String> normalizationPartitionNames = getNormalizationPartitionNames(filterInput.getPartNames());
        return TransactionRunnerUtil.transactionReadRunThrow(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
            TableHistoryObject latestTableHistory = TableHistoryHelper
                    .getLatestTableHistoryOrElseThrow(context, tableIdent,
                            VersionManagerHelper.getLatestVersion(tableIdent));
            final TableSchemaObject tableSchema = tableMetaStore.getTableSchema(context, tableIdent);
            final List<ColumnObject> columns = tableSchema.getColumns();
            final List<PartitionObject> partitionObjects = tableDataStore
                    .getPartitionsByPartitionNames(context, tableIdent, latestTableHistory.getSetIds(),
                            latestTableHistory.getCurSetId(), normalizationPartitionNames,
                            filterInput.getMaxParts());
            return partitionObjects.stream().map(partitionObject -> {
                partitionObject.setColumn(columns);
                return buildTablePartitionModel(tableName, partitionObject);
            })
                .toArray(Partition[]::new);
        }).getResult();
    }

    private List<String> getNormalizationPartitionNames(String[] partNames) {
        List<String> partitionNames = new ArrayList<>();
        if (partNames != null && partNames.length > 0) {
            for (int i = 0; i < partNames.length; i++) {
                partitionNames.add(PartitionUtil.escapePartitionName(partNames[i]));
            }
        }
        return partitionNames;
    }

    @Override
    public Partition getPartitionWithAuth(TableName tableName, GetPartitionWithAuthInput partitionInput) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
            //tableStorage = tableStore.getTableStorage(context, tableIdent);
            final TableSchemaObject tableSchema = tableMetaStore.getTableSchema(context, tableIdent);
            if (tableSchema.getPartitionKeys().size() != partitionInput.getPartVals().size()) {
                return null;
            }
            final Optional<TableHistoryObject> latestTableHistoryOpt = TableHistoryHelper
                .getLatestTableHistory(context, tableIdent,
                    VersionManagerHelper.getLatestVersion(tableIdent));

            if (!latestTableHistoryOpt.isPresent()) {
                return null;
            }

            List<String> partitionKeys = tableSchema.getPartitionKeys().stream().map(ColumnObject::getName)
                .collect(Collectors.toList());
            String partitionName = PartitionUtil.makePartitionName(partitionKeys, partitionInput.getPartVals());
            final ArrayList<String> partitionNames = new ArrayList<>();
            partitionNames.add(partitionName);
            final List<PartitionObject> partitionObjects = tableDataStore
                .getPartitionsByPartitionNames(context, tableIdent, latestTableHistoryOpt.get().getSetIds(),
                    latestTableHistoryOpt.get().getCurSetId(), partitionNames, -1);
            if (partitionObjects.size() > 0) {
                partitionObjects.get(0).setColumn(tableSchema.getColumns());
                return buildTablePartitionModel(tableName, partitionObjects.get(0));
            } else {
                return null;
            }
        }).getResult();
    }

    private List<PartitionObject> listPartitions(TableName tableName, Integer maxParts) throws MetaStoreException {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        return TransactionRunnerUtil.transactionReadRunThrow(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
            List<PartitionObject> partitionList = new ArrayList<>();
            Optional<TableHistoryObject> latestTableHistory = TableHistoryHelper
                    .getLatestTableHistory(context, tableIdent,
                            VersionManagerHelper.getLatestVersion(tableIdent));
            if (!latestTableHistory.isPresent()) {
                return partitionList;
            }
            TableSchemaObject tableSchema = tableMetaStore.getTableSchema(context, tableIdent);
            final List<ColumnObject> columns = tableSchema.getColumns();
            partitionList = tableDataStore.listTablePartitions(context, tableIdent,
                    latestTableHistory.get().getSetIds(),
                    latestTableHistory.get().getCurSetId(), maxParts);
            partitionList.forEach(partitionObject -> partitionObject.setColumn(columns));
            return partitionList;
        }).getResult();
    }

    @Override
    public String[] listPartitionNames(TableName tableName, PartitionFilterInput filterInput, boolean escape) {
        List<String> result = TransactionRunnerUtil.transactionReadRunThrow(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
            TableSchemaObject tableSchema = TableSchemaHelper.getTableSchema(context, tableIdent);
            if (tableSchema != null && tableSchema.getPartitionKeys() != null) {
                List<String> partitionKeys = tableSchema.getPartitionKeys().stream().map(ColumnObject::getName).collect(toList());
                return tableDataStore.listTablePartitionNames(context, tableIdent, filterInput, partitionKeys);
            }
            return new ArrayList<String>();
        }).getResult();
        if (!escape) {
            return result.stream().map(PartitionUtil::unescapePartitionName).collect(Collectors.toList())
                    .toArray(new String[]{});
        } else {
            return result.toArray(new String[]{});
        }
    }

    @Override
    public String[] listPartitionNamesByFilter(TableName tableName, PartitionFilterInput filterInput) {
        final List<PartitionObject> partitionObjectsByFilter =
            getPartitionObjectsByFilter(tableName, filterInput.getFilter(),
                filterInput.getMaxParts());
        return partitionObjectsByFilter.stream().map(x -> PartitionUtil.unescapePartitionName(x.getName()))
            .toArray(String[]::new);
    }

    @Override
    public Partition[] listPartitions(TableName tableName, PartitionFilterInput filterInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "listByValue");
    }

    private List<PartitionObject> getPartitionObjectsByValues(TableName tableName, List<String> values, int maxParts) {
        return TransactionRunnerUtil.transactionReadRunThrow(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
            final TableSchemaObject tableSchema = tableMetaStore.getTableSchema(context, tableIdent);
            List<String> partitionKeys = tableSchema.getPartitionKeys().stream().map(ColumnObject::getName)
                    .collect(Collectors.toList());
            TableHistoryObject latestTableHistory = TableHistoryHelper
                    .getLatestTableHistoryOrElseThrow(context, tableIdent,
                            VersionManagerHelper.getLatestVersion(tableIdent));
            final List<PartitionObject> partitionObjects = tableDataStore
                    .getPartitionsByKeyValues(context, tableIdent, latestTableHistory.getCurSetId(), partitionKeys, values,
                            maxParts);
            partitionObjects.forEach(partitionObject -> partitionObject.setColumn(tableSchema.getColumns()));
            return partitionObjects;
        }).getResult();
    }

    @Override
    public String[] listPartitionNamesPs(TableName tableName, PartitionFilterInput filterInput) {

        final List<PartitionObject> partitionObjects =
            getPartitionObjectsByValues(tableName, Arrays.asList(filterInput.getValues()), filterInput.getMaxParts());
        return partitionObjects.stream().map(x -> PartitionUtil.unescapePartitionName(x.getName()))
            .toArray(String[]::new);
    }

    @Override
    public Partition[] listPartitionsPsWithAuth(TableName tableName, GetPartitionsWithAuthInput filterInput) {
        final List<PartitionObject> partitionObjects =
            getPartitionObjectsByValues(tableName, filterInput.getValues(), filterInput.getMaxParts());
        return partitionObjects.stream().map(partitionObject -> buildTablePartitionModel(tableName, partitionObject))
            .toArray(Partition[]::new);
    }


    @Override
    public Partition[] listPartitionsByExpr(TableName tableName, GetPartitionsByExprInput filterInput) {
        final byte[] expr = filterInput.getExpr();
        TableIdent tableIdent = TableObjectHelper.getTableIdent(null, tableName);
        TableSchemaObject tableSchemaObject = tableMetaStore.getTableSchema(null, tableIdent);
        TableHistoryObject latestTableHistory = TableHistoryHelper
                .getLatestTableHistoryOrElseThrow(null, tableIdent, VersionManagerHelper.getLatestVersion(tableIdent));

        final String sqlFilter = HiveUtil.generatePartitionSqlFilter(tableIdent, tableSchemaObject, expr);

        if (sqlFilter != null) {
            final List<PartitionObject> partitionsByFilter = tableDataStore
                    .getPartitionsByFilter(null, tableIdent, latestTableHistory.getCurSetId(), sqlFilter, filterInput.getMaxParts());
            partitionsByFilter.forEach(partitionObject -> partitionObject.setColumn(tableSchemaObject.getColumns()));
           return partitionsByFilter.stream()
                    .map(partitionObject -> buildTablePartitionModel(tableName, partitionObject))
                    .toArray(Partition[]::new);
        } else {
            log.warn("could not push down");
            final List<ColumnObject> partitionKeys = tableSchemaObject.getPartitionKeys();
            final ArrayList<String> partitionNames = new ArrayList<>(
                    Arrays.asList(listPartitionNames(tableName, new PartitionFilterInput(), false)));
            final boolean hasUnknownPartitions =
                    HiveUtil.filterPartitionsByExpr(partitionKeys, expr, filterInput.getDefaultPartitionName(), partitionNames);
            final PartitionFilterInput partitionFilterInput = new PartitionFilterInput();
            partitionFilterInput.setPartNames(partitionNames.toArray(new String[]{}));
            partitionFilterInput.setMaxParts(0);
            return getPartitionsByNames(tableName, partitionFilterInput);
        }
    }

    @Override
    public TraverseCursorResult<List<Partition>> showTablePartition(TableName tableName, int maxResults,
        String pageToken, FilterInput filterInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "showTablePartition");
    }

    @Override
    public Partition[] listPartitions(TableName tableName, FilterInput filterInput) {
        return listPartitions(tableName, filterInput.getLimit())
            .stream().map(partitionObject -> buildTablePartitionModel(tableName, partitionObject))
            .toArray(Partition[]::new);
    }

    @Override
    public TableStats getTableStats(TableName tableName) {
        return null;
    }

    /**
     * Get the object based on partitionName, partName may need to be escaped.
     *
     * @param tableName
     * @param partitionName TODO rest call need escape.
     * @return
     */
    @Override
    public Partition getPartitionByName(TableName tableName, String partitionName) {
        PartitionObject partitionObject = TransactionRunnerUtil.transactionRunThrow(context -> {
            return getPartitionByNameInternal(context, tableName, partitionName);
        }).getResult();
        if (partitionObject != null) {
            return buildTablePartitionModel(tableName, partitionObject);
        }
        return null;
    }

    private PartitionObject getPartitionByNameInternal(TransactionContext context,
            TableName tableName, String partitionNames) {
        List<PartitionObject> partitionObjects = getPartitionByNameInternal(context, tableName,
                Lists.newArrayList(partitionNames));
        if (CollectionUtils.isNotEmpty(partitionObjects)) {
            return partitionObjects.get(0);
        }
        return null;
    }

    private List<PartitionObject> getPartitionByNameInternal(TransactionContext context,
            TableName tableName, List<String> partitionNames) {
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        TableHistoryObject latestTableHistory = TableHistoryHelper
                .getLatestTableHistoryOrElseThrow(context, tableIdent,
                        VersionManagerHelper.getLatestVersion(tableIdent));
        TableSchemaObject tableSchema = tableMetaStore.getTableSchema(context, tableIdent);
        List<PartitionObject> partitionObjects = tableDataStore
                .getPartitionsByPartitionNames(context, tableIdent, latestTableHistory.getSetIds(),
                        latestTableHistory.getCurSetId(), partitionNames, -1);
        return partitionObjects.stream().peek(x -> {
            x.setColumn(tableSchema.getColumns());
            x.setPartitionKeys(tableSchema.getPartitionKeys());
        }).collect(toList());
    }

    @Override
    public Partition getPartitionByValue(TableName tableName, List<String> partVals) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getPartitionByValue");
    }

    @Override
    public boolean updatePartitionColumnStatistics(TableName tableName, ColumnStatisticsInput columnStatisticsInput) {
        ColumnStatisticsDesc statisticsDesc = columnStatisticsInput.getColumnStatistics().getColumnStatisticsDesc();
        Table table = ensureGetTable(tableName);
        List<ColumnStatisticsObj> statsObjs = columnStatisticsInput.getColumnStatistics().getColumnStatisticsObjs();
        List<String> colNames = TableUtil.getColNamesFromColumnStatistics(statsObjs);
        TableUtil.validateTableColumns(table, colNames);
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            return updatePartitionColumnStatisticsInternal(context, table, tableName, colNames, statsObjs, statisticsDesc, columnStatisticsInput.getPartVals());
        }).getResult();
    }

    private boolean updatePartitionColumnStatisticsInternal(TransactionContext context, Table table,
            TableName tableName, List<String> colNames,
            List<ColumnStatisticsObj> statsObjs, ColumnStatisticsDesc statisticsDesc,
            List<String> partVals) {
        String partitionName = PartitionUtil.makePartitionName(
                table.getPartitionKeys().stream().map(Column::getColumnName).collect(toList()), partVals);
        PartitionObject partitionObject = ensureGetPartition(context, tableName, partitionName);
        List<ColumnStatisticsObject> columnStatisticsObjects = new ArrayList<>();
        Map<String, ColumnStatisticsObj> tableColumnStatisticsMap = getPartitionColumnStatisticsMap(context, tableName, partitionName, colNames);
        for (ColumnStatisticsObj statsObj: statsObjs) {
            if (tableColumnStatisticsMap.containsKey(statsObj.getColName())) {
                statsObj = tableColumnStatisticsMap.get(statsObj.getColName());
            }
            columnStatisticsObjects.add(TableObjectConvertHelper.toPartitionColumnStatisticsObject(table, statsObj, partitionName, partitionObject.getPartitionId(), statisticsDesc.getLastAnalyzed()));
        }
        if (CollectionUtils.isNotEmpty(columnStatisticsObjects)) {
            tableDataStore.updatePartitionColumnStatistics(context, tableName, columnStatisticsObjects);
            alterPartitionStatisticsParameters(context, tableName, table.getTableId(), partitionObject, partitionName, colNames);
        }
        return true;
    }

    private void alterPartitionStatisticsParameters(TransactionContext context,
            TableName tableName, String tableId, PartitionObject partitionObject,
            String partName, List<String> colNames) {
        if (partitionObject.getProperties() == null) {
            partitionObject.setProperties(Maps.newLinkedHashMap());
        } else {
            partitionObject.setProperties(Maps.newLinkedHashMap(partitionObject.getProperties()));
        }
        // TODO Temporarily keep consistent with hive
        StatsSetupConst.setColumnStatsState(partitionObject.getProperties(), colNames);
        TableIdent tableIdent = new TableIdent();
        tableIdent.setTableId(tableId);
        tableIdent.setProjectId(tableName.getProjectId());
        updatePartitionSet(context, tableIdent, partitionObject.getSetId(), Lists.newArrayList(partName), Lists.newArrayList(partitionObject));
    }

    private PartitionObject ensureGetPartition(TransactionContext context, TableName tableName, String partName) {
        PartitionObject partitionObject = getPartitionByNameInternal(context, tableName,
                partName);
        if (partitionObject == null) {
            throw new MetaStoreException(ErrorCode.PARTITION_NAME_NOT_FOUND, partName);
        }
        return partitionObject;
    }

    private Map<String, ColumnStatisticsObj> getPartitionColumnStatisticsMap(
            TransactionContext context, TableName tableName, String partName, List<String> colNames) {
        List<ColumnStatisticsObject> statisticsObjectList = getPartitionColumnStatisticInternal(context, tableName,
                Lists.newArrayList(partName), colNames);
        return TableUtil.getColumnStatisticsMap(statisticsObjectList.stream().map(TableObjectConvertHelper::toColumnStatisticsData)
                .collect(toList()).toArray(new ColumnStatisticsObj[statisticsObjectList.size()]));
    }

    @Override
    public PartitionStatisticData getPartitionColumnStatistics(TableName tableName, List<String> partNames,
        List<String> colNames) {
        if (CollectionUtils.isEmpty(colNames) || CollectionUtils.isEmpty(partNames)) {
            return new PartitionStatisticData(new LinkedHashMap<>());
        }
        //Table table = ensureGetTable(tableName);
        //TableUtil.validateTableColumns(table, colNames);
        List<ColumnStatisticsObject> list = TransactionRunnerUtil.transactionRunThrow(context -> {
            return getPartitionColumnStatisticInternal(context, tableName, partNames, colNames);
        }).getResult();
        return columnStatisticsObjectToPartitionStatisticsData(list);
    }

    private PartitionStatisticData columnStatisticsObjectToPartitionStatisticsData(List<ColumnStatisticsObject> list) {
        Map<String, List<ColumnStatisticsObj>> statisticsMap = Maps.newLinkedHashMapWithExpectedSize(list.size());
        if (CollectionUtils.isNotEmpty(list)) {
            for (ColumnStatisticsObject cso: list) {
                if (statisticsMap.containsKey(cso.getPartitionName())) {
                    statisticsMap.get(cso.getPartitionName()).add(TableObjectConvertHelper.toColumnStatisticsData(cso));
                } else {
                    statisticsMap.put(cso.getPartitionName(), Lists.newArrayList(TableObjectConvertHelper.toColumnStatisticsData(cso)));
                }
            }
        }
        return new PartitionStatisticData(statisticsMap);
    }

    private List<ColumnStatisticsObject> getPartitionColumnStatisticInternal(TransactionContext context, TableName tableName,
            List<String> partNames, List<String> colNames) {
        //partNames = partNames.stream().map(PartitionUtil::escapePartitionName).collect(toList());
        return tableDataStore.getPartitionColumnStatistics(context, tableName, partNames, colNames);
    }

    private Table ensureGetTable(TableName tableName) {
        Table table = tableService.getTableByName(tableName);
        if (table == null) {
            throw new MetaStoreException("Specified database/table does not exist : "
                    + tableName.getDatabaseName() + "." + tableName.getTableName());
        }
        return table;
    }

    @Override
    public void deletePartitionColumnStatistics(TableName tableName, String partName, String columnName) {
        ensureGetTable(tableName);
        TransactionRunnerUtil.transactionRunThrow(context -> {
            ensureGetPartition(context, tableName, partName);
            tableDataStore.deletePartitionColumnStatistics(context, tableName, partName, columnName);
            return null;
        }).getResult();
    }

    @Override
    public void setPartitionColumnStatistics(TableName tableName, SetPartitionColumnStatisticsInput input) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "setPartitionColumnStatistics");
    }

    @Override
    public AggrStatisticData getAggrColStatsFor(TableName tableName, List<String> partNames, List<String> colNames) {
        AggrStatisticData aggrStatisticData = new AggrStatisticData(new ArrayList<ColumnStatisticsObj>(), 0L);
        if (CollectionUtils.isEmpty(colNames) || CollectionUtils.isEmpty(partNames)) {
            return aggrStatisticData;
        }
        //Table table = ensureGetTable(tableName);
        //TableUtil.validateTableColumns(table, colNames);
        List<ColumnStatisticsAggrObject> aggrObjects = TransactionRunnerUtil.transactionRunThrow(context -> {
            aggrStatisticData.setPartsFound(tableDataStore.getFoundPartNums(context, tableName, partNames, colNames));
            return tableDataStore.getAggrColStatsFor(context, tableName, partNames, colNames);
        }).getResult();
        aggrObjects.forEach(x -> {
            aggrStatisticData.getColumnStatistics().add(TableObjectConvertHelper.toColumnStatisticsObjForAggrObject(x, false, 0.0f));
        });
        return aggrStatisticData;
    }

    @Override
    public boolean doesPartitionExists(TableName tableName, PartitionValuesInput partitionValuesInput) {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
            final TableSchemaObject tableSchema = tableMetaStore.getTableSchema(context, tableIdent);
            List<String> partitionKeys = tableSchema.getPartitionKeys().stream().map(ColumnObject::getName)
                .collect(Collectors.toList());
            String partitionName = PartitionUtil
                .makePartitionName(partitionKeys, partitionValuesInput.getPartitionValues());
            return tableDataStore.doesPartitionExists(context, tableIdent, partitionName);
        }).getResult();
    }

    @Override
    public Integer getTablePartitionCount(TableName tableName, PartitionFilterInput filterInput) {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
            TableSchemaObject tableSchemaObject = tableMetaStore.getTableSchema(context, tableIdent);
            Integer count = 0;
            try {
                PartitionFilterStatesWrapper partitionFilterStatesWrapper = new PartitionFilterStatesWrapper();
                final String sqlFilter = HiveUtil.generatePartitionSqlFilter(tableIdent, tableSchemaObject, filterInput.getFilter(), partitionFilterStatesWrapper);
                if (partitionFilterStatesWrapper.filterOptimizerType != null) {
                    switch (partitionFilterStatesWrapper.filterOptimizerType) {
                        case EQUALS:
                            count = tableDataStore.getTablePartitionCountByKeyValues(context, tableIdent,
                                    partitionFilterStatesWrapper.partitionKeys,
                                    partitionFilterStatesWrapper.alignPartitionValues);
                            break;
                        default:
                            count = tableDataStore
                                    .getTablePartitionCountByFilter(context, tableIdent, sqlFilter);
                    }
                } else {
                    count = tableDataStore
                            .getTablePartitionCountByFilter(context, tableIdent, sqlFilter);
                }

            } catch (Exception metaException) {
                throw new CatalogServerException(ErrorCode.PARTITION_FILTER_ILLEGAL, metaException);
            }
            return count;
        }).getResult();
    }

    @Override
    public String getLatestPartitionName(TableName tableName) {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
            String latestPartitionName = tableDataStore.getLatestPartitionName(context, tableIdent);
            if (latestPartitionName != null) {
                return PartitionUtil.unescapePartitionName(latestPartitionName);
            }
            return null;
        }).getResult();
    }
}
