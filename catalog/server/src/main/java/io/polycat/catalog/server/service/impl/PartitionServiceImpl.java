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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.polycat.catalog.server.util.TransactionFrameRunner;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.*;
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
import io.polycat.catalog.common.plugin.request.input.TruncatePartitionInput;
import io.polycat.catalog.common.utils.CatalogToken;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.GsonUtil;
import io.polycat.catalog.common.utils.PartitionUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.service.api.PartitionService;
import io.polycat.catalog.store.api.*;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.common.expression.Expression;
import io.polycat.common.expression.ExpressionParser;
import io.polycat.metrics.MethodStageDurationCollector;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import static io.polycat.catalog.common.Operation.ALTER_PARTITION;
import static io.polycat.catalog.common.Operation.DROP_PARTITION;
import static io.polycat.catalog.common.Operation.INSERT_TABLE;

// @Configuration
// @ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class PartitionServiceImpl implements PartitionService {
    private static final Logger log = Logger.getLogger(PartitionServiceImpl.class);

    @Autowired
    private TableMetaStore tableMetaStore;

    @Autowired
    private TableDataStore tableDataStore;

    @Autowired
    private CatalogStore catalogStore;


    private static int TABLE_STORE_MAX_RETRY_NUM = 256;

    // a maximum of 2048 data partition set records can be stored in a index partition set.
    // the value is not final because the test case needs to modify it.
    private static int indexStoredMaxNumber = 2048;

    // 97280 = 95 x 1024, a data partition set can store up to 97280 bytes.
    // the value is not final because the test case needs to modify it.
    private static int dataStoredMaxSize = 97280;
    private static final String TABLE_STORE_CHECKSUM = "catalogTableStore";

    private static class PartitionServiceImplHandler {

        private static final PartitionServiceImpl INSTANCE = new PartitionServiceImpl();
    }

    public static PartitionServiceImpl getInstance() {
        return PartitionServiceImplHandler.INSTANCE;
    }

    private void insertPartitionForInit(TransactionContext context, TableIdent tableIdent, PartitionObject newPartition,
        TableHistoryObject tableHistoryObject) {
        DataPartitionSetObject dataPartitionSet = makeDataPartitionSet(tableIdent, newPartition);
        tableDataStore.insertDataPartitionSet(context, tableIdent, dataPartitionSet);
        tableHistoryObject.clearSetIds();
        tableHistoryObject.setPartitionSetType(TablePartitionSetType.DATA);
        tableHistoryObject.setCurSetId(dataPartitionSet.getSetId());
    }

    private void insertPartitionFromDataToIndex(TransactionContext context, TableIdent tableIdent,
        PartitionObject newPartition,
        TableHistoryObject tableHistoryObject) {
        DataPartitionSetObject dataPartitionSet = makeDataPartitionSet(tableIdent, newPartition);
        tableDataStore.insertDataPartitionSet(context, tableIdent, dataPartitionSet);

        List<String> setIds = tableHistoryObject.getSetIds();
        setIds.add(tableHistoryObject.getCurSetId());
        IndexPartitionSetObject indexPartitionSet = makeIndexPartitionSet(tableIdent, dataPartitionSet.getSetId(),
            setIds);
        tableDataStore.insertIndexPartitionSet(context, tableIdent, indexPartitionSet);

        tableHistoryObject.clearSetIds();
        tableHistoryObject.setPartitionSetType(TablePartitionSetType.INDEX);
        tableHistoryObject.setCurSetId(indexPartitionSet.getSetId());
    }

    private void insertPartitionForData(TransactionContext context, TableIdent tableIdent, PartitionObject newPartition,
        TableHistoryObject tableHistoryObject, boolean isFirst) {
        DataPartitionSetObject dataPartitionSet = tableDataStore
            .getDataPartitionSet(context, tableIdent, tableHistoryObject.getCurSetId());
        // if the data partition set space is insufficient, apply for a new data partition set.
        if (tableDataStore.getParitionSerializedSize(newPartition) + dataPartitionSet.getPartitionsSize()
            >= dataStoredMaxSize) {
            if (tableHistoryObject.getSetIds().size() >= indexStoredMaxNumber) {
                insertPartitionFromDataToIndex(context, tableIdent, newPartition, tableHistoryObject);
            } else {
                DataPartitionSetObject dataPartitionSetNew = makeDataPartitionSet(tableIdent, newPartition);
                tableDataStore.insertDataPartitionSet(context, tableIdent, dataPartitionSetNew);
                String curSetId = tableHistoryObject.getCurSetId();
                tableHistoryObject.addSetId(curSetId);
                tableHistoryObject.setCurSetId(dataPartitionSetNew.getSetId());
            }
        } else {
            if (isFirst) {
                DataPartitionSetObject dataPartitionSetNew = makeDataPartitionSet(dataPartitionSet, newPartition);
                tableDataStore.insertDataPartitionSet(context, tableIdent, dataPartitionSetNew);
                tableHistoryObject.setCurSetId(dataPartitionSetNew.getSetId());
            } else {
                DataPartitionSetObject dataPartitionSetNew = makeDataPartitionSet(dataPartitionSet, newPartition);
                tableDataStore.updateDataPartitionSet(context, tableIdent, dataPartitionSetNew);
            }
        }
    }

    private void insertPartitionForIndex(TransactionContext context, TableIdent tableIdent,
        PartitionObject newPartition,
        TableHistoryObject tableHistoryObject, boolean isFirst) {
        IndexPartitionSetObject indexPartitionSet = tableDataStore
            .getIndexPartitionSet(context, tableIdent, tableHistoryObject.getCurSetId());
        DataPartitionSetObject dataPartitionSet = tableDataStore
            .getDataPartitionSet(context, tableIdent, indexPartitionSet.getCurSetId());

        // if the dataPartitionset space is insufficient, apply for a new dataPartitionset.
        if (tableDataStore.getParitionSerializedSize(newPartition) + dataPartitionSet.getPartitionsSize()
            >= dataStoredMaxSize) {
            // if the index partition set space is insufficient, apply for a new index partition set.
            if (indexPartitionSet.getSetIds().size() >= indexStoredMaxNumber) {
                DataPartitionSetObject dataPartitionSetNew = makeDataPartitionSet(tableIdent, newPartition);
                tableDataStore.insertDataPartitionSet(context, tableIdent, dataPartitionSetNew);
                List<String> setIds = new ArrayList<>();// set empty list
                IndexPartitionSetObject indexPartitionSetNew = makeIndexPartitionSet(tableIdent,
                    dataPartitionSetNew.getSetId(), setIds);
                tableDataStore.insertIndexPartitionSet(context, tableIdent, indexPartitionSet);
                tableHistoryObject.addSetId(indexPartitionSet.getSetId());
                tableHistoryObject.setCurSetId(indexPartitionSetNew.getSetId());
            } else {
                DataPartitionSetObject dataPartitionSetNew = makeDataPartitionSet(tableIdent, newPartition);
                tableDataStore.insertDataPartitionSet(context, tableIdent, dataPartitionSetNew);
                if (isFirst) {
                    IndexPartitionSetObject indexPartitionSetNew = makeIndexPartitionSet(tableIdent,
                        dataPartitionSetNew.getSetId(),
                        indexPartitionSet.getSetIds());
                    tableDataStore.insertIndexPartitionSet(context, tableIdent, indexPartitionSetNew);
                    tableHistoryObject.setCurSetId(indexPartitionSetNew.getSetId());
                } else {
                    List<String> setIds = new ArrayList<>();
                    setIds.add(indexPartitionSet.getCurSetId());
                    IndexPartitionSetObject indexPartitionSetNew = makeIndexPartitionSet(tableIdent,
                        dataPartitionSetNew.getSetId(), setIds);
                    tableDataStore.updateIndexPartitionSet(context, tableIdent, indexPartitionSetNew);
                }
            }
        } else {
            if (isFirst) {
                DataPartitionSetObject dataPartitionSetNew = makeDataPartitionSet(dataPartitionSet, newPartition);
                tableDataStore.insertDataPartitionSet(context, tableIdent, dataPartitionSetNew);
                IndexPartitionSetObject indexPartitionSetNew = makeIndexPartitionSet(tableIdent,
                    dataPartitionSetNew.getSetId(),
                    indexPartitionSet.getSetIds());
                tableDataStore.insertIndexPartitionSet(context, tableIdent, indexPartitionSetNew);
                tableHistoryObject.setCurSetId(indexPartitionSetNew.getSetId());
            } else {
                DataPartitionSetObject dataPartitionSetNew = makeDataPartitionSet(dataPartitionSet, newPartition);
                tableDataStore.updateDataPartitionSet(context, tableIdent, dataPartitionSetNew);
            }
        }
    }

    /**
     * INIT : inserting Partitions to tablehistory for the first time. DATA : setids in the current tablehistory stores
     * dataPartition sets. INDEX : setids in the current tablehistory stores indexPartition sets. isFirst : When
     * Partition is inserted in batches, the dataPartitionset and indexPartitionsetdoes not need to be applied for
     * repeatedly.
     */

    private void insertPartitionByType(TransactionContext context, TableIdent tableIdent, PartitionObject newPartition,
        TableHistoryObject tableHistoryObject, boolean isFirst) {

        MethodStageDurationCollector.Timer timer = MethodStageDurationCollector.startCollectTimer();

        switch (tableHistoryObject.getPartitionSetType()) {
            case INIT:
                insertPartitionForInit(context, tableIdent, newPartition, tableHistoryObject);
                break;
            case DATA:
                insertPartitionForData(context, tableIdent, newPartition, tableHistoryObject, isFirst);
                break;
            case INDEX:
                insertPartitionForIndex(context, tableIdent, newPartition, tableHistoryObject, isFirst);
                break;
            default:
                throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
        timer.observeTotalDuration("TableStoreImpl.insertPartitionByType", "totalLatency");
    }

    private void insertTablePartition(TransactionContext context, TableIdent tableIdent, PartitionObject newPartition,
        TableHistoryObject latestDataVersion) {

        String version = VersionManagerHelper.getNextVersion(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId());

        if (newPartition.getType() == TablePartitionType.EXTERNAL) {
            DataPartitionSetObject dataPartitionSet = new DataPartitionSetObject();
            dataPartitionSet.setSetId(UuidUtil.generateId());
            dataPartitionSet.addDataPartition(newPartition);
            tableDataStore.insertDataPartitionSet(context, tableIdent, dataPartitionSet);


            TableHistoryObject tableHistory = new TableHistoryObject(latestDataVersion);
            tableHistory.clearSetIds();
            tableHistory.setPartitionSetType(TablePartitionSetType.DATA);
            tableHistory.addSetId(dataPartitionSet.getSetId());
            tableHistory.setCurSetId(dataPartitionSet.getSetId());
            tableHistory.setVersion(null);

            tableDataStore.insertTableHistory(context, tableIdent, version, tableHistory);
        } else {
            TableHistoryObject tableHistory = new TableHistoryObject(latestDataVersion);
            insertPartitionByType(context, tableIdent, newPartition, tableHistory, true);
            tableDataStore.insertTableHistory(context, tableIdent, version, tableHistory);
        }
    }


    private TableIdent insertPartitionInternal(TransactionContext context,
        TableName tableName, AddPartitionInput partitionInput,  boolean overwrite, String catalogCommitEventId) {
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        TableBaseObject tableBaseObject = TableBaseHelper.getTableBase(context, tableIdent);
        TableSchemaObject tableSchemaObject = TableSchemaHelper.getTableSchema(context, tableIdent);
        TableStorageObject tableStorage = TableStorageHelper.getTableStorage(context, tableIdent);

        if (isNotTransactionAndPartitionTable(tableBaseObject, tableSchemaObject)) {
            return null;
        }

        fillPartLocationIfNotExists(partitionInput, tableStorage, tableSchemaObject);
        List<String> partitionKeys = tableSchemaObject.getPartitionKeys().stream().map(ColumnObject::getName)
                .collect(Collectors.toList());

        PartitionObject partitionObject = makePartitionInfo(tableIdent, partitionInput.getPartitions()[0],
                partitionInput.getBasedVersion(), partitionInput.getFileFormat(), partitionKeys);

        PartitionObject newPartition = buildNewPartition(context, tableIdent, partitionObject);

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

    private PartitionObject buildNewPartition(TransactionContext context, TableIdent tableIdent,
        PartitionObject partition)
        throws MetaStoreException {
        PartitionObject partitionObject = new PartitionObject(partition);
        partitionObject.setPartitionId(UuidUtil.generateId());

        if (!partition.getSchemaVersion().isEmpty()) {
            String versionstamp = partition.getSchemaVersion();
            TableSchemaHistoryObject basedSchema = TableSchemaHelper
                .getLatestTableSchemaOrElseThrow(context, tableIdent, versionstamp);
            partitionObject.setSchemaVersion(basedSchema.getVersion());
        } else {
            partitionObject.setColumn(partition.getColumn());
        }

        return partitionObject;
    }

    /**
     * insert a partition without any conflict
     */
    private void addPartition(TableName tableName, AddPartitionInput partitionInput, boolean overwrite)
            throws MetaStoreException {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        runner.run(context -> insertPartitionInternal(context, tableName, partitionInput, overwrite,
                catalogCommitEventId)).getResultAndCheck(ret -> CatalogCommitHelper
                .catalogCommitExist(ret, catalogCommitEventId));

    }

    @Override
    public void addPartition(TableName tableName, AddPartitionInput partitionInput) {
        addPartition(tableName, partitionInput, partitionInput.isOverwrite());
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


    private PartitionObject makePartitionInfo(TableIdent tableIdent, PartitionInput partitionBase, String basedVersion,
        String fileFormat,
        List<String> partitionKeys) {
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
        partitionObject.setName(PartitionUtil.makePartitionName(partitionKeys, partitionBase.getPartitionValues()));
        return partitionObject;
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

    private boolean isNotTransactionAndPartitionTable(TableBaseObject tableBaseObject,
        TableSchemaObject tableSchemaObject) {
        return !tableBaseObject.isLmsMvcc() && tableSchemaObject.getPartitionKeys().size() == 0;
        //return !tableBaseObject.isLmsMvcc() && tableBaseObject.getPartitions().size() == 0;
    }

    private void dropPartitioByName(TableName tableName, DropPartitionInput dropPartitionInput)
        throws MetaStoreException {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        runner.run(context -> {
            return dropPartitionInternal(context, tableName, dropPartitionInput, catalogCommitEventId);
        }).getResultAndCheck(ret -> CatalogCommitHelper.catalogCommitExist(ret, catalogCommitEventId));
    }


    private TableIdent dropPartitionInternal(TransactionContext context, TableName tableName,
        DropPartitionInput dropPartitionInput, String catalogCommitEventId) {
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        TableHistoryObject latestTableHistory = TableHistoryHelper
            .getLatestTableHistoryOrElseThrow(context, tableIdent, VersionManagerHelper.getLatestVersion(tableIdent));
        List<PartitionObject> partitionList = PartitionHelper
            .getAllPartitionsFromTableHistory(context, tableIdent, latestTableHistory);
        HashSet<String> names = new HashSet<>(dropPartitionInput.getPartitionNames());

        List<PartitionObject> newPartitions = new ArrayList<>(partitionList.size());
        partitionList.forEach(partition -> {
            if (!names.contains(partition.getName())) {
                newPartitions.add(partition);
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
        PartitionHelper.insertTablePartitions(context, tableIdent, baseTableHistory, newPartitions);

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

    @Override
    public void dropPartition(TableName tableName, DropPartitionInput dropPartitionInput) {
        dropPartitioByName(tableName, dropPartitionInput);
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

    private void addPartitionsByName(TableName tableName, List<PartitionObject> partitions, boolean overwrite)
        throws MetaStoreException {
        MethodStageDurationCollector.Timer timer = MethodStageDurationCollector.startCollectTimer();
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();

        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        runner.run(context -> {
            return addPartitionsInternal(context, tableName, partitions, overwrite, catalogCommitEventId);
        }).getResultAndCheck(ret -> CatalogCommitHelper.catalogCommitExist(ret, catalogCommitEventId));

        timer.observeTotalDuration("TableStoreImpl.addPartitions", "totalLatency-success");

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
        List<PartitionObject> partitions, boolean overwrite, String catalogCommitEventId) {
        MethodStageDurationCollector.Timer timer = MethodStageDurationCollector.startCollectTimer();



        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        List<PartitionObject> newPartitions = buildNewPartitions(tableIdent, partitions, context);

        timer.observePrevDuration("TableStoreImpl.addPartitionsInternal", "buildNewPartitions");

        tableMetaStore.upsertTableReference(context, tableIdent);

        timer.observePrevDuration("TableStoreImpl.addPartitionsInternal", "saveTableReference");

        String currentVersionStamp = VersionManagerHelper.getLatestVersion(tableIdent);

        timer.observePrevDuration("TableStoreImpl.addPartitionsInternal", "getCurContextVersion");

        //tableHistory commit
        TableHistoryObject baseTableHistory = TableHistoryHelper
            .getLatestTableHistory(context, tableIdent, currentVersionStamp).get();

        timer.observePrevDuration("TableStoreImpl.addPartitionsInternal", "getLatestTableHistory");

        PartitionHelper.insertTablePartitions(context, tableIdent, baseTableHistory, newPartitions);

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
            .append("partition names: ");
        newPartitions.forEach(partition -> {
            builder.append(partition.getName()).append(", ");
        });

        catalogStore.insertCatalogCommit(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId(), catalogCommitEventId, commitTime, INSERT_TABLE, builder.toString());

        timer.observePrevDuration("TableStoreImpl.addPartitionsInternal", "insertCatalogCommit");

        return tableIdent;
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
        addPartitions(tableName, partitionInput);
        // todo ： need return partitions which added in this operate
        return new Partition[0];
    }

    @Override
    public void addPartitions(TableName tableName, AddPartitionInput partitionInput) {
        MethodStageDurationCollector.Timer timer = MethodStageDurationCollector.startCollectTimer();

        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        List<PartitionObject> partitionObjects = runner.run(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
            TableStorageObject tableStorage = TableStorageHelper.getTableStorage(context, tableIdent);
            TableBaseObject tableBaseObject = TableBaseHelper.getTableBase(context, tableIdent);
            TableSchemaObject tableSchemaObject = TableSchemaHelper.getTableSchema(context, tableIdent);

            List<PartitionObject> partitions = new ArrayList<>();
            if (isNotTransactionAndPartitionTable(tableBaseObject, tableSchemaObject)) {
                return partitions;
            }

            fillPartLocationIfNotExists(partitionInput, tableStorage, tableSchemaObject);

            List<String> partitionKeys = tableSchemaObject.getPartitionKeys().stream().map(ColumnObject::getName)
                    .collect(Collectors.toList());


            Arrays.stream(partitionInput.getPartitions()).forEach(partitionBase -> {
                PartitionObject builder = makePartitionInfo(tableIdent, partitionBase, partitionInput.getBasedVersion(),
                        partitionInput.getFileFormat(), partitionKeys);
                partitions.add(builder);
            });
            return partitions;
        }).getResult();


        addPartitionsByName(tableName, partitionObjects, partitionInput.isOverwrite());

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

    public void alterPartitionsByName(TableName tableName, AlterPartitionInput input) throws MetaStoreException {
        String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        runner.run(context -> {
            return alterPartitionsInternal(context, tableName, input, catalogCommitEventId);
        }).getResultAndCheck(ret -> CatalogCommitHelper.catalogCommitExist(ret, catalogCommitEventId));
    }

    private String getPartitionNameList(HashMap<String, PartitionAlterContext> nameMap) {
        StringBuffer nameBuffer = new StringBuffer();
        for (Map.Entry<String, PartitionAlterContext> entry : nameMap.entrySet()) {
            String key = entry.getKey();
            nameBuffer.append(key + ", ");
        }
        return nameBuffer.toString();
    }

    private TableIdent alterPartitionsInternal(TransactionContext context, TableName tableName,
        AlterPartitionInput input, String catalogCommitEventId) {
        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
        String latestVersion = VersionManagerHelper.getLatestVersion(tableIdent);
        TableHistoryObject latestTableHistory = TableHistoryHelper
            .getLatestTableHistoryOrElseThrow(context, tableIdent, latestVersion);
        List<PartitionObject> partitionList = PartitionHelper
            .getAllPartitionsFromTableHistory(context, tableIdent, latestTableHistory);
        HashMap<String, PartitionAlterContext> nameMap = new HashMap<>();
        TableSchemaObject tableSchemaObject = TableSchemaHelper.getTableSchema(context, tableIdent);
        //TableStorageObject tableStorage = TableStorageHelper.getTableStorage(context, tableIdent);
        List<String> partitionKeys = tableSchemaObject.getPartitionKeys().stream()
            .map(ColumnObject::getName).collect(Collectors.toList());
        for (PartitionAlterContext alterContext : input.getPartitionContexts()) {
            String oldPartName = PartitionUtil.makePartitionName(partitionKeys, alterContext.getOldValues());
            nameMap.put(oldPartName, alterContext);
        }

        List<PartitionObject> newPartitions = new ArrayList<>(partitionList.size());
        partitionList.forEach(partition -> {
            if (nameMap.containsKey(partition.getName())) {
                PartitionAlterContext alterContext = nameMap.get(partition.getName());
                String newPartName = PartitionUtil.makePartitionName(partitionKeys, alterContext.getNewValues());
                PartitionObject newPartition = new PartitionObject(partition);
                newPartition.setName(newPartName);
                newPartition.setInputFormat(alterContext.getInputFormat());
                newPartition.setOutputFormat(alterContext.getOutputFormat());
                newPartition.setProperties(alterContext.getParameters());

                newPartitions.add(newPartition);
                nameMap.remove(partition.getName());
            } else {
                newPartitions.add(partition);
            }
        });

        if (!nameMap.isEmpty()) {
            String nameBuffer = getPartitionNameList(nameMap);
            throw new MetaStoreException(ErrorCode.PARTITION_NAME_NOT_FOUND, nameBuffer);
        }

        // update TableReference
        /*TableReferenceObject tableReference = TableReferenceHelper.getTableReferenceOrElseThrow(context, tableIdent);
        tableReference.setDataUpdateTime(System.currentTimeMillis());*/
        tableMetaStore.upsertTableReference(context, tableIdent);

        String currentVersionStamp = VersionManagerHelper.getLatestVersion(tableIdent);
        //tableHistory commit
        TableHistoryObject baseTableHistory = TableHistoryHelper
            .getLatestTableHistory(context, tableIdent, currentVersionStamp).get();
        baseTableHistory.setPartitionSetType(TablePartitionSetType.INIT);
        PartitionHelper.insertTablePartitions(context, tableIdent, baseTableHistory, newPartitions);

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


    @Override
    public void alterPartitions(TableName tableName, AlterPartitionInput alterPartitionInput) {
        alterPartitionsByName(tableName, alterPartitionInput);
    }


    private List<PartitionObject> listPartitions(TableName tableName) throws MetaStoreException {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        return runner.run(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
            List<PartitionObject> partitionList = new ArrayList<>();
            Optional<TableHistoryObject> latestTableHistory = TableHistoryHelper
                    .getLatestTableHistory(context, tableIdent,
                            VersionManagerHelper.getLatestVersion(tableIdent));
            if (!latestTableHistory.isPresent()) {
                return partitionList;
            }
            partitionList = PartitionHelper.getAllPartitionsFromTableHistory(context, tableIdent,
                    latestTableHistory.get());
            return partitionList;
        }).getResult();
    }

    @Override
    public void alterPartition(TableName tableName, AlterPartitionInput alterPartitionInput) {
        alterPartitions(tableName, alterPartitionInput);
    }

    @Override
    public void renamePartition(TableName tableName, AlterPartitionInput alterPartitionInput) {
        alterPartitions(tableName, alterPartitionInput);
    }

    @Override
    public Partition[] getPartitionsByFilter(TableName tableName, PartitionFilterInput filterInput) {
        MethodStageDurationCollector.Timer timer = MethodStageDurationCollector.startCollectTimer();

        List<PartitionObject> partitions = listPartitions(tableName);

        timer.observePrevDuration("PartitionServiceImpl.getPartitionsByFilter", "list_partitions");

        //TableStorageObject tableStorage;
        TableSchemaObject tableSchemaObject;
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        tableSchemaObject = runner.run(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
            //tableStorage = tableStore.getTableStorage(context, tableIdent);
            return tableMetaStore.getTableSchema(context, tableIdent);
        }).getResult();

        timer.observePrevDuration("PartitionServiceImpl.getPartitionsByFilter", "getTableByName");

        List<Column> partitionColumns = convertToFilterColumn(tableSchemaObject.getPartitionKeys());
        ExpressionParser parser = new ExpressionParser();
        Expression expression = parser.parse(filterInput.getFilter().replaceAll("\"", "\'"));
        List<PartitionObject> newPartitions = new TablePartitionFilter()
            .prune(partitions, expression, partitionColumns);

        timer.observePrevDuration("PartitionServiceImpl.getPartitionsByFilter", "filterPartitionInMemory");
        timer.observeTotalDuration("PartitionServiceImpl.getPartitionsByFilter", "totalLatency");
        if (filterInput.getMaxParts() > 0){
            return newPartitions.stream().limit(filterInput.getMaxParts()).map(partition -> buildTablePartitionModel(partition))
                    .collect(Collectors.toList()).toArray(new Partition[0]);
        }
        return newPartitions.stream().map(partition -> buildTablePartitionModel(partition))
            .collect(Collectors.toList()).toArray(new Partition[0]);
    }

    @Override
    public Partition[] getPartitionsByNames(TableName tableName, PartitionFilterInput filterInput) {
        List<PartitionObject> partitions = listPartitions(tableName);
        HashMap<String, PartitionObject> partMaps = new HashMap<>();
        partitions.forEach(partition -> {
            partMaps.put(partition.getName(), partition);
        });

        List<PartitionObject> newPartitions = new ArrayList<>(partitions.size());
        for (String partName : filterInput.getPartNames()) {
            if (partMaps.containsKey(partName)) {
                newPartitions.add(partMaps.get(partName));
            }
        }

        return newPartitions.stream().map(partition -> buildTablePartitionModel(partition))
            .collect(Collectors.toList()).toArray(new Partition[0]);
    }

    private Partition buildTablePartitionModel(PartitionObject partition) {
        return buildTablePartitionModel(partition, false);
    }


    private Partition buildTablePartitionModel(PartitionObject partition, boolean fillFileDetails) {
        Partition tablePartition = new Partition();
        tablePartition.setStorageDescriptor(fillStorageDescriptorByPartitionObject(partition));
        tablePartition.setFileIndexUrl(partition.getPartitionIndexUrl());
        tablePartition.setPartitionValues(PartitionUtil.convertNameToVals(partition.getName()));
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

    private StorageDescriptor fillStorageDescriptorByPartitionObject(PartitionObject partition) {
        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(partition.getLocation());
        sd.setColumns(convertToColumn(partition.getColumn()));
        sd.setInputFormat(partition.getInputFormat());
        sd.setOutputFormat(partition.getOutputFormat());
        sd.setSerdeInfo(convertToSerDeInfo(partition));
        return sd;
    }

    private SerDeInfo convertToSerDeInfo(PartitionObject partition) {
        return new SerDeInfo("", partition.getSerde(), new HashMap<>());
    }

    @Override
    public Partition getPartitionWithAuth(TableName tableName, GetPartitionWithAuthInput partitionInput) {
        List<PartitionObject> partitions = listPartitions(tableName);

        TableSchemaObject tableSchemaObject;
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        tableSchemaObject = runner.run(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
            //tableStorage = tableStore.getTableStorage(context, tableIdent);
            return tableMetaStore.getTableSchema(context, tableIdent);
        }).getResult();

        if (tableSchemaObject.getPartitionKeys().size() != partitionInput.getPartVals().size()) {
            return null;
        }
        List<String> partitionKeys = tableSchemaObject.getPartitionKeys().stream().map(partitionColumn ->
            partitionColumn.getName()).collect(Collectors.toList());
        String partitionName = PartitionUtil.makePartitionName(partitionKeys, partitionInput.getPartVals());
        for (PartitionObject partition : partitions) {
            if (partition.getName().equals(partitionName)) {
                return buildTablePartitionModel(partition);
            }
        }
        return null;
    }

    @Override
    public String[] listPartitionNames(TableName tableName, PartitionFilterInput filterInput, boolean escape) {
        List<PartitionObject> partitions = listPartitions(tableName);
        if (filterInput.getMaxParts() > 0) {
            return partitions.stream().limit(filterInput.getMaxParts()).map(PartitionObject::getName).toArray(String[]::new);
        }
        return partitions.stream().map(PartitionObject::getName).toArray(String[]::new);
    }

    @Override
    public String[] listPartitionNamesByFilter(TableName tableName, PartitionFilterInput filterInput) {
        List<PartitionObject> partitions = listPartitions(tableName);
        //TableStorageObject tableStorage;
        TableSchemaObject tableSchemaObject;
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        tableSchemaObject = runner.run(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
            //tableStorage = tableStore.getTableStorage(context, tableIdent);
            return tableMetaStore.getTableSchema(context, tableIdent);
        }).getResult();

        List<Column> partitionColumns = convertToFilterColumn(tableSchemaObject.getPartitionKeys());
        ExpressionParser parser = new ExpressionParser();
        Expression expression = parser.parse(filterInput.getFilter().replaceAll("\"", "\'"));
        List<PartitionObject> newPartitions = new TablePartitionFilter()
            .prune(partitions, expression, partitionColumns);
        if (filterInput.getMaxParts() > 0) {
            return newPartitions.stream().limit(filterInput.getMaxParts()).map(PartitionObject::getName).toArray(String[]::new);
        }
        return newPartitions.stream().map(partition -> partition.getName())
            .collect(Collectors.toList()).toArray(new String[0]);
    }

    @Override
    public String[] listPartitionNamesPs(TableName tableName, PartitionFilterInput filterInput) {
        List<PartitionObject> partitions = listPartitions(tableName);
        List<String> names = new ArrayList<>(partitions.size());

        if (partitions.size() != 0 &&
            filterInput.getValues().length > PartitionUtil.convertNameToVals(partitions.get(0).getName()).size()) {
            throw new CatalogServerException(ErrorCode.PARTITION_VALUES_NOT_MATCH, filterInput.getValues().length);
        }

        partitions.forEach(partition -> {
            List<String> values = PartitionUtil.convertNameToVals(partition.getName());
            for (int i = 0; i < filterInput.getValues().length; i++) {
                if (StringUtils.isNotEmpty(filterInput.getValues()[i]) && !filterInput.getValues()[i].equals(values.get(i))) {
                    return;
                }
            }
            names.add(partition.getName());
        });
        return names.toArray(new String[0]);
    }

    @Override
    public Partition[] listPartitionsPsWithAuth(TableName tableName, GetPartitionsWithAuthInput filterInput) {
        List<PartitionObject> partitions = listPartitions(tableName);
        List<Partition> tablePartitions = new ArrayList<>(partitions.size());

        if (partitions.size() != 0 &&
            filterInput.getValues().size() > PartitionUtil.convertNameToVals(partitions.get(0).getName()).size()) {
            throw new CatalogServerException(ErrorCode.PARTITION_VALUES_NOT_MATCH, filterInput.getValues().size());
        }

        partitions.forEach(partition -> {
            List<String> values = PartitionUtil.convertNameToVals(partition.getName());
            for (int i = 0; i < filterInput.getValues().size(); i++) {
                if (!filterInput.getValues().get(i).equals(values.get(i)) && !filterInput.getValues().get(i)
                    .equals("")) {
                    return;
                }
            }
            tablePartitions.add(buildTablePartitionModel(partition));
        });
        return tablePartitions.toArray(new Partition[0]);
    }

    private CatalogToken buildDataNodeCatalogToken(String method, int indexSet, int startRecord, String version) {
        Map<String, String> map = new HashMap<>();
        map.put(method, String.valueOf(indexSet));
        map.put(method + "_startRecord", String.valueOf(startRecord));
        return new CatalogToken(TABLE_STORE_CHECKSUM, map, version);
    }

    private TraverseCursorResult<List<PartitionObject>> getPartitionsFromDataNodeWithTokenInternal(
        TransactionContext context,
        TableIdent tableIdent, List<String> dataSetIds, String curDataSetId, int maxResultNum,
        CatalogToken catalogToken) {
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();
        Integer indexDataNode = 0;
        Integer startRecord = 0;
        if (catalogToken.getContextMapValue(method) != null) {
            String indexDataNodeValue = catalogToken.getContextMapValue(method);
            indexDataNode = Integer.valueOf(indexDataNodeValue);
            String startRecordValue = catalogToken.getContextMapValue(method + "_startRecord");
            startRecord = Integer.valueOf(startRecordValue);
        }

        List<PartitionObject> partitionList = new ArrayList<>();

        int remainResult = maxResultNum;
        for (int i = indexDataNode; i < dataSetIds.size(); i++) {
            String setId = dataSetIds.get(i);
            DataPartitionSetObject dataPartitionSet = tableDataStore.getDataPartitionSet(context, tableIdent, setId);
            int remainListNum = dataPartitionSet.getDataPartitions().size() - startRecord;
            if (remainListNum == 0) {
                startRecord = 0;
                continue;
            }

            int batchNum = Math.min(remainResult, remainListNum);
            partitionList.addAll(dataPartitionSet.getDataPartitions().subList(startRecord, startRecord + batchNum));
            remainResult = remainResult - batchNum;
            if (remainResult == 0) {
                CatalogToken catalogTokenNew = buildDataNodeCatalogToken(method, i, startRecord + batchNum,
                    catalogToken.getReadVersion());
                return new TraverseCursorResult<>(partitionList, catalogTokenNew);
            }
            startRecord = 0;
        }

        DataPartitionSetObject dataPartitionSet = tableDataStore.getDataPartitionSet(context, tableIdent, curDataSetId);
        if (dataPartitionSet == null) {
            return new TraverseCursorResult<>(partitionList, null);
        }

        int remainListNum = dataPartitionSet.getDataPartitions().size() - startRecord;
        if (remainListNum == 0) {
            return new TraverseCursorResult<>(partitionList, null);
        }

        int batchNum = Math.min(remainResult, remainListNum);
        partitionList.addAll(dataPartitionSet.getDataPartitions().subList(startRecord, startRecord + batchNum));
        CatalogToken catalogTokenNew =
            (startRecord + batchNum < dataPartitionSet.getDataPartitions().size()) ? buildDataNodeCatalogToken(
                method, dataSetIds.size(), startRecord + batchNum, catalogToken.getReadVersion()) : null;
        return new TraverseCursorResult<>(partitionList, catalogTokenNew);
    }

    private TraverseCursorResult<List<PartitionObject>> getPartitionsFromDataNodeWithToken(TableIdent tableIdent,
        TableHistoryObject latestTableHistory, int maxResultNum, CatalogToken catalogToken) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
         return runner.run(context -> {
            return getPartitionsFromDataNodeWithTokenInternal(context, tableIdent,
                    latestTableHistory.getSetIds(), latestTableHistory.getCurSetId(), maxResultNum, catalogToken);
        }).getResult();
    }

    private CatalogToken buildIndexNodeCatalogToken(String method, int indexSet, String version) {
        Map<String, String> map = new HashMap<>();
        map.put(method, String.valueOf(indexSet));
        return new CatalogToken(TABLE_STORE_CHECKSUM, map, version);
    }


    private TraverseCursorResult<List<PartitionObject>> getPartitionsFromIndexNodeWithToken(TableIdent tableIdent,
        TableHistoryObject latestTableHistory, int maxResultNum, CatalogToken catalogToken) {
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();
        Integer indexIndexSetNodeToken = 0;
        if (catalogToken.getContextMapValue(method) != null) {
            String indexLevValue = catalogToken.getContextMapValue(method);
            indexIndexSetNodeToken = Integer.valueOf(indexLevValue);
        }

        Integer indexIndexSetNode = indexIndexSetNodeToken;

        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        return runner.run(context -> {
            int remainResult = maxResultNum;
            CatalogToken nextCatalogToken = catalogToken;
            List<PartitionObject> partitionList = new ArrayList<>();
            List<String> setIdsList = latestTableHistory.getSetIds();

            for (int i = indexIndexSetNode; i < setIdsList.size(); i++) {
                String setId = setIdsList.get(i);
                IndexPartitionSetObject indexPartitionSet = tableDataStore.getIndexPartitionSet(context, tableIdent, setId);
                TraverseCursorResult<List<PartitionObject>> batchPartitionList = getPartitionsFromDataNodeWithTokenInternal(
                        context, tableIdent, indexPartitionSet.getSetIds(),
                        indexPartitionSet.getCurSetId(), remainResult, nextCatalogToken);
                partitionList.addAll(batchPartitionList.getResult());
                remainResult = remainResult - batchPartitionList.getResult().size();
                if (remainResult == 0) {
                    String indexValue = String.valueOf(i);
                    CatalogToken catalogTokenNew = batchPartitionList.getContinuation().map(token -> {
                        token.putContextMap(method, indexValue);
                        return token;
                    }).orElse(buildIndexNodeCatalogToken(method, i + 1, catalogToken.getReadVersion()));
                    return new TraverseCursorResult<>(partitionList, catalogTokenNew);
                }
                nextCatalogToken = new CatalogToken(TABLE_STORE_CHECKSUM, catalogToken.getReadVersion());
            }

            IndexPartitionSetObject indexPartitionSet = tableDataStore
                    .getIndexPartitionSet(context, tableIdent, latestTableHistory.getCurSetId());
            if (indexPartitionSet == null) {
                return new TraverseCursorResult<>(partitionList, null);
            }

            TraverseCursorResult<List<PartitionObject>> batchPartitionList = getPartitionsFromDataNodeWithTokenInternal(
                    context,
                    tableIdent, indexPartitionSet.getSetIds(), indexPartitionSet.getCurSetId(),
                    remainResult, nextCatalogToken);
            partitionList.addAll(batchPartitionList.getResult());

            String indexValue = String.valueOf(setIdsList.size());
            CatalogToken catalogTokenNew = batchPartitionList.getContinuation().map(token -> {
                token.putContextMap(method, indexValue);
                return token;
            }).orElse(null);

            return new TraverseCursorResult<>(partitionList, catalogTokenNew);
        }).getResult();
    }


    private TraverseCursorResult<List<PartitionObject>> showTablePartitionsWithTokenInternal(TableIdent tableIdent,
        int maxResultNum, CatalogToken catalogToken) {
        TableHistoryObject latestTableHistory;
        String readVersion = catalogToken.getReadVersion();
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        latestTableHistory = runner.run(context -> {
            return TableHistoryHelper
                    .getLatestTableHistoryOrElseThrow(context, tableIdent, readVersion);
        }).getResult();

        if (latestTableHistory.getPartitionSetType() == TablePartitionSetType.DATA) {
            return getPartitionsFromDataNodeWithToken(tableIdent, latestTableHistory, maxResultNum, catalogToken);
        }

        return getPartitionsFromIndexNodeWithToken(tableIdent, latestTableHistory, maxResultNum, catalogToken);
    }

    private TraverseCursorResult<List<PartitionObject>> showTablePartitionsWithToken(TableIdent tableIdent,
        TableName tableName, int maxResults, String pageToken) {
        Optional<CatalogToken> catalogToken = CatalogToken.parseToken(pageToken, TABLE_STORE_CHECKSUM);
        if (!catalogToken.isPresent()) {
            String version = VersionManagerHelper.getLatestVersion(tableIdent); ;
            catalogToken = Optional.of(new CatalogToken(TABLE_STORE_CHECKSUM, version));
        }

        TraverseCursorResult<List<PartitionObject>> tablePartitions = showTablePartitionsWithTokenInternal(tableIdent,
            maxResults, catalogToken.get());
        return tablePartitions;
    }

    private TraverseCursorResult<List<PartitionObject>> showTablePartitions(TableName tableName, int maxResults,
        String pageToken, Expression filter) throws MetaStoreException {
        TableIdent tableIdent;
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        tableIdent = runner.run(context -> {
            return TableObjectHelper.getTableIdent(context, tableName);
        }).getResult();

        return showTablePartitionsWithToken(tableIdent, tableName, maxResults, pageToken);
    }

    @Override
    public Partition[] listPartitions(TableName tableName, PartitionFilterInput filterInput) {
        // todo: tmp impl in this method. Should be modified to implement completely later.
        TraverseCursorResult<List<PartitionObject>> partitions = showTablePartitions(tableName, Integer.MAX_VALUE,
            null, null);
        if (filterInput.getMaxParts() > 0) {
            return partitions.getResult().stream().limit(filterInput.getMaxParts())
                    .map(this::buildTablePartitionModel).toArray(Partition[]::new);
        }
        return partitions.getResult().stream()
            .map(this::buildTablePartitionModel).toArray(Partition[]::new);
    }

    @Override
    public Partition[] listPartitionsByExpr(TableName tableName, GetPartitionsByExprInput filterInput) {
        return null;
    }

    @Override
    public TraverseCursorResult<List<Partition>> showTablePartition(TableName tableName, int maxResults,
        String pageToken, FilterInput filterInput) {
        TraverseCursorResult<List<PartitionObject>> partitions = showTablePartitions(tableName, maxResults, pageToken,
            null);
        List<Partition> partitionList = partitions.getResult().stream()
            .map(partition -> buildTablePartitionModel(partition)).collect(Collectors.toList());
        return new TraverseCursorResult(partitionList, partitions.getContinuation().orElse(null));
    }

    private List<FileGroupObject> listFileGroups(TableName tableName, Expression filter) throws MetaStoreException {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(TABLE_STORE_MAX_RETRY_NUM);
        return runner.run(context -> {
            TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
            String latestVersion = VersionManagerHelper.getLatestVersion(tableIdent);
            TableHistoryObject latestTableHistory = TableHistoryHelper
                    .getLatestTableHistoryOrElseThrow(context, tableIdent, latestVersion);
            List<PartitionObject> partitions = tableDataStore
                    .getAllPartitionsFromTableHistory(context, tableIdent, latestTableHistory);
            TableSchemaHistoryObject latestTableSchemaHistory = TableSchemaHelper
                    .getLatestTableSchemaOrElseThrow(context, tableIdent,
                            latestVersion);
            List<ColumnObject> schema = latestTableSchemaHistory.getTableSchemaObject().getColumns();

            final Map<String, List<ColumnObject>> schemaMap = collectSchemaVersions(context, tableIdent,
                    partitions);
            return new MinMaxIndex().prune(schema, partitions, schemaMap, filter);
        }).getResult();
    }

    private static Expression convertFilter(FilterInput filterInput) {
        if (filterInput == null || StringUtils.isBlank(filterInput.getExpressionGson())) {
            return null;
        }
        return GsonUtil.fromJson(filterInput.getExpressionGson(), Expression.class);
    }

    private Map<String, List<ColumnObject>> collectSchemaVersions(TransactionContext context, TableIdent tableIdent,
        List<PartitionObject> partitions) {
        Map<String, List<ColumnObject>> schemaMap = new HashMap<>(partitions.size());
        try {
            partitions.forEach(partition -> {
                if (!partition.getSchemaVersion().isEmpty()) {
                    String schemaVersion = partition.getSchemaVersion();
                    if (!schemaMap.containsKey(schemaVersion)) {
                        String versionstamp = schemaVersion;
                        Optional<TableSchemaHistoryObject> schema = TableSchemaHelper
                            .getLatestTableSchema(context, tableIdent, versionstamp);
                        if (!schema.isPresent()) {
                            throw new RuntimeException(
                                "Failed to get schema of version: " + schemaVersion);
                        }
                        schemaMap.put(schemaVersion, schema.get().getTableSchemaObject().getColumns());
                    }
                }
            });
        } catch (MetaStoreException e) {
            log.error(e.getMessage(), e);
        }
        return schemaMap;
    }

    private Partition buildPartitionModel(FileGroupObject fileGroup) {
        Partition partition = new Partition();
        partition.setPartitionValues(PartitionUtil.convertNameToVals(fileGroup.getName()));
        List<DataFile> tableDataFileModelsList = new ArrayList<>(fileGroup.getDataFile().size());
        for (DataFileObject dataFileObject : fileGroup.getDataFile()) {
            DataFile dataFile = new DataFile();
            dataFile.setFileName(dataFileObject.getFileName());
            dataFile.setLength(dataFileObject.getLength());
            dataFile.setOffset(dataFileObject.getOffset());
            dataFile.setRowCount(dataFileObject.getRowCount());
            tableDataFileModelsList.add(dataFile);
        }

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(fileGroup.getBaseLocation());
        sd.setColumns(convertToColumn(fileGroup.getColumn()));
        partition.setStorageDescriptor(sd);
        partition.setDataFiles(tableDataFileModelsList);
        partition.setFileIndexUrl(fileGroup.getPartitionIndexUrl());
        return partition;
    }

    private List<Column> convertToColumn(List<ColumnObject> columnObjs) {
        if (columnObjs == null) {
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

    @Override
    public Partition[] listPartitions(TableName tableName, FilterInput filterInput) {
        try {
            List<FileGroupObject> fileGroupList = listFileGroups(tableName, convertFilter(filterInput));
            List<Partition> partitionList = new ArrayList<>(fileGroupList.size());
            for (FileGroupObject fileGroup : fileGroupList) {
                partitionList.add(buildPartitionModel(fileGroup));
            }
            return partitionList.toArray(new Partition[partitionList.size()]);
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
    }

    @Override
    public TableStats getTableStats(TableName tableName) {
        List<FileGroupObject> fileGroupList;
        try {
            fileGroupList = listFileGroups(tableName, null);
        } catch (MetaStoreException e) {
            throw new CatalogServerException(e.getMessage(), e.getErrorCode());
        }
        if (fileGroupList.size() == 0) {
            return new TableStats(-1, -1, -1);
        }
        long byteSize = 0;
        long numRows = 0;
        int numFiles = 0;
        boolean missedNumRows = false;
        for (FileGroupObject fileGroup : fileGroupList) {
            for (DataFileObject dataFile : fileGroup.getDataFile()) {
                byteSize += dataFile.getLength();
                if (dataFile.getRowCount() == -1) {
                    missedNumRows = true;
                } else {
                    numRows += dataFile.getRowCount();
                }
                numFiles += 1;
            }
        }
        long mbUnit = 1024L * 1024L;
        long sizeInMB = 0;
        if (byteSize != 0) {
            sizeInMB = Math.min(1, byteSize / mbUnit);
        }
        if (missedNumRows) {
            numRows = -1;
        }
        return new TableStats(sizeInMB, numRows, numFiles);
    }

    private List<Column> convertToFilterColumn(List<ColumnObject> columns) {
        List<Column> columnOutputs = new ArrayList<>(columns.size());
        for (ColumnObject column : columns) {
            String dataType;
            dataType = column.getDataType().toString();
            columnOutputs.add(new Column(column.getName(), dataType, column.getComment()));
        }
        return columnOutputs;
    }


    private IndexPartitionSetObject makeIndexPartitionSet(TableIdent tableIdent, String curSetId, List<String> setIds) {
        IndexPartitionSetObject indexPartitionSetObject = new IndexPartitionSetObject();
        indexPartitionSetObject.setCatalogId(tableIdent.getCatalogId());
        indexPartitionSetObject.setDatabaseId(tableIdent.getDatabaseId());
        indexPartitionSetObject.setTableId(tableIdent.getTableId());
        indexPartitionSetObject.setSetId(UuidUtil.generateId());
        indexPartitionSetObject.setCurSetId(curSetId);
        indexPartitionSetObject.setSetIds(setIds);
        return indexPartitionSetObject;
    }

    private DataPartitionSetObject makeDataPartitionSet(DataPartitionSetObject dataPartitionSetObject,
        PartitionObject newPartitionObject) {
        long size =
            dataPartitionSetObject.getPartitionsSize() + tableDataStore.getParitionSerializedSize(newPartitionObject);
        DataPartitionSetObject dataPartitionSetObjectNew = new DataPartitionSetObject(dataPartitionSetObject);
        dataPartitionSetObjectNew.setSetId(UuidUtil.generateId());
        dataPartitionSetObjectNew.getDataPartitions().add(newPartitionObject);
        dataPartitionSetObjectNew.setPartitionsSize(size);

        return dataPartitionSetObjectNew;
    }

    private DataPartitionSetObject makeDataPartitionSet(TableIdent tableIdent, PartitionObject newPartition) {
        DataPartitionSetObject dataPartitionSetObject = new DataPartitionSetObject();
        dataPartitionSetObject.setSetId(UuidUtil.generateId());
        dataPartitionSetObject.setCatalogId(tableIdent.getCatalogId());
        dataPartitionSetObject.setDatabaseId(tableIdent.getDatabaseId());
        dataPartitionSetObject.setTableId(tableIdent.getTableId());
        dataPartitionSetObject.setPartitionsSize(tableDataStore.getParitionSerializedSize(newPartition));
        dataPartitionSetObject.addDataPartition(newPartition);

        return dataPartitionSetObject;
    }

    @Override
    public Partition getPartitionByName(TableName tableName, String partitionName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getPartitionByName");
    }

    @Override
    public Partition getPartitionByValue(TableName tableName, List<String> partVals) {
        return null;
    }

    @Override
    public boolean updatePartitionColumnStatistics(TableName tableName,
        ColumnStatisticsInput stats) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "updatePartitionColumnStatistics");
    }

    @Override
    public PartitionStatisticData getPartitionColumnStatistics(TableName tableName, List<String> partNames,
        List<String> columns) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getPartitionColumnStatistics");
    }

    @Override
    public void deletePartitionColumnStatistics(TableName tableName, String partName, String columnName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "deletePartitionColumnStatistics");
    }

    @Override
    public void setPartitionColumnStatistics(TableName tableName, SetPartitionColumnStatisticsInput input) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "setPartitionColumnStatistics");
    }

    @Override
    public AggrStatisticData getAggrColStatsFor(TableName tableName, List<String> partNames, List<String> columns) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getAggrColStatsFor");
    }

    @Override
    public boolean doesPartitionExists(TableName tableName, PartitionValuesInput partitionValuesInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "doesPartitionExists");
    }

    @Override
    public Integer getTablePartitionCount(TableName tableName, PartitionFilterInput filterInput) {
        return null;
    }

    @Override
    public String getLatestPartitionName(TableName tableName) {
        return null;
    }
}
