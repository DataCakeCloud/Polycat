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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.DataPartitionSetObject;
import io.polycat.catalog.common.model.IndexPartitionSetObject;
import io.polycat.catalog.common.model.PartitionObject;
import io.polycat.catalog.common.model.TableHistoryObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TablePartitionSetType;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.api.TableDataStore;
import io.polycat.metrics.MethodStageDurationCollector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class PartitionHelper {

    private static TableDataStore tableDataStore;

    @Autowired
    public void setTableDataStore(TableDataStore tableStore) {
        this.tableDataStore = tableStore;
    }



    // a maximum of 2048 data partition set records can be stored in a index partition set.
    // the value is not final because the test case needs to modify it.
    private static int indexStoredMaxNumber = 2048;

    // 97280 = 95 x 1024, a data partition set can store up to 97280 bytes.
    // the value is not final because the test case needs to modify it.
    private static int dataStoredMaxSize = 97280;

    public static void insertTablePartitionForMerge(TransactionContext context, TableIdent tableIdent,
        TableHistoryObject tableHistoryObject) {
        List<PartitionObject> partitionList = getAllPartitionsFromTableHistory(context, tableIdent, tableHistoryObject);
        TableHistoryObject tableHistoryObjectNew = new TableHistoryObject(tableHistoryObject);
        tableHistoryObjectNew.clearSetIds();
        tableHistoryObjectNew.setEventId(UuidUtil.generateId());
        tableHistoryObjectNew.setVersion(null);
        tableHistoryObjectNew.setPartitionSetType(TablePartitionSetType.INIT);

        insertTablePartitions(context, tableIdent, tableHistoryObjectNew, partitionList);
    }

    public static void insertTablePartitions(TransactionContext context, TableIdent tableIdent,
        TableHistoryObject latestTableHistoryObject,
        List<PartitionObject> partitionList) {
        MethodStageDurationCollector.Timer timer = MethodStageDurationCollector.startCollectTimer();

        timer.observePrevDuration("TableStoreImpl.insertTableHistory", "getTableHistoryStore");

        TableHistoryObject tableHistoryObject = new TableHistoryObject(latestTableHistoryObject);
        if (!partitionList.isEmpty()) {
            insertPartitions(context, tableIdent, partitionList, tableHistoryObject);
        }
        tableHistoryObject.setEventId(UuidUtil.generateId());

        timer.observePrevDuration("TableStoreImpl.insertTableHistory", "insertPartitionByType");
        String version = VersionManagerHelper.getNextVersion(context, tableIdent.getProjectId(),
            tableIdent.getCatalogId());
        tableDataStore.insertTableHistory(context, tableIdent, version, tableHistoryObject);

        timer.observePrevDuration("TableStoreImpl.insertTableHistory", "insertRecord");
        timer.observeTotalDuration("TableStoreImpl.insertTableHistory", "totalLatency");
    }


    public static List<PartitionObject> getAllPartitionsFromTableHistory(TransactionContext context, TableIdent tableIdent,
        TableHistoryObject latestTableHistory) {
        List<PartitionObject> partitionList = new ArrayList<>();
        if (latestTableHistory.getPartitionSetType() == TablePartitionSetType.DATA) {
            List<PartitionObject> partitionList1 = tableDataStore
                .getAllPartitionsFromDataNode(context, tableIdent, latestTableHistory.getSetIds(),
                    latestTableHistory.getCurSetId());
            partitionList.addAll(partitionList1);
        } else if (latestTableHistory.getPartitionSetType() == TablePartitionSetType.INDEX) {
            for (String setId : latestTableHistory.getSetIds()) {
                IndexPartitionSetObject indexPartitionSet = tableDataStore.getIndexPartitionSet(context, tableIdent, setId);
                List<PartitionObject> partitionList1 = tableDataStore
                    .getAllPartitionsFromDataNode(context, tableIdent, indexPartitionSet.getSetIds(),
                        indexPartitionSet.getCurSetId());
                partitionList.addAll(partitionList1);
            }

            IndexPartitionSetObject indexPartitionSet = tableDataStore
                .getIndexPartitionSet(context, tableIdent, latestTableHistory.getCurSetId());
            List<PartitionObject> partitionList1 = tableDataStore
                .getAllPartitionsFromDataNode(context, tableIdent, indexPartitionSet.getSetIds(),
                    indexPartitionSet.getCurSetId());
            partitionList.addAll(partitionList1);
        }
        return partitionList;
    }

    public static List<PartitionObject> getPartitionsByPartitionNames(TransactionContext context, TableIdent tableIdent,
        TableHistoryObject latestTableHistory, List<String> partitionNames) {
        return tableDataStore.getPartitionsByPartitionNamesWithColumnInfo(
            context, tableIdent, new ArrayList<>(), latestTableHistory.getCurSetId(), partitionNames, -1);
    }

    /**
     * INIT : inserting Partitions to tablehistory for the first time. DATA : setids in the current tablehistory stores
     * dataPartition sets. INDEX : setids in the current tablehistory stores indexPartition sets. isFirst : When
     * Partition is inserted in batches, the dataPartitionset and indexPartitionsetdoes not need to be applied for
     * repeatedly.
     */
    private static void insertPartitions(TransactionContext context, TableIdent tableIdent,
        List<PartitionObject> newPartitionList,
        TableHistoryObject tableHistoryObject) {
        switch (tableHistoryObject.getPartitionSetType()) {
            case INIT:
                insertPartitionsForInit(context, tableIdent, newPartitionList, tableHistoryObject);
                break;
            case DATA:
                insertPartitionsForData(context, tableIdent, newPartitionList, tableHistoryObject);
                break;
            case INDEX:
                insertPartitionsForIndex(context, tableIdent, newPartitionList, tableHistoryObject);
                break;
            default:
                throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    private static void insertPartitionsForInit(TransactionContext context, TableIdent tableIdent,
        List<PartitionObject> newPartitionList, TableHistoryObject tableHistoryObject) {
        DataPartitionSetObject dataPartitionSet = makeDataPartitionSet(context, tableIdent, newPartitionList);
        tableHistoryObject.clearSetIds();
        tableHistoryObject.setPartitionSetType(TablePartitionSetType.DATA);
        tableHistoryObject.setCurSetId(dataPartitionSet.getSetId());

        if (!newPartitionList.isEmpty()) {
            insertPartitionsForDataToNewSet(context, tableIdent, newPartitionList, tableHistoryObject);
        }
    }

    private static void insertPartitionsForData(TransactionContext context, TableIdent tableIdent,
        List<PartitionObject> newPartitionList, TableHistoryObject tableHistoryObject) {
        DataPartitionSetObject oldDataPartitionSetObject = tableDataStore
            .getDataPartitionSet(context, tableIdent, tableHistoryObject.getCurSetId());
        //insert partitions with old dataPartitionSet
        if (oldDataPartitionSetObject.getPartitionsSize() + tableDataStore
            .getParitionSerializedSize(newPartitionList.get(0)) < dataStoredMaxSize) {
            DataPartitionSetObject newDataPartitionSet = makeDataPartitionSet(context, tableIdent,
                oldDataPartitionSetObject, newPartitionList);
            tableHistoryObject.setCurSetId(newDataPartitionSet.getSetId());
        }
        if (!newPartitionList.isEmpty()) {
            insertPartitionsForDataToNewSet(context, tableIdent, newPartitionList, tableHistoryObject);
        }
    }

    private static void insertPartitionsForIndex(TransactionContext context, TableIdent tableIdent,
        List<PartitionObject> newPartitionList, TableHistoryObject tableHistoryObject) {
        IndexPartitionSetObject indexPartitionSetObject = tableDataStore
            .getIndexPartitionSet(context, tableIdent, tableHistoryObject.getCurSetId());
        DataPartitionSetObject oldDataPartitionSetObject = tableDataStore
            .getDataPartitionSet(context, tableIdent, indexPartitionSetObject.getCurSetId());

        //update indexPartitionSetBuilder
        if (indexPartitionSetObject.getSetIds().size() < indexStoredMaxNumber
            || oldDataPartitionSetObject.getPartitionsSize() + tableDataStore
            .getParitionSerializedSize(newPartitionList.get(0)) < dataStoredMaxSize) {
            indexPartitionSetObject.setSetId(UuidUtil.generateId());
            tableHistoryObject.setCurSetId(indexPartitionSetObject.getSetId());
        }

        //insert partitions with old dataPartitionSet
        if (oldDataPartitionSetObject.getPartitionsSize() + tableDataStore
            .getParitionSerializedSize(newPartitionList.get(0)) < dataStoredMaxSize) {
            DataPartitionSetObject newDataPartitionSet = makeDataPartitionSet(context, tableIdent, oldDataPartitionSetObject,
                newPartitionList);
            indexPartitionSetObject.setCurSetId(newDataPartitionSet.getSetId());
        }

        if (!newPartitionList.isEmpty()) {
            insertPartitionsForIndexToNewSet(context, tableIdent, newPartitionList, tableHistoryObject,
                indexPartitionSetObject);
        }
    }

    private static DataPartitionSetObject makeDataPartitionSet(TransactionContext context, TableIdent tableIdent,
        DataPartitionSetObject oldDataPartitionSet, List<PartitionObject> newPartitionList) {
        DataPartitionSetObject dataPartitionSetBuilder = new DataPartitionSetObject(oldDataPartitionSet);
        dataPartitionSetBuilder.setSetId(UuidUtil.generateId());
        Iterator<PartitionObject> iterator = newPartitionList.iterator();
        long size = dataPartitionSetBuilder.getPartitionsSize();
        while (iterator.hasNext()) {
            PartitionObject partition = iterator.next();
            int paritionSerializedSize = tableDataStore.getParitionSerializedSize(partition);
            size +=  paritionSerializedSize;
            if (size >= dataStoredMaxSize) {
                size -= paritionSerializedSize;
                break;
            }
            dataPartitionSetBuilder.addDataPartition(partition);
            iterator.remove();
        }
        dataPartitionSetBuilder.setPartitionsSize(size);
        tableDataStore.insertDataPartitionSet(context, tableIdent, dataPartitionSetBuilder);
        return dataPartitionSetBuilder;
    }

    private static void insertPartitionsForIndexToNewSet(TransactionContext context, TableIdent tableIdent,
        List<PartitionObject> newPartitionList, TableHistoryObject tableHistoryObject,
        IndexPartitionSetObject indexPartitionSetObject) {
        while (!newPartitionList.isEmpty()) {
            if (indexPartitionSetObject.getSetIds().size() >= indexStoredMaxNumber) {
                tableDataStore.insertIndexPartitionSet(context, tableIdent, indexPartitionSetObject);
                tableHistoryObject.addSetId(indexPartitionSetObject.getSetId());
                //construct a new indexPartitionSetBuilder
                indexPartitionSetObject = new IndexPartitionSetObject();
                indexPartitionSetObject.setCatalogId(tableIdent.getCatalogId());
                indexPartitionSetObject.setDatabaseId(tableIdent.getDatabaseId());
                indexPartitionSetObject.setTableId(tableIdent.getTableId());
                indexPartitionSetObject.setSetId(UuidUtil.generateId());
                tableHistoryObject.setCurSetId(indexPartitionSetObject.getSetId());
            } else {
                indexPartitionSetObject.addSetId(indexPartitionSetObject.getCurSetId());
            }
            DataPartitionSetObject newDataPartitionSet = makeDataPartitionSet(context, tableIdent,
                newPartitionList);
            indexPartitionSetObject.setCurSetId(newDataPartitionSet.getSetId());
        }

        tableDataStore.insertIndexPartitionSet(context, tableIdent, indexPartitionSetObject);
    }

    private static void insertPartitionsForDataToNewSet(TransactionContext context, TableIdent tableIdent,
        List<PartitionObject> newPartitionList, TableHistoryObject tableHistoryObject) {
        while (!newPartitionList.isEmpty()) {
            if (tableHistoryObject.getSetIds().size() >= indexStoredMaxNumber) {
                //construct a indexPartitionSetBuilder
                IndexPartitionSetObject indexPartitionSetObject = new IndexPartitionSetObject();
                indexPartitionSetObject.setCatalogId(tableIdent.getCatalogId());
                indexPartitionSetObject.setDatabaseId(tableIdent.getDatabaseId());
                indexPartitionSetObject.setTableId(tableIdent.getTableId());
                indexPartitionSetObject.setSetId(UuidUtil.generateId());
                indexPartitionSetObject.setSetIds(tableHistoryObject.getSetIds());
                indexPartitionSetObject.setCurSetId(tableHistoryObject.getCurSetId());
                tableHistoryObject.clearSetIds();
                tableHistoryObject.setPartitionSetType(TablePartitionSetType.INDEX);
                tableHistoryObject.setCurSetId(indexPartitionSetObject.getSetId());
                insertPartitionsForIndexToNewSet(context, tableIdent, newPartitionList, tableHistoryObject, indexPartitionSetObject);
            } else {
                tableHistoryObject.addSetId(tableHistoryObject.getCurSetId());
                DataPartitionSetObject newDataPartitionSet = makeDataPartitionSet(context, tableIdent,
                    newPartitionList);
                tableHistoryObject.setCurSetId(newDataPartitionSet.getSetId());
            }
        }
    }

    private static DataPartitionSetObject makeDataPartitionSet(TransactionContext context, TableIdent tableIdent,
        List<PartitionObject> newPartitionList) {
        DataPartitionSetObject dataPartitionSetBuilder = new DataPartitionSetObject();
        dataPartitionSetBuilder.setSetId(UuidUtil.generateId());
        dataPartitionSetBuilder.setCatalogId(tableIdent.getCatalogId());
        dataPartitionSetBuilder.setDatabaseId(tableIdent.getDatabaseId());
        dataPartitionSetBuilder.setTableId(tableIdent.getTableId());

        Iterator<PartitionObject> iterator = newPartitionList.iterator();
        long size = 0;
        while (iterator.hasNext()) {
            PartitionObject partition = iterator.next();
            int paritionSerializedSize = tableDataStore.getParitionSerializedSize(partition);
            size += paritionSerializedSize;
            if (size >= dataStoredMaxSize) {
                size -= paritionSerializedSize;
                break;
            }
            dataPartitionSetBuilder.addDataPartition(partition);
            iterator.remove();
        }

        dataPartitionSetBuilder.setPartitionsSize(size);
        tableDataStore.insertDataPartitionSet(context, tableIdent, dataPartitionSetBuilder);
        return dataPartitionSetBuilder;
    }



}
