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
package io.polycat.catalog.store.api;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.plugin.request.input.PartitionFilterInput;

import java.util.List;
import java.util.Optional;

public interface TableDataStore {

    /**
     * table history subspace
     */

    void createTableDataPartitionSetSubspace(TransactionContext context, TableIdent tableIdent);

    void createTableIndexPartitionSetSubspace(TransactionContext context, TableIdent tableIdent);

    void dropTableDataPartitionSetSubspace(TransactionContext context, TableIdent tableIdent);

    void dropTableIndexPartitionSetSubspace(TransactionContext context, TableIdent tableIdent);

    void createTableHistorySubspace(TransactionContext context, String projectId) throws MetaStoreException;

    void createTableDataPartitionSet(TransactionContext context, String projectId);

    void dropTableHistorySubspace(TransactionContext context, TableIdent tableIdent) throws MetaStoreException;

    void createTableIndexSubspace(TransactionContext context, String projectId) throws MetaStoreException;

    void dropTableIndexSubspace(TransactionContext context, TableIdent tableIdent) throws MetaStoreException;

    void createTableIndexHistorySubspace(TransactionContext context, String projectId) throws MetaStoreException;

    void dropTableIndexHistorySubspace(TransactionContext context, TableIdent tableIdent) throws MetaStoreException;

    TableHistoryObject getLatestTableHistoryOrElseThrow(TransactionContext context, TableIdent tableIdent,
            String basedVersion) throws MetaStoreException;

    void insertTableHistory(TransactionContext context, TableIdent tableIdent, String version,
            TableHistoryObject tableHistory);

    byte[] deleteTableHistory(TransactionContext context, TableIdent tableIdent, String startVersion, String endVersion,
            byte[] continuation);

    Optional<TableHistoryObject> getLatestTableHistory(TransactionContext context, TableIdent tableIdent,
            String basedVersion);

    Optional<TableHistoryObject> getTableHistory(TransactionContext context, TableIdent tableIdent,
            String version);

    Optional<TableIndexesHistoryObject> getLatestTableIndexes(TransactionContext context,
            TableIdent tableIdent, String basedVersion);

    TableIndexesObject getTableIndexes(TransactionContext context, TableIdent tableIdent, String tableName);

    TableIndexesObject insertTableIndexes(TransactionContext context, TableIdent tableIdent,
            List<TableIndexInfoObject> tableIndexInfoList);

    void insertTableIndexesHistory(TransactionContext context, TableIdent tableIdent, String version,
            TableIndexesObject tableIndexes);

    /**
     * table partition
     */

    List<PartitionObject> getAllPartitionsFromTableHistory(TransactionContext context, TableIdent tableIdent,
            TableHistoryObject latestTableHistory);

    byte[] deleteDataPartition(TransactionContext context, TableIdent tableIdent, byte[] continuation);

    byte[] deleteIndexPartition(TransactionContext context, TableIdent tableIdent, byte[] continuation);

    void deletePartitionInfoByNames(TransactionContext context, TableIdent tableIdent, String setId,
            List<String> partitionNames);

    List<PartitionObject> getAllPartitionsFromDataNode(TransactionContext context, TableIdent tableIdent,
            List<String> setIds, String curSetId);

    List<PartitionObject> listTablePartitions(TransactionContext context, TableIdent tableIdent,
            List<String> setIds, String curSetId, Integer maxParts);

    List<String> listTablePartitionNames(TransactionContext context, TableIdent tableIdent, PartitionFilterInput filterInput, List<String> partitionKeys);

    List<PartitionObject> getPartitionsByPartitionNames(TransactionContext context, TableIdent tableIdent,
            List<String> setIds, String curSetId, List<String> partitionNames, int maxParts);

    Integer getTablePartitionCountByFilter(TransactionContext context, TableIdent tableIdent, String filter);
    Integer getTablePartitionCountByKeyValues(TransactionContext context, TableIdent tableIdent,
                                              List<String> partitionKeys, List<String> values);
    String getLatestPartitionName(TransactionContext context, TableIdent tableIdent);

    List<PartitionObject> getPartitionsByPartitionNamesWithColumnInfo(TransactionContext context, TableIdent tableIdent,
            List<String> setIds, String curSetId, List<String> partitionNames, int maxParts);

    List<PartitionObject> getPartitionsByFilter(TransactionContext context, TableIdent tableIdent,
            String curSetId, String filter, int maxParts);

    List<PartitionObject> getPartitionsByKeyValues(TransactionContext context, TableIdent tableIdent,
            String curSetId, List<String> partitionKeys, List<String> values, int maxParts);

    void insertDataPartitionSet(TransactionContext context, TableIdent tableIdent,
            DataPartitionSetObject dataPartitionSetObject);

    void createTablePartitionInfo(TransactionContext context, TableIdent tableIdent);

    void dropTablePartitionInfo(TransactionContext context, TableIdent tableIdent);

    void insertPartitionInfo(TransactionContext context, TableIdent tableIdent,
            DataPartitionSetObject dataPartitionSetObject);

    DataPartitionSetObject getDataPartitionSet(TransactionContext context, TableIdent tableIdent, String setId);

    void updateDataPartitionSet(TransactionContext context, TableIdent tableIdent,
            DataPartitionSetObject dataPartitionSetObject);

    void insertIndexPartitionSet(TransactionContext context, TableIdent tableIdent,
            IndexPartitionSetObject indexPartitionSetObject);

    IndexPartitionSetObject getIndexPartitionSet(TransactionContext context, TableIdent tableIdent, String setId);

    void updateIndexPartitionSet(TransactionContext context, TableIdent tableIdent,
            IndexPartitionSetObject indexPartitionSetObject);

    int getParitionSerializedSize(PartitionObject partitionObject);

    boolean doesPartitionExists(TransactionContext context, TableIdent tableIdent, String partitionName);

    void createColumnStatisticsSubspace(TransactionContext context, String projectId);

    List<ColumnStatisticsObject> getTableColumnStatistics(TransactionContext context, String projectId,
            TableName tableName, List<String> colNames);

    void updateTableColumnStatistics(TransactionContext context, String projectId,
            List<ColumnStatisticsObject> columnStatisticsObjects);

    void deleteTableColumnStatistics(TransactionContext context, TableName tableName, String colName);

    List<ColumnStatisticsObject> getPartitionColumnStatistics(TransactionContext context, TableName tableName,
            List<String> partNames, List<String> colNames);

    void updatePartitionColumnStatistics(TransactionContext context, TableName tableName,
            List<ColumnStatisticsObject> columnStatisticsObjects);

    void deletePartitionColumnStatistics(TransactionContext context, TableName tableName, String partName,
            String columnName);

    long getFoundPartNums(TransactionContext context, TableName tableName,
            List<String> partNames, List<String> colNames);

    List<ColumnStatisticsAggrObject> getAggrColStatsFor(TransactionContext context, TableName tableName,
            List<String> partNames, List<String> colNames);
}
