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
package io.polycat.catalog.service.api;

import java.util.List;

import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.TableStats;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.stats.AggrStatisticData;
import io.polycat.catalog.common.model.stats.PartitionStatisticData;
import io.polycat.catalog.common.plugin.request.input.AlterPartitionInput;
import io.polycat.catalog.common.plugin.request.input.ColumnStatisticsInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionByValuesInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionsByExprsInput;
import io.polycat.catalog.common.plugin.request.input.FilterInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionWithAuthInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionsByExprInput;
import io.polycat.catalog.common.plugin.request.input.GetPartitionsWithAuthInput;
import io.polycat.catalog.common.plugin.request.input.PartitionDescriptorInput;
import io.polycat.catalog.common.plugin.request.input.PartitionFilterInput;
import io.polycat.catalog.common.plugin.request.input.AddPartitionInput;
import io.polycat.catalog.common.plugin.request.input.PartitionValuesInput;
import io.polycat.catalog.common.plugin.request.input.SetPartitionColumnStatisticsInput;
import io.polycat.catalog.common.plugin.request.input.TruncatePartitionInput;

public interface PartitionService {
    void addPartition(TableName tableName, AddPartitionInput partitionInput);

    void addPartitions(TableName tableName, AddPartitionInput partitionInput);

    int addPartitionsReturnCnt(TableName tableName, AddPartitionInput partitionInput);

    Partition appendPartition(TableName tableName, PartitionDescriptorInput descriptor);

    void alterPartitions(TableName tableName, AlterPartitionInput alterPartitionInput);

    void alterPartition(TableName tableName, AlterPartitionInput alterPartitionInput);

    void renamePartition(TableName tableName, AlterPartitionInput alterPartitionInput);

    void dropPartition(TableName tableName, DropPartitionInput dropPartitionInput);

    void dropPartition(TableName tableName, DropPartitionByValuesInput dropPartitionByValuesInput);

    void truncatePartitions(TableName tableName, TruncatePartitionInput truncatePartitionInput);

    Partition[] dropPartitionsByExprs(TableName tableName, DropPartitionsByExprsInput dropPartitionsByExprsInput);

    Partition[] addPartitionsBackResult(TableName tableName, AddPartitionInput partitionInput);

    Partition[] getPartitionsByFilter(TableName tableName, PartitionFilterInput filterInput);

    Partition[] getPartitionsByNames(TableName tableName, PartitionFilterInput filterInput);

    Partition getPartitionWithAuth(TableName tableName, GetPartitionWithAuthInput partitionInput);

    String[] listPartitionNames(TableName tableName, PartitionFilterInput filterInput, boolean escape);

    String[] listPartitionNamesByFilter(TableName tableName, PartitionFilterInput filterInput);

    String[] listPartitionNamesPs(TableName tableName, PartitionFilterInput filterInput);

    Partition[] listPartitionsPsWithAuth(TableName tableName, GetPartitionsWithAuthInput filterInput);

    Partition[] listPartitions(TableName tableName, PartitionFilterInput filterInput);

    Partition[] listPartitionsByExpr(TableName tableName, GetPartitionsByExprInput filterInput);

    TraverseCursorResult<List<Partition>> showTablePartition(TableName tableName, int maxResults,
        String pageToken, FilterInput filterInput);

    Partition[] listPartitions(TableName tableName, FilterInput filterInput);

    TableStats getTableStats(TableName tableName);

    /**
     * Get the object based on partitionName, partName may need to be escaped.
     *
     * @param tableName
     * @param partitionName
     * @return
     */
    Partition getPartitionByName(TableName tableName, String partitionName);

    Partition getPartitionByValue(TableName tableName, List<String> partVals);

    boolean updatePartitionColumnStatistics(TableName tableName, ColumnStatisticsInput stats);

    PartitionStatisticData getPartitionColumnStatistics(TableName tableName, List<String> partNames, List<String> colNames);

    void deletePartitionColumnStatistics(TableName tableName, String partName, String columnName);

    void setPartitionColumnStatistics(TableName tableName, SetPartitionColumnStatisticsInput input);

    AggrStatisticData getAggrColStatsFor(TableName tableName, List<String> partNames, List<String> columns);

    boolean doesPartitionExists(TableName tableName, PartitionValuesInput partitionValuesInput);

    Integer getTablePartitionCount(TableName tableName, PartitionFilterInput filterInput);

    String getLatestPartitionName(TableName tableName);
}
