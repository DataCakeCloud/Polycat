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
package io.polycat.hiveService.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.PartitionAlterContext;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TableStats;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.model.stats.AggrStatisticData;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import io.polycat.catalog.common.model.stats.PartitionStatisticData;
import io.polycat.catalog.common.plugin.request.input.AddPartitionInput;
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
import io.polycat.catalog.common.plugin.request.input.PartitionValuesInput;
import io.polycat.catalog.common.plugin.request.input.SetPartitionColumnStatisticsInput;
import io.polycat.catalog.common.plugin.request.input.TruncatePartitionInput;
import io.polycat.catalog.service.api.PartitionService;
import io.polycat.hiveService.common.HiveServiceHelper;
import io.polycat.hiveService.data.accesser.HiveDataAccessor;
import io.polycat.hiveService.data.accesser.PolyCatDataAccessor;
import io.polycat.hiveService.hive.HiveMetaStoreClientUtil;

import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HivePartitionServiceImp implements PartitionService {

    @Override
    public void addPartition(TableName tableName, AddPartitionInput partitionInput) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient()
                .add_partition(HiveDataAccessor.toPartition(partitionInput.getPartitions()[0])));
    }

    @Override
    public void addPartitions(TableName tableName, AddPartitionInput partitionInput) {
        addPartitionsReturnCnt(tableName, partitionInput);
    }

    @Override
    public void dropPartition(TableName tableName, DropPartitionInput input) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient()
                .dropPartition(tableName.getCatalogName(), tableName.getDatabaseName(),
                    tableName.getTableName(),
                    input.getPartitionNames().get(0), input.isDeleteData()));
    }

    @Override
    public void dropPartition(TableName tableName, DropPartitionByValuesInput dropPartitionByValuesInput) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> {
                List<String> partVals = dropPartitionByValuesInput.getValues();
                PartitionDropOptions options = HiveDataAccessor.toPartitionDropOptions(dropPartitionByValuesInput);
                return HiveMetaStoreClientUtil.getHMSClient()
                    .dropPartition(tableName.getCatalogName(), tableName.getDatabaseName(),
                        tableName.getTableName(), partVals, options);
            });
    }

    @Override
    public void truncatePartitions(TableName tableName, TruncatePartitionInput truncatePartitionInput) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient()
                .truncateTable(tableName.getCatalogName(), tableName.getDatabaseName(),
                    tableName.getTableName(),
                    truncatePartitionInput.getPartName()));
    }

    @Override
    public Partition[] dropPartitionsByExprs(TableName tableName, DropPartitionsByExprsInput input) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                List<ObjectPair<Integer, byte[]>> exprs = HiveDataAccessor.toExprsList(input.getExprs());
                PartitionDropOptions options = HiveDataAccessor.toPartitionDropOptions(input);
                List<org.apache.hadoop.hive.metastore.api.Partition> partitions = HiveMetaStoreClientUtil.getHMSClient()
                    .dropPartitions(tableName.getCatalogName(),
                        tableName.getDatabaseName(), tableName.getTableName(), exprs, options);
                return partitions.stream().map(PolyCatDataAccessor::toPartition)
                    .toArray(Partition[]::new);
            });
    }

    @Override
    public Partition[] addPartitionsBackResult(TableName tableName, AddPartitionInput partitionInput) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                List<org.apache.hadoop.hive.metastore.api.Partition> hiveParts = Arrays.stream(
                        partitionInput.getPartitions())
                    .map(HiveDataAccessor::toPartition).collect(
                        Collectors.toList());
                List<org.apache.hadoop.hive.metastore.api.Partition> hiveAddedParts = HiveMetaStoreClientUtil.getHMSClient()
                    .add_partitions(hiveParts, partitionInput.isIfNotExist(),
                        partitionInput.isNeedResult());

                return hiveAddedParts == null ? null :
                    hiveAddedParts.stream().map(PolyCatDataAccessor::toPartition).toArray(Partition[]::new);
            });
    }

    @Override
    public int addPartitionsReturnCnt(TableName tableName, AddPartitionInput partitionInput) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                List<org.apache.hadoop.hive.metastore.api.Partition> hiveParts = Arrays.stream(
                        partitionInput.getPartitions())
                    .map(HiveDataAccessor::toPartition).collect(
                        Collectors.toList());
                return HiveMetaStoreClientUtil.getHMSClient().add_partitions(hiveParts);
            });
    }

    @Override
    public Partition appendPartition(TableName tableName, PartitionDescriptorInput descriptor) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                org.apache.hadoop.hive.metastore.api.Partition hivePart;
                if (descriptor.isSetValue()) {
                    hivePart = HiveMetaStoreClientUtil.getHMSClient()
                        .appendPartition(tableName.getCatalogName(), tableName.getDatabaseName(),
                            tableName.getTableName(), descriptor.getValues());
                    return PolyCatDataAccessor.toPartition(hivePart);
                } else {
                    hivePart = HiveMetaStoreClientUtil.getHMSClient()
                        .appendPartition(tableName.getCatalogName(), tableName.getDatabaseName(),
                            tableName.getTableName(), descriptor.getPartName());
                    return PolyCatDataAccessor.toPartition(hivePart);
                }
            });
    }

    @Override
    public void alterPartitions(TableName tableName, AlterPartitionInput alterPartitionInput) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> {
                List<org.apache.hadoop.hive.metastore.api.Partition> partitions = new ArrayList<>();
                org.apache.hadoop.hive.metastore.api.Partition hivePart;
                for (PartitionAlterContext partitionContext : alterPartitionInput.getPartitionContexts()) {
                    hivePart = HiveMetaStoreClientUtil.getHMSClient()
                        .getPartition(tableName.getCatalogName(), tableName.getDatabaseName(),
                            tableName.getTableName(), partitionContext.getOldValues());
                    partitions.add(HiveDataAccessor.toPartition(hivePart, partitionContext));
                }
                HiveMetaStoreClientUtil.getHMSClient()
                    .alter_partitions(tableName.getCatalogName(), tableName.getDatabaseName(),
                        tableName.getTableName(), partitions);
            });
    }

    @Override
    public void alterPartition(TableName tableName, AlterPartitionInput alterPartitionInput) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> {
                if (alterPartitionInput.getPartitionContexts().length != 1) {
                    throw new CatalogServerException(ErrorCode.ARGUMENT_ILLEGAL,
                        "AlterPartitionContexts should only have one context");
                }

                PartitionAlterContext context = alterPartitionInput.getPartitionContexts()[0];
                org.apache.hadoop.hive.metastore.api.Partition oldPart = HiveMetaStoreClientUtil.getHMSClient()
                    .getPartition(tableName.getCatalogName(), tableName.getDatabaseName(),
                        tableName.getTableName(), context.getOldValues());

                HiveMetaStoreClientUtil.getHMSClient()
                    .alter_partition(tableName.getCatalogName(), tableName.getDatabaseName(),
                        tableName.getTableName(), HiveDataAccessor.toPartition(oldPart, context));
            });
    }

    @Override
    public void renamePartition(TableName tableName, AlterPartitionInput alterPartitionInput) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> {
                if (alterPartitionInput.getPartitionContexts().length != 1) {
                    throw new CatalogServerException(ErrorCode.ARGUMENT_ILLEGAL,
                        "AlterPartitionContexts should only have one context");
                }

                PartitionAlterContext context = alterPartitionInput.getPartitionContexts()[0];
                org.apache.hadoop.hive.metastore.api.Partition oldPart = HiveMetaStoreClientUtil.getHMSClient()
                    .getPartition(tableName.getCatalogName(), tableName.getDatabaseName(),
                        tableName.getTableName(), context.getOldValues());

                HiveMetaStoreClientUtil.getHMSClient()
                    .renamePartition(tableName.getCatalogName(), tableName.getDatabaseName(),
                        tableName.getTableName(), context.getOldValues(),
                        HiveDataAccessor.toPartition(oldPart, context));
            });
    }

    @Override
    public Partition[] getPartitionsByFilter(TableName tableName, PartitionFilterInput filterInput) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient()
                .listPartitionsByFilter(tableName.getCatalogName(), tableName.getDatabaseName(),
                    tableName.getTableName(), filterInput.getFilter(), filterInput.getMaxParts()).stream()
                .map(PolyCatDataAccessor::toPartition).toArray(Partition[]::new));
    }

    @Override
    public Partition[] getPartitionsByNames(TableName tableName, PartitionFilterInput filterInput) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                List<org.apache.hadoop.hive.metastore.api.Partition> hiveParts = HiveMetaStoreClientUtil.getHMSClient()
                    .getPartitionsByNames(tableName.getCatalogName(),
                        tableName.getDatabaseName(), tableName.getTableName(),
                        Arrays.asList(filterInput.getPartNames()));
                return hiveParts.stream().map(PolyCatDataAccessor::toPartition)
                    .toArray(Partition[]::new);
            });
    }

    @Override
    public Partition getPartitionWithAuth(TableName tableName, GetPartitionWithAuthInput partitionInput) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                org.apache.hadoop.hive.metastore.api.Partition hivePart = HiveMetaStoreClientUtil.getHMSClient()
                    .getPartitionWithAuthInfo(tableName.getCatalogName(),
                        tableName.getDatabaseName(),
                        tableName.getTableName(), partitionInput.getPartVals(), partitionInput.getUserName(),
                        partitionInput.getGroupNames());

                return PolyCatDataAccessor.toPartition(hivePart);
            });
    }

    @Override
    public String[] listPartitionNames(TableName tableName, PartitionFilterInput filterInput, boolean escape) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().listPartitionNames(tableName.getCatalogName(),
                tableName.getDatabaseName(), tableName.getTableName(), filterInput.getMaxParts()).toArray(new String[0]));
    }

//    @Override
//    public String[] listPartitionNames(TableName tableName, short maxParts) {
//        return HiveServiceHelper.HiveExceptionHandler(
//            () -> HiveMetaStoreClientUtil.getHMSClient().listPartitionNames(tableName.getCatalogName(), tableName.getDatabaseName(),
//                tableName.getTableName(), maxParts).toArray(new String[0]));
//    }

    @Override
    public String[] listPartitionNamesByFilter(TableName tableName, PartitionFilterInput filterInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "listPartitionNamesByFilter");
    }

    @Override
    public String[] listPartitionNamesPs(TableName tableName, PartitionFilterInput filterInput) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                if (filterInput.getValues() == null || filterInput.getValues().length == 0) {
                    return HiveMetaStoreClientUtil.getHMSClient()
                        .listPartitionNames(tableName.getCatalogName(), tableName.getDatabaseName(),
                            tableName.getTableName(), filterInput.getMaxParts()).toArray(new String[0]);
                } else {
                    return HiveMetaStoreClientUtil.getHMSClient()
                        .listPartitionNames(tableName.getCatalogName(), tableName.getDatabaseName(),
                            tableName.getTableName(), Arrays.asList(filterInput.getValues()), filterInput.getMaxParts())
                        .toArray(new String[0]);
                }
            });
    }

    @Override
    public Partition[] listPartitionsPsWithAuth(TableName tableName, GetPartitionsWithAuthInput input) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                List<org.apache.hadoop.hive.metastore.api.Partition> hiveParts;
                if (input.getValues() == null || input.getValues().isEmpty()) {
                    hiveParts = HiveMetaStoreClientUtil.getHMSClient()
                        .listPartitionsWithAuthInfo(tableName.getCatalogName(),
                            tableName.getDatabaseName(), tableName.getTableName(), input.getMaxParts(),
                            input.getUserName(),
                            input.getGroupNames());
                } else {
                    hiveParts = HiveMetaStoreClientUtil.getHMSClient()
                        .listPartitionsWithAuthInfo(tableName.getCatalogName(),
                            tableName.getDatabaseName(), tableName.getTableName(), input.getValues(),
                            input.getMaxParts(),
                            input.getUserName(), input.getGroupNames());
                }
                return hiveParts.stream().map(PolyCatDataAccessor::toPartition)
                    .toArray(Partition[]::new);
            });
    }

    @Override
    public Partition[] listPartitions(TableName tableName, PartitionFilterInput filterInput) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                List<org.apache.hadoop.hive.metastore.api.Partition> hiveParts;
                if (filterInput.getValues() == null || filterInput.getValues().length == 0) {
                    hiveParts = HiveMetaStoreClientUtil.getHMSClient()
                        .listPartitions(tableName.getCatalogName(), tableName.getDatabaseName(),
                            tableName.getTableName(), filterInput.getMaxParts());
                } else {
                    hiveParts = HiveMetaStoreClientUtil.getHMSClient().listPartitions(tableName.getCatalogName(),
                        tableName.getDatabaseName(), tableName.getTableName(),
                        Arrays.asList(filterInput.getValues()), filterInput.getMaxParts());
                }
                return hiveParts.stream().map(PolyCatDataAccessor::toPartition)
                    .toArray(Partition[]::new);
            });
    }

    @Override
    public Partition[] listPartitionsByExpr(TableName tableName, GetPartitionsByExprInput filterInput) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                List<org.apache.hadoop.hive.metastore.api.Partition> retParts = new ArrayList<>();
                HiveMetaStoreClientUtil.getHMSClient().listPartitionsByExpr(tableName.getCatalogName(),
                    tableName.getDatabaseName(), tableName.getTableName(), filterInput.getExpr(),
                    filterInput.getDefaultPartitionName(), filterInput.getMaxParts(), retParts);
                return retParts.stream().map(PolyCatDataAccessor::toPartition)
                    .toArray(Partition[]::new);
            });
    }

    @Override
    public TraverseCursorResult<List<Partition>> showTablePartition(TableName tableName, int maxResults,
        String pageToken, FilterInput filterInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "showTablePartition");
    }

    @Override
    public Partition[] listPartitions(TableName tableName, FilterInput filterInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "listPartitonsReturnFiles");
    }

    @Override
    public TableStats getTableStats(TableName tableName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getTableStats");
    }

    @Override
    public Partition getPartitionByName(TableName tableName, String partitionName) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                org.apache.hadoop.hive.metastore.api.Partition hivePartition = HiveMetaStoreClientUtil.getHMSClient()
                    .getPartition(tableName.getCatalogName(), tableName.getDatabaseName(),
                        tableName.getTableName(), partitionName);
                return PolyCatDataAccessor.toPartition(hivePartition);
            });
    }

    @Override
    public Partition getPartitionByValue(TableName tableName, List<String> partVals) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                org.apache.hadoop.hive.metastore.api.Partition hivePartition = HiveMetaStoreClientUtil.getHMSClient()
                    .getPartition(tableName.getCatalogName(), tableName.getDatabaseName(),
                        tableName.getTableName(), partVals);
                return PolyCatDataAccessor.toPartition(hivePartition);
            });
    }

    @Override
    public boolean updatePartitionColumnStatistics(TableName tableName, ColumnStatisticsInput stats) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().updatePartitionColumnStatistics(
                HiveDataAccessor.toColumnStatistics(stats.getColumnStatistics())));
    }

    @Override
    public PartitionStatisticData getPartitionColumnStatistics(TableName tableName, List<String> partNames,
        List<String> columns) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                Map<String, List<org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj>> hiveStatsMap =
                    HiveMetaStoreClientUtil.getHMSClient()
                        .getPartitionColumnStatistics(tableName.getCatalogName(), tableName.getDatabaseName(),
                            tableName.getTableName(), partNames, columns);
                Map<String, List<ColumnStatisticsObj>> lsmStatsMap = new HashMap<>(hiveStatsMap.size());

                hiveStatsMap.forEach((partName, stats) -> lsmStatsMap.put(partName,
                    stats.stream().map(PolyCatDataAccessor::toColumnStatisticsObj).collect(Collectors.toList())));

                return new PartitionStatisticData(lsmStatsMap);
            });
    }

    @Override
    public void deletePartitionColumnStatistics(TableName tableName, String partName, String columnName) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient()
                .deletePartitionColumnStatistics(tableName.getCatalogName(), tableName.getDatabaseName(),
                    tableName.getTableName(), partName, columnName));
    }

    @Override
    public void setPartitionColumnStatistics(TableName tableName,
        SetPartitionColumnStatisticsInput input) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient()
                .setPartitionColumnStatistics(HiveDataAccessor.toSetPartitionsStatsRequest(input)));
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

    @Override
    public AggrStatisticData getAggrColStatsFor(TableName tableName, List<String> partNames, List<String> columns) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> PolyCatDataAccessor.toAggrStatisticResult(
                HiveMetaStoreClientUtil.getHMSClient()
                    .getAggrColStatsFor(tableName.getCatalogName(), tableName.getDatabaseName(),
                        tableName.getTableName(), columns, partNames)));
    }
}
