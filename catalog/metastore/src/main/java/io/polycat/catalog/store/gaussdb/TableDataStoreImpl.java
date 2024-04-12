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
package io.polycat.catalog.store.gaussdb;

import io.polycat.catalog.store.gaussdb.pojo.ColumnStatisticsAggrRecord;
import io.polycat.catalog.store.gaussdb.pojo.ColumnStatisticsRecord;
import io.polycat.catalog.store.gaussdb.pojo.PartitionColumnStatisticsTableMetaRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableDataHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableDataPartitionSetRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableIndexHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableIndexPartitionSetRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableIndexRecord;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.plugin.request.input.PartitionFilterInput;
import io.polycat.catalog.common.utils.SQLUtil;
import com.google.common.collect.Lists;
import io.polycat.catalog.store.gaussdb.pojo.ColumnStatisticsRecord.Fields;
import io.polycat.catalog.store.mapper.ColumnStatisticsMapper;

import java.util.*;

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.utils.PartitionUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.api.TableDataStore;
import io.polycat.catalog.store.common.StoreSqlConvertor;
import io.polycat.catalog.store.common.TableStoreConvertor;
import io.polycat.catalog.store.mapper.TableDataMapper;
import io.polycat.catalog.store.protos.common.Partition;
import io.polycat.catalog.store.protos.common.TableDataInfo;
import io.polycat.catalog.store.protos.common.TableDataPartitionSetInfo;
import io.polycat.catalog.store.protos.common.TableIndexInfoSet;
import io.polycat.catalog.store.protos.common.TableIndexPartitionSetInfo;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import static java.util.stream.Collectors.toList;

/**
 * table data history subspace
 */

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class TableDataStoreImpl implements TableDataStore {
    private static final Logger log = Logger.getLogger(TableDataStoreImpl.class);
    /**
     * Partition level statistics table name prefix
     */
    private static final String STAT_PARTITION_META_PREFIX = "pcs_";
    @Autowired
    TableDataMapper tableDataMapper;
    @Autowired
    ColumnStatisticsMapper statisticsMapper;

    @Override
    public void createTableDataPartitionSetSubspace(TransactionContext context, TableIdent tableIdent)
        throws MetaStoreException {
        tableDataMapper.createTableDataPartitionSetSubspace(tableIdent.getProjectId(), tableIdent.getTableId());
    }

    @Override
    public void dropTableDataPartitionSetSubspace(TransactionContext context, TableIdent tableIdent)
        throws MetaStoreException {
        tableDataMapper.dropTableDataPartitionSetSubspace(tableIdent.getProjectId(), tableIdent.getTableId());
    }

    @Override
    public void createTableIndexPartitionSetSubspace(TransactionContext context, TableIdent tableIdent)
        throws MetaStoreException {
        tableDataMapper.createTableDataPartitionSetSubspace(tableIdent.getProjectId(), tableIdent.getTableId());
    }

    @Override
    public void dropTableIndexPartitionSetSubspace(TransactionContext context, TableIdent tableIdent)
        throws MetaStoreException {
        tableDataMapper.dropTableIndexPartitionSetSubspace(tableIdent.getProjectId(), tableIdent.getTableId());
    }

    @Override
    public List<PartitionObject> getAllPartitionsFromTableHistory(TransactionContext context, TableIdent tableIdent,
        TableHistoryObject latestTableHistory) {
        try {
            List<PartitionObject> partitionList = new ArrayList<>();
            if (latestTableHistory.getPartitionSetType() == TablePartitionSetType.DATA) {
                List<PartitionObject> partitionList1 = getAllPartitionsFromDataNode(context, tableIdent,
                    latestTableHistory.getSetIds(),
                    latestTableHistory.getCurSetId());
                partitionList.addAll(partitionList1);
            } else if (latestTableHistory.getPartitionSetType() == TablePartitionSetType.INDEX) {
                for (String setId : latestTableHistory.getSetIds()) {
                    TableIndexPartitionSetRecord indexRecord = tableDataMapper
                        .getTableIndexPartitionSet(tableIdent.getProjectId(),
                            tableIdent.getTableId(), setId);
                    if (indexRecord == null) {
                        throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
                    }
                    TableIndexPartitionSetInfo tableIndexPartitionSetInfo = TableIndexPartitionSetInfo
                        .parseFrom(indexRecord.getIndexPartitionSetInfo());
                    List<PartitionObject> partitionList1 = getAllPartitionsFromDataNode(context, tableIdent,
                        tableIndexPartitionSetInfo.getSetIdsList(),
                        tableIndexPartitionSetInfo.getCurSetId());
                    partitionList.addAll(partitionList1);
                }

                TableIndexPartitionSetRecord indexRecord = tableDataMapper
                    .getTableIndexPartitionSet(tableIdent.getProjectId(),
                        tableIdent.getTableId(), latestTableHistory.getCurSetId());
                if (indexRecord == null) {
                    throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
                }
                TableIndexPartitionSetInfo tableIndexPartitionSetInfo = TableIndexPartitionSetInfo
                    .parseFrom(indexRecord.getIndexPartitionSetInfo());
                List<PartitionObject> partitionList1 = getAllPartitionsFromDataNode(context, tableIdent,
                    tableIndexPartitionSetInfo.getSetIdsList(),
                    tableIndexPartitionSetInfo.getCurSetId());
                partitionList.addAll(partitionList1);
            }
            return partitionList;
        }  catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public byte[] deleteDataPartition(TransactionContext context, TableIdent tableIdent, byte[] continuation) {
        tableDataMapper.deleteTableDataPartition(tableIdent.getProjectId(), tableIdent.getCatalogId(),
            tableIdent.getDatabaseId(), tableIdent.getTableId());
        return null;
    }

    @Override
    public byte[] deleteIndexPartition(TransactionContext context, TableIdent tableIdent, byte[] continuation) {
        tableDataMapper.deleteTableIndexPartition(tableIdent.getProjectId(), tableIdent.getCatalogId(),
            tableIdent.getDatabaseId(), tableIdent.getTableId());
        return null;
    }

    @Override
    public void deletePartitionInfoByNames(TransactionContext context, TableIdent tableIdent,
        String setId, List<String> partitionNames) {
        if(CollectionUtils.isEmpty(partitionNames)) {
            return;
        }
        tableDataMapper.deletePartitionInfoByName(tableIdent.getProjectId(), tableIdent.getTableId(), setId, partitionNames);
    }

    @Override
    public List<PartitionObject> getAllPartitionsFromDataNode(TransactionContext context, TableIdent tableIdent,
        List<String> setIds, String curSetId) {
        try {
            List<Partition> partitionList = new ArrayList<>();
            for (String setId : setIds) {
                TableDataPartitionSetRecord dataRecord = tableDataMapper
                    .getTableDataPartitionSet(tableIdent.getProjectId(),
                        tableIdent.getTableId(), setId);
                if (dataRecord == null) {
                    throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
                }

                TableDataPartitionSetInfo tableDataPartitionSetInfo = TableDataPartitionSetInfo
                    .parseFrom(dataRecord.getDataPartitionSetInfo());
                partitionList.addAll(tableDataPartitionSetInfo.getDataPartitionsList());
            }

            TableDataPartitionSetRecord dataRecord = tableDataMapper
                .getTableDataPartitionSet(tableIdent.getProjectId(),
                    tableIdent.getTableId(), curSetId);
            if (dataRecord == null) {
                throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
            }

            TableDataPartitionSetInfo tableDataPartitionSetInfo = TableDataPartitionSetInfo
                .parseFrom(dataRecord.getDataPartitionSetInfo());
            partitionList.addAll(tableDataPartitionSetInfo.getDataPartitionsList());

            return partitionList.stream().map(partition -> new PartitionObject(partition)).collect(toList());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public List<PartitionObject> listTablePartitions(TransactionContext context, TableIdent tableIdent,
        List<String> setIds, String curSetId, Integer maxParts) {
        try {
            if (maxParts <= 0) {
                maxParts = Integer.MAX_VALUE;
            }
            final List<PartitionInfo> partitionInfos = tableDataMapper
                .listTablePartitionInfos(tableIdent.getProjectId(), tableIdent.getTableId(), curSetId, maxParts);
            if (partitionInfos.size() > 0) {
                return partitionInfos.stream().map(TableStoreConvertor::convertToPartitionObject).collect(toList());
            } else {
                return Collections.emptyList();
            }
        } catch (Exception e) {
            log.warn("listTablePartitions error: ", e);
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }

    }

    @Override
    public List<String> listTablePartitionNames(TransactionContext context, TableIdent tableIdent, PartitionFilterInput filterInput, List<String> partitionKeys) {
        int maxParts = filterInput.getMaxParts();
        if (maxParts <= 0) {
            maxParts = Integer.MAX_VALUE;
        }
        StoreSqlConvertor sqlConvertor = StoreSqlConvertor.get();
        setFilterForPartitionKeyValue(sqlConvertor, partitionKeys, Arrays.asList(filterInput.getValues()), true);
        return tableDataMapper.listTablePartitionNames(tableIdent.getProjectId(), tableIdent.getTableId(), sqlConvertor.getFilterSql(), maxParts);
    }

    @Override
    public Integer getTablePartitionCountByFilter(TransactionContext context, TableIdent tableIdent, String filter) {
        return tableDataMapper.getPartitionCountByFilter(tableIdent.getProjectId(), tableIdent.getTableId(), filter);
    }

    @Override
    public Integer getTablePartitionCountByKeyValues(TransactionContext context, TableIdent tableIdent, List<String> partitionKeys, List<String> values) {
        if (values.size() > partitionKeys.size()) {
            throw new MetaStoreException(ErrorCode.PARTITION_VALUES_NOT_MATCH, partitionKeys.size(), values.size());
        }
        StoreSqlConvertor sqlConvertor = StoreSqlConvertor.get();
        setFilterForPartitionKeyValue(sqlConvertor, partitionKeys, values, true);

        log.info("sqlConvertor.getFilterSql(): {}", sqlConvertor.getFilterSql());
        return tableDataMapper
                .getPartitionCountByFilter(tableIdent.getProjectId(), tableIdent.getTableId(), sqlConvertor.getFilterSql(true));
    }

    @Override
    public String getLatestPartitionName(TransactionContext context, TableIdent tableIdent) {
        return tableDataMapper.getLatestPartitionName(tableIdent.getProjectId(), tableIdent.getTableId());
    }

    @Override
    public List<PartitionObject> getPartitionsByPartitionNames(TransactionContext context, TableIdent tableIdent,
        List<String> setIds, String curSetId, List<String> partitionNames, int maxParts) {
        try {
            if (maxParts <= 0) {
                maxParts = Integer.MAX_VALUE;
            }
            if (partitionNames == null || partitionNames.isEmpty()) {
                return Collections.emptyList();
            }
            final List<PartitionInfo> partitionInfos = tableDataMapper
                .getTablePartitionInfoByName(tableIdent.getProjectId(), tableIdent.getTableId(), curSetId, partitionNames, maxParts);
            if (partitionInfos.size() > 0) {
                return partitionInfos.stream().map(TableStoreConvertor::convertToPartitionObject).collect(toList());
            } else {
                return Collections.emptyList();
            }
        } catch (Exception e) {
            log.warn("getPartitionsByPartitionNames error: ", e);
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public List<PartitionObject> getPartitionsByPartitionNamesWithColumnInfo(TransactionContext context, TableIdent tableIdent,
        List<String> setIds, String curSetId, List<String> partitionNames, int maxParts) {
        try {
            if (maxParts <= 0) {
                maxParts = Integer.MAX_VALUE;
            }
            final List<PartitionInfo> partitionInfos = tableDataMapper
                .getTablePartitionInfoByNameWithColumnInfo(tableIdent.getProjectId(), tableIdent.getTableId(), curSetId, partitionNames, maxParts);
            if (partitionInfos.size() > 0) {
                return partitionInfos.stream().map(TableStoreConvertor::convertToPartitionObject).collect(toList());
            } else {
                return Collections.emptyList();
            }
        } catch (Exception e) {
            log.warn("getPartitionsByPartitionNamesWithColumnInfo error: ", e);
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public List<PartitionObject> getPartitionsByFilter(TransactionContext context, TableIdent tableIdent,
        String curSetId, String filter, int maxParts) {
        final List<PartitionInfo> tablePartitionInfos;
        if (maxParts <= 0) {
            maxParts = Integer.MAX_VALUE;
        }
        if (filter == null) {
            return Collections.emptyList();
        } else if (StringUtils.isBlank(filter)) {
            return listTablePartitions(context, tableIdent, null, curSetId, maxParts);
        } else {
            tablePartitionInfos = tableDataMapper
                    .getTablePartitionInfoByFilter(tableIdent.getProjectId(), tableIdent.getTableId(), curSetId, filter, maxParts);
        }
        if (tablePartitionInfos.size() > 0) {
            return tablePartitionInfos.stream().map(TableStoreConvertor::convertToPartitionObject).collect(toList());
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public List<PartitionObject> getPartitionsByKeyValues(TransactionContext context, TableIdent tableIdent,
        String curSetId, List<String> partitionKeys, List<String> values, int maxParts) {
        if (values.size() > partitionKeys.size()) {
            //throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR, "partitionKeys length must equal values length");
            throw new MetaStoreException(ErrorCode.PARTITION_VALUES_NOT_MATCH, partitionKeys.size(), values.size());
        }
        if (maxParts <= 0) {
            maxParts = Integer.MAX_VALUE;
        }
        StoreSqlConvertor sqlConvertor = StoreSqlConvertor.get();
        setFilterForPartitionKeyValue(sqlConvertor, partitionKeys, values, true);

        log.info("sqlConvertor.getFilterSql(): {}", sqlConvertor.getFilterSql());
        List<PartitionInfo> tablePartitionInfos = tableDataMapper
                .getTablePartitionInfoByFilter(tableIdent.getProjectId(), tableIdent.getTableId(), curSetId, sqlConvertor.getFilterSql(true),
                        maxParts);
        if (tablePartitionInfos.size() > 0) {
            return tablePartitionInfos.stream().map(TableStoreConvertor::convertToPartitionObject).collect(toList());
        } else {
            return Collections.emptyList();
        }

    }

    /**
     *
     * @param sqlConvertor
     * @param partitionKeys
     * @param values
     * @param wildcard Whether the value supports wildcarding
     */
    private void setFilterForPartitionKeyValue(StoreSqlConvertor sqlConvertor,
            List<String> partitionKeys, List<String> values, boolean wildcard) {
        if (CollectionUtils.isEmpty(partitionKeys) || CollectionUtils.isEmpty(values)) {
            return;
        }
        List<String> subListCols = partitionKeys.subList(0, values.size());
        StringBuilder valueFilter = new StringBuilder("");
        String partitionKey;
        String value;
        boolean likeFilter = false;
        for (int i = 0; i < subListCols.size(); i++) {
            partitionKey = subListCols.get(i);
            value = values.get(i);
            if (StringUtils.isEmpty(value)) {
                value = SQLUtil.OPERATOR_LIKE;
                likeFilter = true;
            } else {
                if (wildcard && value.contains(SQLUtil.HIVE_WILDCARD_IDENTIFIER)) {
                    value = SQLUtil.likeEscapeForHive(value);
                    likeFilter = true;
                } else {
                    value = SQLUtil.likeEscapeForHive(value);
                }
            }
            valueFilter.append(partitionKey).append("=").append(value).append("/");
        }
        // add ".*" to the regex to match anything else afterwards the partial spec.
        if (values.size() < partitionKeys.size()) {
            likeFilter = true;
            valueFilter.append(SQLUtil.OPERATOR_LIKE);
            valueFilter.append("/");
        }
        if (likeFilter) {
            sqlConvertor.AND().likeSpec("partition_name", valueFilter.deleteCharAt(valueFilter.length() - 1).toString());
        } else {
            sqlConvertor.AND().equals("partition_name", valueFilter.deleteCharAt(valueFilter.length() - 1).toString());
        }
    }

    @Override
    public void insertDataPartitionSet(TransactionContext context, TableIdent tableIdent,
        DataPartitionSetObject dataPartitionSetObject) {
        /*byte[] tableDataPartitionSetInfo = TableStoreConvertor.getDataPartitionSetInfo(dataPartitionSetObject)
            .toByteArray();*/
        tableDataMapper.insertTableDataPartitionSet(tableIdent.getProjectId(), tableIdent.getCatalogId(),
            tableIdent.getDatabaseId(),
            tableIdent.getTableId(), dataPartitionSetObject.getSetId());
    }

    @Override
    public void createTablePartitionInfo(TransactionContext context, TableIdent tableIdent) {
        tableDataMapper.createTablePartitionInfo(tableIdent.getProjectId(), tableIdent.getTableId());
        tableDataMapper.createTablePartitionColumnInfo(tableIdent.getProjectId(), tableIdent.getTableId());
    }

    @Override
    public void dropTablePartitionInfo(TransactionContext context, TableIdent tableIdent) {
        tableDataMapper.dropTableDataPartitionColumnInfo(tableIdent.getProjectId(), tableIdent.getTableId());
        tableDataMapper.dropTableDataPartitionInfo(tableIdent.getProjectId(), tableIdent.getTableId());
    }

    @Override
    public void insertPartitionInfo(TransactionContext context, TableIdent tableIdent,
        DataPartitionSetObject dataPartitionSetObject) {
        final List<PartitionObject> dataPartitions = dataPartitionSetObject.getDataPartitions();
        final List<PartitionInfo> partitionInfos = new ArrayList<>();
        final List<PartitionColumnInfo> partitionColumnInfoList = new ArrayList<>();
        dataPartitions.forEach(partitionObject -> {
            final PartitionInfo partitionInfo = TableStoreConvertor.getPartitionInfo(partitionObject);
            partitionInfo.setTableId(tableIdent.getTableId());
            partitionInfo.setSetId(dataPartitionSetObject.getSetId());
            partitionInfos.add(partitionInfo);
            List<ColumnObject> partitionKeys = partitionObject.getPartitionKeys();
            if (partitionKeys != null) {
                Map<String, String> partitionKV = PartitionUtil.convertNameToKvMap(partitionObject.getName());
                PartitionColumnInfo partitionColumnInfo;
                for (ColumnObject columnObject : partitionKeys) {
                    // TODO Ordinal Temporary filling
                    partitionColumnInfo = TableStoreConvertor
                            .getPartitionColumnInfo(columnObject);
                    partitionColumnInfo.setId(UuidUtil.generateUUID32());
                    partitionColumnInfo.setPartitionId(partitionInfo.getId());
                    partitionColumnInfo.setValue(partitionKV.get(columnObject.getName()));
                    partitionColumnInfo.setTableId(tableIdent.getTableId());
                    partitionColumnInfoList.add(partitionColumnInfo);
                }
            }
        });
        if (partitionInfos.size() > 0) {
            tableDataMapper.insertTablePartitionInfo(tableIdent.getProjectId(), tableIdent.getTableId(), partitionInfos);
        }
        if (partitionColumnInfoList.size() > 0) {
            tableDataMapper.insertTablePartitionColumnInfo(tableIdent.getProjectId(), tableIdent.getTableId(), partitionColumnInfoList);
        }
    }

    /*private static PartitionColumnInfo convertToColumnInfos(ColumnObject columnObject) {
        return new PartitionColumnInfo(columnObject.getName(), columnObject.getDataType(), columnObject.getComment(), columnObject.getOrdinal());
    }*/

    @Override
    public DataPartitionSetObject getDataPartitionSet(TransactionContext context, TableIdent tableIdent, String setId) {
        try {
            TableDataPartitionSetRecord tableDataPartitionSetRecord = tableDataMapper
                    .getTableDataPartitionSet(tableIdent.getProjectId(), tableIdent.getTableId(), setId);
            if (tableDataPartitionSetRecord == null) {
                return null;
            }

            /*TableDataPartitionSetInfo tableDataPartitionSetInfo = TableDataPartitionSetInfo
                .parseFrom(tableDataPartitionSetRecord.getDataPartitionSetInfo());*/

            DataPartitionSetObject dataPartitionSetObject = new DataPartitionSetObject(
                tableDataPartitionSetRecord.getSetId(),
                tableIdent.getCatalogId(), tableIdent.getDatabaseId(), tableIdent.getTableId(),
                null);
            return dataPartitionSetObject;
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void updateDataPartitionSet(TransactionContext context, TableIdent tableIdent,
        DataPartitionSetObject dataPartitionSetObject) {
        byte[] tableDataPartitionSetInfo = TableStoreConvertor.getDataPartitionSetInfo(dataPartitionSetObject)
            .toByteArray();
        tableDataMapper.updateTableDataPartitionSet(tableIdent.getProjectId(), tableIdent.getCatalogId(),
            tableIdent.getDatabaseId(),
            tableIdent.getTableId(), dataPartitionSetObject.getSetId(), tableDataPartitionSetInfo);
    }

    @Override
    public void insertIndexPartitionSet(TransactionContext context, TableIdent tableIdent,
        IndexPartitionSetObject indexPartitionSetObject) {
        byte[] tableIndexPartitionSetInfo = TableStoreConvertor.getIndexPartitionSetInfo(indexPartitionSetObject)
            .toByteArray();
        tableDataMapper.insertTableIndexPartitionSet(tableIdent.getProjectId(), tableIdent.getCatalogId(),
            tableIdent.getDatabaseId(),
            tableIdent.getTableId(), indexPartitionSetObject.getSetId(), tableIndexPartitionSetInfo);
    }

    @Override
    public IndexPartitionSetObject getIndexPartitionSet(TransactionContext context, TableIdent tableIdent,
        String setId) {
        try {
            TableIndexPartitionSetRecord tableIndexPartitionSetRecord = tableDataMapper
                .getTableIndexPartitionSet(tableIdent.getProjectId(), tableIdent.getTableId(), setId);
            if (tableIndexPartitionSetRecord == null) {
                return null;
            }

            TableIndexPartitionSetInfo tableIndexPartitionSetInfo = TableIndexPartitionSetInfo
                .parseFrom(tableIndexPartitionSetRecord.getIndexPartitionSetInfo());
            IndexPartitionSetObject indexPartitionSetObject = new IndexPartitionSetObject(
                tableIndexPartitionSetRecord.getSetId(),
                tableIdent.getCatalogId(), tableIdent.getDatabaseId(), tableIdent.getTableId(),
                tableIndexPartitionSetInfo);
            return indexPartitionSetObject;
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void updateIndexPartitionSet(TransactionContext context, TableIdent tableIdent,
        IndexPartitionSetObject indexPartitionSetObject) {
        byte[] tableIndexPartitionSetInfo = TableStoreConvertor.getIndexPartitionSetInfo(indexPartitionSetObject)
            .toByteArray();
        tableDataMapper.updateTableIndexPartitionSet(tableIdent.getProjectId(), tableIdent.getCatalogId(),
            tableIdent.getDatabaseId(),
            tableIdent.getTableId(), indexPartitionSetObject.getSetId(), tableIndexPartitionSetInfo);
    }

    @Override
    public int getParitionSerializedSize(PartitionObject partitionObject) {
        return 0;
    }

    @Override
    public boolean doesPartitionExists(TransactionContext context, TableIdent tableIdent, String partitionName) {
        return tableDataMapper.tablePartitionNameExist(tableIdent.getProjectId(), tableIdent.getTableId(), partitionName);
    }

    @Override
    public void createColumnStatisticsSubspace(TransactionContext context, String projectId) {
        statisticsMapper.createColumnStatisticsSubspace(projectId);
    }

    @Override
    public List<ColumnStatisticsObject> getTableColumnStatistics(TransactionContext context, String projectId,
            TableName tableName, List<String> colNames) {
        if (CollectionUtils.isEmpty(colNames)) {
            return Lists.newArrayList();
        }
        StoreSqlConvertor sqlConvertor = getTableColumnStatisticsFilterConvertor(tableName);
        String filterSql = sqlConvertor.AND()
                .in(Fields.columnName, colNames).getFilterSql();
        List<ColumnStatisticsRecord> tableColumnStatistics = statisticsMapper.getTableColumnStatistics(projectId,
                filterSql);
        return tableColumnStatistics.stream().map(this::convertColumnStatisticsData).collect(toList());
    }

    private StoreSqlConvertor getTableColumnStatisticsFilterConvertor(TableName tableName) {
        if (tableName == null) {
            return StoreSqlConvertor.get();
        }
        return StoreSqlConvertor.get().equals(Fields.catalogName, tableName.getCatalogName()).AND()
                .equals(Fields.databaseName, tableName.getDatabaseName()).AND()
                .equals(Fields.tableName, tableName.getTableName());
    }

    @Override
    public void updateTableColumnStatistics(TransactionContext context, String projectId,
            List<ColumnStatisticsObject> columnStatisticsObjects) {
        if (CollectionUtils.isNotEmpty(columnStatisticsObjects)) {
            statisticsMapper.updateTableColumnStatistics(projectId, columnStatisticsObjects.stream().map(this::convertColumnStatisticsRecord).collect(toList()));
        }
    }

    @Override
    public void deleteTableColumnStatistics(TransactionContext context, TableName tableName, String colName) {
        StoreSqlConvertor sqlConvertor = getTableColumnStatisticsFilterConvertor(tableName);
        if (colName != null) {
            sqlConvertor.AND().equals(Fields.columnName, colName);
        }
        statisticsMapper.deleteTableColumnStatistics(tableName.getProjectId(), sqlConvertor.getFilterSql());
    }

    @Override
    public List<ColumnStatisticsObject> getPartitionColumnStatistics(TransactionContext context,
            TableName tableName, List<String> partNames, List<String> colNames) {
        PartitionColumnStatisticsTableMetaRecord partitionStatisticsTableMeta = statisticsMapper.getPartitionStatisticsTableMeta(
                tableName.getProjectId(), getTableColumnStatisticsFilterConvertor(tableName).getFilterSql());
        if (partitionStatisticsTableMeta == null) {
            return Lists.newArrayList();
        }
        List<ColumnStatisticsRecord> list = statisticsMapper.getPartitionColumnStatistics(tableName.getProjectId(),
                partitionStatisticsTableMeta.getTableMetaName(),
                StoreSqlConvertor.get().in(Fields.columnName, colNames).AND().in(Fields.partitionName, partNames).getFilterSql());
        return list.stream().map(this::convertColumnStatisticsData).peek(x -> {
            x.setCatalogName(tableName.getCatalogName());
            x.setDatabaseName(tableName.getDatabaseName());
            x.setTableName(tableName.getTableName());
        }).collect(toList());
    }

    @Override
    public void updatePartitionColumnStatistics(TransactionContext context, TableName tableName,
            List<ColumnStatisticsObject> columnStatisticsObjects) {
        if (CollectionUtils.isNotEmpty(columnStatisticsObjects)) {
            StoreSqlConvertor sqlConvertor = getTableColumnStatisticsFilterConvertor(tableName);
            PartitionColumnStatisticsTableMetaRecord statTableMeta = statisticsMapper.getPartitionStatisticsTableMeta(
                    tableName.getProjectId(), sqlConvertor.getFilterSql());
            if (statTableMeta == null) {
                String pscTableName = STAT_PARTITION_META_PREFIX + UuidUtil.generateUUID32();
                statisticsMapper.createPartitionStatisticsTable(tableName.getProjectId(), pscTableName);
                statTableMeta = getPcsTableMetaRecord(pscTableName, columnStatisticsObjects.get(0).getTableId(), tableName);
                statisticsMapper.updatePartitionStatisticsTableMeta(tableName.getProjectId(), statTableMeta);
            }
            statisticsMapper.updatePartitionColumnStatistics(tableName.getProjectId(), statTableMeta.getTableMetaName(), columnStatisticsObjects.stream().map(this::convertColumnStatisticsRecord).collect(toList()));
        }

    }

    private PartitionColumnStatisticsTableMetaRecord getPcsTableMetaRecord(String pscTableName,
            String tableId, TableName tableName) {
        return new PartitionColumnStatisticsTableMetaRecord(
                UuidUtil.generateUUID32(),
                tableId,
                tableName.getCatalogName(),
                tableName.getDatabaseName(),
                tableName.getTableName(),
                pscTableName,
                System.currentTimeMillis()
        );
    }

    @Override
    public void deletePartitionColumnStatistics(TransactionContext context, TableName tableName,
            String partName, String columnName) {
        PartitionColumnStatisticsTableMetaRecord statTableMeta = statisticsMapper.getPartitionStatisticsTableMeta(
                tableName.getProjectId(), getTableColumnStatisticsFilterConvertor(tableName).getFilterSql());
        if (statTableMeta != null) {
            StoreSqlConvertor statSqlConvertor = StoreSqlConvertor.get().equals(Fields.partitionName, partName);
            if (columnName != null) {
                statSqlConvertor.AND().equals(Fields.columnName, columnName);
            }
            statisticsMapper.deletePartitionColumnStatistics(tableName.getProjectId(), statTableMeta.getTableMetaName(), statSqlConvertor.getFilterSql());
        }
    }

    @Override
    public long getFoundPartNums(TransactionContext context, TableName tableName,
            List<String> partNames, List<String> colNames) {
        PartitionColumnStatisticsTableMetaRecord partitionStatisticsTableMeta = statisticsMapper.getPartitionStatisticsTableMeta(
                tableName.getProjectId(), getTableColumnStatisticsFilterConvertor(tableName).getFilterSql());
        if (partitionStatisticsTableMeta == null) {
            return 0;
        }
        List<Long> allPartCounts = statisticsMapper.getFoundPartNums(tableName.getProjectId(), partitionStatisticsTableMeta.getTableMetaName(), StoreSqlConvertor.get().in(Fields.columnName, colNames).AND().in(Fields.partitionName, partNames).getFilterSql());
        long foundPartNums = 0;
        for (Long val : allPartCounts) {
            foundPartNums += val;
        }
        return foundPartNums;
    }

    @Override
    public List<ColumnStatisticsAggrObject> getAggrColStatsFor(TransactionContext context, TableName tableName,
            List<String> partNames, List<String> colNames) {
        PartitionColumnStatisticsTableMetaRecord partitionStatisticsTableMeta = statisticsMapper.getPartitionStatisticsTableMeta(
                tableName.getProjectId(), getTableColumnStatisticsFilterConvertor(tableName).getFilterSql());
        if (partitionStatisticsTableMeta == null) {
            return Lists.newArrayList();
        }
        List<ColumnStatisticsAggrRecord> aggrRecordList = statisticsMapper.getAggrColStatsFor(tableName.getProjectId(), partitionStatisticsTableMeta.getTableMetaName(), StoreSqlConvertor.get().in(Fields.columnName, colNames).AND().in(Fields.partitionName, partNames).getFilterSql());
        return aggrRecordList.stream().map(this::convertColumnStatisticsAggrObject).collect(toList());
    }

    private ColumnStatisticsAggrObject convertColumnStatisticsAggrObject(ColumnStatisticsAggrRecord aggrRecord) {
        if (aggrRecord == null) {
            return null;
        }
        return new ColumnStatisticsAggrObject(
                aggrRecord.getColumnName(),
                aggrRecord.getColumnType(),
                aggrRecord.getLongLowValue(),
                aggrRecord.getLongHighValue(),
                aggrRecord.getEstimationLongNumDistincts(),
                aggrRecord.getDoubleLowValue(),
                aggrRecord.getDoubleHighValue(),
                aggrRecord.getEstimationDoubleNumDistincts(),
                aggrRecord.getDecimalLowValue(),
                aggrRecord.getDecimalHighValue(),
                aggrRecord.getEstimationDecimalNumDistincts(),
                aggrRecord.getNumNulls(),
                aggrRecord.getLowerNumDistincts(),
                aggrRecord.getHigherNumDistincts(),
                aggrRecord.getAvgColLen(),
                aggrRecord.getMaxColLen(),
                aggrRecord.getNumTrues(),
                aggrRecord.getNumFalses()
        );
    }

    private ColumnStatisticsRecord convertColumnStatisticsRecord(ColumnStatisticsObject stat) {
        if (stat == null) {
            return null;
        }
        return new ColumnStatisticsRecord(
                stat.getTcsId(),
                stat.getTableId(),
                stat.getCatalogName(),
                stat.getDatabaseName(),
                stat.getTableName(),
                stat.getColumnName(),
                stat.getColumnType(),
                stat.getLongLowValue(),
                stat.getLongHighValue(),
                stat.getDoubleLowValue(),
                stat.getDoubleHighValue(),
                stat.getDecimalLowValue(),
                stat.getDecimalHighValue(),
                stat.getNumNulls(),
                stat.getNumDistincts(),
                stat.getBitVector(),
                stat.getAvgColLen(),
                stat.getMaxColLen(),
                stat.getNumTrues(),
                stat.getNumFalses(),
                stat.getLastAnalyzed(),
                stat.getPcsId(),
                stat.getPartitionName(),
                stat.getPartitionId()
        );
    }

    private ColumnStatisticsObject convertColumnStatisticsData(ColumnStatisticsRecord record) {
        if (record == null) {
            return null;
        }
        return new ColumnStatisticsObject(
                record.getTcsId(),
                record.getTableId(),
                record.getCatalogName(),
                record.getDatabaseName(),
                record.getTableName(),
                record.getColumnName(),
                record.getColumnType(),
                record.getLongLowValue(),
                record.getLongHighValue(),
                record.getDoubleLowValue(),
                record.getDoubleHighValue(),
                record.getDecimalLowValue(),
                record.getDecimalHighValue(),
                record.getNumNulls(),
                record.getNumDistincts(),
                record.getBitVector(),
                record.getAvgColLen(),
                record.getMaxColLen(),
                record.getNumTrues(),
                record.getNumFalses(),
                record.getLastAnalyzed(),
                record.getPcsId(),
                record.getPartitionName(),
                record.getPartitionId()
        );
    }

    @Override
    public void createTableHistorySubspace(TransactionContext context, String projectId)
        throws MetaStoreException {
        tableDataMapper.createTableDataHistorySubspace(projectId);
    }

    @Override
    public void createTableDataPartitionSet(TransactionContext context, String projectId) {
        tableDataMapper.createTableDataPartitionSetSubspace(projectId, "");
    }

    @Override
    public void dropTableHistorySubspace(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {
        tableDataMapper.dropTableDataHistorySubspace(tableIdent.getProjectId(), tableIdent.getCatalogId(),
            tableIdent.getTableId());
    }

    @Override
    public TableHistoryObject getLatestTableHistoryOrElseThrow(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        Optional<TableHistoryObject> tableHistoryObject = getLatestTableHistory(context, tableIdent,
            basedVersion);
        if (!tableHistoryObject.isPresent()) {
            throw new MetaStoreException(ErrorCode.TABLE_DATA_HISTORY_NOT_FOUND, tableIdent.getTableId());
        }

        return tableHistoryObject.get();
    }

    @Override
    public void insertTableHistory(TransactionContext context, TableIdent tableIdent, String version,
        TableHistoryObject tableHistory) {
        byte[] tableDataInfo = TableStoreConvertor.getTableDataInfo(tableHistory).toByteArray();
        tableDataMapper
            .insertTableDataHistory(tableIdent.getProjectId(), tableIdent.getCatalogId(), tableIdent.getTableId(),
                UuidUtil.generateUUID32(), version, tableDataInfo);
    }

    @Override
    public byte[] deleteTableHistory(TransactionContext context, TableIdent tableIdent, String startVersion,
        String endVersion, byte[] continuation) {
        tableDataMapper.deleteTableDataHistory(tableIdent.getProjectId(), tableIdent.getCatalogId(),
            tableIdent.getTableId(), startVersion, endVersion);
        return null;
    }

    @Override
    public Optional<TableHistoryObject> getLatestTableHistory(TransactionContext context, TableIdent tableIdent,
        String basedVersion) {
        try {
            TableDataHistoryRecord tableDataHistoryRecord = tableDataMapper
                .getLatestTableDataHistory(tableIdent.getProjectId(), tableIdent.getCatalogId(), tableIdent.getTableId(),
                    basedVersion);
            if (tableDataHistoryRecord == null) {
                return Optional.empty();
            }

            TableDataInfo tableDataInfo = TableDataInfo.parseFrom(tableDataHistoryRecord.getData());
            TableHistoryObject tableHistoryObject = new TableHistoryObject(tableDataInfo,
                tableDataHistoryRecord.getDataHisId(), tableDataHistoryRecord.getVersion());
            return Optional.of(tableHistoryObject);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Optional<TableHistoryObject> getTableHistory(TransactionContext context, TableIdent tableIdent,
        String version) {
        try {
            TableDataHistoryRecord tableDataHistoryRecord = tableDataMapper.getTableDataHistory(tableIdent.getProjectId(),
                tableIdent.getCatalogId(), tableIdent.getTableId(), version);
            TableDataInfo tableDataInfo = TableDataInfo.parseFrom(tableDataHistoryRecord.getData());
            TableHistoryObject tableHistoryObject = new TableHistoryObject(tableDataInfo,
                tableDataHistoryRecord.getDataHisId(), tableDataHistoryRecord.getVersion());
            return Optional.of(tableHistoryObject);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void createTableIndexHistorySubspace(TransactionContext context, String projectId)
            throws MetaStoreException {
        tableDataMapper.createTableIndexHistorySubspace(projectId);
    }

    @Override
    public void dropTableIndexHistorySubspace(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {
        tableDataMapper.dropTableIndexHistorySubspace(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getTableId());
    }

    @Override
    public void createTableIndexSubspace(TransactionContext context, String projectId)
            throws MetaStoreException {
        tableDataMapper.createTableIndexSubspace(projectId);
    }

    @Override
    public void dropTableIndexSubspace(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {
        tableDataMapper.dropTableIndexSubspace(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getTableId());
    }

    @Override
    public void insertTableIndexesHistory(TransactionContext context, TableIdent tableIdent, String version,
        TableIndexesObject tableIndexes) {
        byte[] indexInfo = TableStoreConvertor.getTableIndexInfoSet(tableIndexes.getTableIndexInfoObjectList())
            .toByteArray();
        tableDataMapper
            .insertTableIndexHistory(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getTableId(), UuidUtil.generateUUID32(), version, indexInfo);
    }

    @Override
    public Optional<TableIndexesHistoryObject> getLatestTableIndexes(TransactionContext context, TableIdent tableIdent,
        String basedVersion) {
        try {

            if (!tableDataMapper.tableIndexHistorySubspaceExist(tableIdent.getProjectId(),
                    tableIdent.getCatalogId(), tableIdent.getTableId())) {
                return Optional.empty();
            }

            TableIndexHistoryRecord tableIndexHistoryRecord = tableDataMapper
                .getLatestTableIndexHistory(tableIdent.getProjectId(),
                    tableIdent.getCatalogId(), tableIdent.getTableId(), basedVersion);
            if (tableIndexHistoryRecord == null) {
                return Optional.empty();
            }

            TableIndexInfoSet tableIndexInfoSet = TableIndexInfoSet.parseFrom(tableIndexHistoryRecord.getIndexInfo());
            TableIndexesHistoryObject tableIndexesHistoryObject = new TableIndexesHistoryObject(
                tableIndexHistoryRecord.getDataHisId(),
                tableIndexInfoSet.getIndexInfosList().stream()
                    .map(tableIndexInfo -> new TableIndexInfoObject(tableIndexInfo)).collect(toList()),
                tableIndexHistoryRecord.getVersion());
            return Optional.of(tableIndexesHistoryObject);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public TableIndexesObject insertTableIndexes(TransactionContext context, TableIdent tableIdent,
        List<TableIndexInfoObject> tableIndexInfoObjectList) {
        // create new tableIndexes
        byte[] tableIndexInfoSet = TableStoreConvertor.getTableIndexInfoSet(tableIndexInfoObjectList).toByteArray();
        tableDataMapper.insertTableIndex(tableIdent.getProjectId(), tableIdent.getCatalogId(), tableIdent.getTableId(),
            tableIndexInfoSet);
        return new TableIndexesObject(tableIndexInfoObjectList);
    }

    @Override
    public TableIndexesObject getTableIndexes(TransactionContext context, TableIdent tableIdent, String tableName) {
        try {
            if (!tableDataMapper.tableIndexSubspaceExist(tableIdent.getProjectId(),
                    tableIdent.getCatalogId(), tableIdent.getTableId())) {
                return null;
            }

            TableIndexRecord tableIndexRecord = tableDataMapper.getTableIndex(tableIdent.getProjectId(),
                    tableIdent.getCatalogId(), tableIdent.getTableId());
            if (tableIndexRecord == null) {
                return null;
            }

            TableIndexInfoSet tableIndexInfoSet = TableIndexInfoSet.parseFrom(tableIndexRecord.getIndexInfo());
            return new TableIndexesObject(
                tableIndexInfoSet.getIndexInfosList().stream()
                    .map(tableIndexInfo -> new TableIndexInfoObject(tableIndexInfo)).collect(toList()));
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }
}
