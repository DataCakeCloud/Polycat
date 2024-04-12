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

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.store.ResourceStoreHelper;
import io.polycat.catalog.store.gaussdb.common.MetaTableConsts;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.api.UsageProfileStore;
import io.polycat.catalog.store.common.StoreSqlConvertor;
import io.polycat.catalog.store.mapper.ResourceMapper;
import io.polycat.catalog.store.mapper.UsageProfileMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.math.BigInteger;
import java.util.*;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class UsageProfileStoreImpl implements UsageProfileStore {

    @Autowired
    private UsageProfileMapper usageProfileMapper;

    @Autowired
    ResourceMapper resourceMapper;

    private Long tableUsageProfileHotStatMillisecond = 30 * 24 *3600 * 1000L;

    @Override
    public void createUsageProfileSubspace(TransactionContext context, String projectId) {
        usageProfileMapper.createUsageProfileSubspace(projectId);
        usageProfileMapper.createUsageProfilePreStatSubspace(projectId);
        usageProfileMapper.createUsageProfileAccessStatSubspace(projectId);
    }

    @Override
    public void dropUsageProfileSubspace(TransactionContext context, String projectId) {
        usageProfileMapper.dropUsageProfileSubspace(projectId);
        usageProfileMapper.dropUsageProfilePreStatSubspace(projectId);
        usageProfileMapper.dropUsageProfileAccessStatSubspace(projectId);
    }

    @Override
    public void updateUsageProfile(TransactionContext context, String projectId, UsageProfileObject usageProfileObject) throws MetaStoreException {

    }

    @Override
    public Optional<UsageProfileObject> getUsageProfile(TransactionContext context, String projectId, String catalogId, String databaseId, String tableId, long startTime, String opType) throws MetaStoreException {
        return Optional.empty();
    }

    @Override
    public void deleteUsageProfile(TransactionContext context, String projectId, long startTime, long endTime) throws MetaStoreException {

    }

    @Override
    public ScanRecordCursorResult<List<UsageProfileObject>> listUsageProfile(TransactionContext context, String projectId, String tableId, String catalogName, String databaseName, List<String> opTypes, long startTime, long endTime, byte[] continuation) throws MetaStoreException {
        long offset = 0;
        if (continuation != null) {
            offset = CodecUtil.bytesToLong(continuation);
        }


        return null;
    }


    @Override
    public void recordUsageProfile(TransactionContext ctx, UsageProfileObject record) {
        // insert table usageProfile detail info
        usageProfileMapper.insertUsageProfileDetail(record.getProjectId(), record);
        // update table usageProfile pre-statistics
        updateUsageProfilePreStat(ctx, record);
        // update table usageProfile access-statistics
        updateUsageProfileAccessStat(ctx, record);
    }

    @Override
    public ScanRecordCursorResult<List<UsageProfileObject>> listUsageProfilePreStatByFilter(TransactionContext context,
                                                                                            String projectId, String catalogName, String databaseName, String tableName, List<String> opTypes, long startTime, long endTime, int maxBatchRowNum, byte[] continuation) {
        long offset = 0;
        if (continuation != null) {
            offset = CodecUtil.bytesToLong(continuation);
        }
        String filterSql = StoreSqlConvertor.get()
                .equals(UsageProfilePrePreStatObject.Fields.catalogName, catalogName).AND()
                .equals(UsageProfilePrePreStatObject.Fields.databaseName, databaseName).AND()
                .equals(UsageProfilePrePreStatObject.Fields.tableName, tableName).AND()
                .greaterThanOrEquals(UsageProfilePrePreStatObject.Fields.startTime, startTime).AND()
                .lessThanOrEquals(UsageProfilePrePreStatObject.Fields.endTime, endTime).AND()
                .in(UsageProfilePrePreStatObject.Fields.opType, opTypes).getFilterSql();
        List<UsageProfilePrePreStatObject> usageProfileObjectList = usageProfileMapper.listUsageProfilePreStatByFilter(projectId, filterSql, maxBatchRowNum, offset);
        List<UsageProfileObject> result = buildUsageProfileObject(projectId, usageProfileObjectList);
        if (usageProfileObjectList.size() < maxBatchRowNum) {
            return new ScanRecordCursorResult<>(result, null);
        } else {
            return new ScanRecordCursorResult<>(result, CodecUtil.longToBytes(offset + maxBatchRowNum));
        }
    }

    @Override
    public List<UsageProfileAccessStatObject> getUsageProfileAccessStatList(String projectId, UsageProfileAccessStatObject accessStatObject, Set<String> opTypesSet, boolean sortAsc) {
        String filterSql = buildUPAccessStatWhereCondition(projectId, accessStatObject, opTypesSet).getFilterSql();
        return usageProfileMapper.getUsageProfileAccessStatListByFilter(projectId, filterSql, sortAsc ? "ASC" : "DESC");
    }

    private StoreSqlConvertor buildUPAccessStatWhereCondition(String projectId, UsageProfileAccessStatObject accessStatObject, Set<String> opTypesSet) {
        return StoreSqlConvertor.get()
                .equals(UsageProfileAccessStatObject.Fields.catalogName, accessStatObject.getCatalogName()).AND()
                .equals(UsageProfileAccessStatObject.Fields.databaseName, accessStatObject.getDatabaseName()).AND()
                .equals(UsageProfileAccessStatObject.Fields.tableName, accessStatObject.getTableName()).AND()
                .equals(UsageProfileAccessStatObject.Fields.tableId, accessStatObject.getTableId()).AND()
                .greaterThanOrEquals(UsageProfileAccessStatObject.Fields.startTime, accessStatObject.getStartTime()).AND()
                .lessThanOrEquals(UsageProfileAccessStatObject.Fields.endTime, accessStatObject.getEndTime()).AND()
                .in(UsageProfileAccessStatObject.Fields.opType, opTypesSet);
    }

    @Override
    public List<String> getTableAccessUsers(TransactionContext context, String projectId, String catalogName, String databaseName, String tableName) {
        return usageProfileMapper.getTableAccessUsers(projectId, catalogName, databaseName, tableName);
    }

    @Override
    public List<TableUsageProfile> getUsageProfileDetailsByCondition(TransactionContext context, String projectId, UsageProfileObject upo, List<String> operations, long startTime, long endTime, int rowCount, long offset) {
        String filter = buildUPDetailWhereCondition(projectId, upo, startTime, endTime, operations);
        List<UsageProfileObject> usageProfileObject = usageProfileMapper.getUsageProfileDetailsByCondition(projectId, filter, rowCount, offset);
        List<TableUsageProfile> tableUsageProfiles = buildTableUsageProfile(projectId, usageProfileObject);
        return tableUsageProfiles;
    }

    private List<TableUsageProfile> buildTableUsageProfile(String projectId, List<UsageProfileObject> details) {
        List<TableUsageProfile> list = new ArrayList<>();
        if(CollectionUtils.isNotEmpty(details)) {
            TableUsageProfile tup;
            TableSource tableSource;
            for (UsageProfileObject upo: details) {
                tableSource = new TableSource(projectId, upo.getCatalogName(), upo.getDatabaseName(), upo.getTableName());
                tup = new TableUsageProfile(tableSource, upo.getOpType(), upo.getCreateTime(), BigInteger.valueOf(upo.getCount()));
                tup.setTag(upo.getTag());
                tup.setUserId(upo.getUserId());
                tup.setUserGroup(upo.getUserGroup());
                tup.setTaskId(upo.getTaskId());
                tup.setCreateDayTimestamp(upo.getCreateDayTime());
                tup.setCreateTimestamp(upo.getCreateTime());
                tup.setOpTypes(Collections.singletonList(upo.getOpType()));
                tup.setOriginOpTypes(Collections.singletonList(upo.getOriginOpType()));
                tup.setStatement(upo.getStatement());
                list.add(tup);
            }
        }
        return list;
    }

    private String buildUPDetailWhereCondition(String projectId, UsageProfileObject upo, long startTime, long endTime, Collection<String> opTypes) {
        StoreSqlConvertor storeSqlConvertor = StoreSqlConvertor.get();
        if (!Objects.isNull(upo.getOpType()) && !upo.getOpType().isEmpty()) {
            storeSqlConvertor.in(UsageProfileObject.Fields.opType, Collections.singleton(upo.getOriginOpType())).AND();
        }
        if (!Objects.isNull(upo.getOriginOpType()) && !upo.getOriginOpType().isEmpty()) {
            storeSqlConvertor.in(UsageProfileObject.Fields.originOpType, Collections.singleton(upo.getOriginOpType())).AND();
        }
        if (StringUtils.isNotEmpty(upo.getTag())) {
            storeSqlConvertor.equals(UsageProfileObject.Fields.tag, upo.getTag()).AND();
        }
        if (StringUtils.isNotEmpty(upo.getTaskId())) {
            storeSqlConvertor.equals(UsageProfileObject.Fields.taskId, upo.getTaskId()).AND();
        }
        if (StringUtils.isNotEmpty(upo.getUserId())) {
            storeSqlConvertor.equals(UsageProfileObject.Fields.userId, upo.getUserId()).AND();
        }
        if (startTime > 0) {
            storeSqlConvertor.greaterThanOrEquals(UsageProfileObject.Fields.createTime, startTime).AND();
        }
        if (endTime > 0) {
            storeSqlConvertor.lessThanOrEquals(UsageProfileObject.Fields.createTime, endTime).AND();
        }
        if (StringUtils.isNotEmpty(upo.getTableId())) {
            storeSqlConvertor.equals(UsageProfileObject.Fields.tableId, upo.getTableId()).AND();
        }
        if (StringUtils.isNotEmpty(upo.getTableName())) {
            storeSqlConvertor.equals(UsageProfileObject.Fields.tableName, upo.getTableName()).AND();
        }
        if (StringUtils.isNotEmpty(upo.getDatabaseName())) {
            storeSqlConvertor.equals(UsageProfileObject.Fields.databaseName, upo.getDatabaseName()).AND();
        }
        if (Objects.nonNull(opTypes) && opTypes.size() > 0) {
            storeSqlConvertor.nestStart().in(UsageProfileObject.Fields.opType, opTypes).OR()
                    .in(UsageProfileObject.Fields.originOpType, opTypes).nestEnd().AND();
        }

        return storeSqlConvertor
                .equals(UsageProfileObject.Fields.catalogName, upo.getCatalogName())
                .getFilterSql();
    }

    private List<UsageProfileObject> buildUsageProfileObject(String projectId, List<UsageProfilePrePreStatObject> statRecords) {
        List<UsageProfileObject> result = new ArrayList<>();
        UsageProfileObject usageProfileObject;
        for (UsageProfilePrePreStatObject statRecord: statRecords) {
            usageProfileObject = new UsageProfileObject(projectId, statRecord.getCatalogName(), statRecord.getDatabaseName(), statRecord.getTableName(), statRecord.getStartTime(), statRecord.getOpType(), statRecord.getSumCount());
            result.add(usageProfileObject);
        }
        return result;
    }


    private void updateUsageProfileAccessStat(TransactionContext ctx, UsageProfileObject record) {
        UsageProfileAccessStatObject accessStatObject = buildAccessStatObject(record);
        UsageProfileAccessStatObject existAccessStatObject = usageProfileMapper.getUsageProfileAccessStat(record.getProjectId(), accessStatObject);
        if (existAccessStatObject == null) {
            usageProfileMapper.insertUsageProfileAccessStat(record.getProjectId(), accessStatObject);
            return;
        }
        existAccessStatObject.setSumCount(existAccessStatObject.getSumCount() + accessStatObject.getSumCount());
        existAccessStatObject.setEndTime(accessStatObject.getEndTime());
        usageProfileMapper.updateUsageProfileAccessStat(record.getProjectId(), existAccessStatObject);
    }

    private UsageProfileAccessStatObject buildAccessStatObject(UsageProfileObject record) {
        return UsageProfileAccessStatObject.builder()
                .id(UuidUtil.generateUUID32())
                .catalogName(record.getCatalogName())
                .databaseName(record.getDatabaseName())
                .tableName(record.getTableName())
                .tableId(record.getTableId())
                .createDayTime(record.getCreateDayTime())
                .opType(record.getOpType())
                .originOpType(record.getOriginOpType())
                .sumCount(record.getCount())
                .userId(record.getUserId())
                .startTime(record.getCreateTime())
                .endTime(record.getCreateTime())
                .build();
    }

    private void updateUsageProfilePreStat(TransactionContext ctx, UsageProfileObject record) {
        UsageProfilePrePreStatObject buildPreStatObject = buildPreStatObject(record);
        List<UsageProfilePrePreStatObject> existPreStats = usageProfileMapper.getUsageProfilePreStat(record.getProjectId(), buildPreStatObject);
        if (existPreStats.isEmpty()) {
            usageProfileMapper.insertUsageProfilePreStat(record.getProjectId(), buildPreStatObject);
            return;
        }
        UsageProfilePrePreStatObject existPreStat = existPreStats.get(0);
        existPreStat.setSumCount(existPreStat.getSumCount() + buildPreStatObject.getSumCount());
        existPreStat.setEndTime(buildPreStatObject.getEndTime());
        usageProfileMapper.updateUsageProfilePreStat(record.getProjectId(), existPreStat);
    }

    private UsageProfilePrePreStatObject buildPreStatObject(UsageProfileObject record) {
        return UsageProfilePrePreStatObject.builder()
                .id(UuidUtil.generateUUID32())
                .catalogName(record.getCatalogName())
                .databaseName(record.getDatabaseName())
                .tableName(record.getTableName())
                .tableId(record.getTableId())
                .opType(record.getOpType())
                .originOpType(record.getOriginOpType())
                .createDayTime(record.getCreateDayTime())
                .sumCount(record.getCount())
                .startTime(record.getCreateTime())
                .endTime(record.getCreateTime())
                .build();
    }

    @Override
    public void createSubspace(TransactionContext context, String projectId) {

        if (!ResourceStoreHelper.doesExistsTable(context, projectId, MetaTableConsts.TABLE_USAGE_PROFILE_DETAIL)) {
            usageProfileMapper.createUsageProfileSubspace(projectId);
        }

        if (!ResourceStoreHelper.doesExistsTable(context, projectId, MetaTableConsts.TABLE_USAGE_PROFILE_PRESTAT)) {
            usageProfileMapper.createUsageProfilePreStatSubspace(projectId);
        }

        if (!ResourceStoreHelper.doesExistsTable(context, projectId, MetaTableConsts.TABLE_USAGE_PROFILE_ACCESS_STAT)) {
            usageProfileMapper.createUsageProfileAccessStatSubspace(projectId);
        }

        if (!ResourceStoreHelper.doesExistsView(context, projectId, MetaTableConsts.MV_TABLE_PROFILE_HOTSTAT)) {
            usageProfileMapper.createViewTableProfileHotStatSubspace(projectId, tableUsageProfileHotStatMillisecond);
        }
    }
}
