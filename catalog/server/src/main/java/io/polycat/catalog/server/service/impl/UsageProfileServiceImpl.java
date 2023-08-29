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

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.plugin.request.input.TableUsageProfileInput;
import io.polycat.catalog.common.plugin.request.input.TopTableUsageProfileInput;
import io.polycat.catalog.common.utils.CatalogStringUtils;
import io.polycat.catalog.common.utils.CatalogToken;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.server.config.AsyncStoreTPConfig;
import io.polycat.catalog.server.util.TransactionRunnerUtil;
import io.polycat.catalog.service.api.UsageProfileService;
import io.polycat.catalog.store.api.Transaction;
import io.polycat.catalog.store.api.UsageProfileStore;
import io.polycat.catalog.util.CheckUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ExecutorService;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class UsageProfileServiceImpl implements UsageProfileService {

    private final String usageProfileStoreCheckSum = "usageProfileStore";
    private final int minute = 60000;
    private final static long DAY_OF_MILLIS = 1000 * 24 * 3600;
    private final static int maxBatchRowNum = 1024;

    @Autowired
    private UsageProfileStore usageProfileStore;
    @Autowired
    private Transaction storeTransaction;
    @Autowired
    @Qualifier("asyncStoreTP")
    private ExecutorService executorService;

    /**
     * record UsageProfile
     *
     * @param tableUsageProfileInput tableUsageProfileInput
     */
    @Override
    public void recordTableUsageProfile(TableUsageProfileInput tableUsageProfileInput) {
        TransactionRunnerUtil.transactionRunThrow(context -> {
            for (TableUsageProfile value : tableUsageProfileInput.getTableUsageProfiles()) {
                AsyncStoreTPConfig.MsgRunnable msgRunnable = new AsyncStoreTPConfig.MsgRunnable() {
                    @Override
                    public void run() {
                        normalizeIdentifier(value.getTable());
                        setMsgObj(value);
                        recordUsageProfileInternal(context, value);
                    }
                };
                msgRunnable.setMsgObj(value);
                executorService.execute(msgRunnable);
            }
            return null;
        }).getResult();
    }

    private void normalizeIdentifier(TableSource tableSource) {
        if (tableSource == null) {
            return;
        }
        tableSource.setProjectId(CatalogStringUtils.normalizeIdentifier(tableSource.getProjectId()));
        tableSource.setCatalogName(CatalogStringUtils.normalizeIdentifier(tableSource.getCatalogName()));
        tableSource.setDatabaseName(CatalogStringUtils.normalizeIdentifier(tableSource.getDatabaseName()));
        tableSource.setTableName(CatalogStringUtils.normalizeIdentifier(tableSource.getTableName()));
    }

    private void recordUsageProfileInternal(TransactionContext context, TableUsageProfile value) {
        try {
            if (CollectionUtils.isEmpty(value.getOpTypes())) {
                log.warn("UsageProfile record write opTypes not null");
                return;
            }
            TableName tableName = buildTableName(value.getTable());
            TableObject table = TableObjectHelper.getTableObject(tableName);
            if (table == null) {
                log.warn("Record table usage profile error: " + ErrorCode.TABLE_NOT_FOUND.getMessageFormat(), tableName.getTableName());
                return;
            }
            UsageProfileObject usageProfileObject = buildUsageProfileObject(value, table);
            usageProfileStore.recordUsageProfile(context, usageProfileObject);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Record table usage profile error: {}" + e.getMessage());
        }
    }

    private TableName buildTableName(TableSource table) {
        return new TableName(table.getProjectId(), table.getCatalogName(),
                table.getDatabaseName(), table.getTableName());
    }

    private UsageProfileObject buildUsageProfileObject(TableUsageProfile value, TableObject table) {
        long createTimestamp = value.getCreateTimestamp();
        UsageProfileObject usageProfileObject = new UsageProfileObject();
        usageProfileObject.setId(UuidUtil.generateUUID32());
        usageProfileObject.setProjectId(table.getProjectId());
        usageProfileObject.setCatalogName(table.getCatalogName());
        usageProfileObject.setDatabaseName(table.getDatabaseName());
        usageProfileObject.setTableName(table.getName());
        usageProfileObject.setTableId(table.getTableId());
        // The service is centralized. The incoming createDayTime has different time zone differences in different regions.
        // It is untrustworthy and needs to be calculated by yourself.
        if (createTimestamp > DAY_OF_MILLIS) {
            usageProfileObject.setCreateDayTime(createTimestamp - createTimestamp % DAY_OF_MILLIS);
        }
        usageProfileObject.setCreateTime(createTimestamp);
        usageProfileObject.setOpType(value.getOpTypes().get(0));
        usageProfileObject.setCount(value.getSumCount().longValue());
        usageProfileObject.setUserId(value.getUserId());
        usageProfileObject.setTaskId(value.getTaskId());
        usageProfileObject.setTag(value.getTag());
        return usageProfileObject;
    }

    private TraverseCursorResult<List<UsageProfileObject>> getTableUsageProfileRecords(
        TransactionContext ctx, String projectId, String catalogName, String databaseName, String tableName, List<String> opTypes, long startTime, long endTime, CatalogToken catalogToken) {
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();
        byte[] continuation = null;
        if (catalogToken.getContextMapValue(method) != null) {
            continuation = CodecUtil.hex2Bytes(catalogToken.getContextMapValue(method));
        }

        ScanRecordCursorResult<List<UsageProfileObject>> scanRecordCursorResult =
            usageProfileStore.listUsageProfilePreStatByFilter(ctx, projectId, catalogName, databaseName, tableName, opTypes, startTime, endTime, maxBatchRowNum, continuation);

        List<UsageProfileObject> usageProfileObjectList = scanRecordCursorResult.getResult();

        continuation = scanRecordCursorResult.getContinuation().orElse(null);
        if (continuation == null) {
            return new TraverseCursorResult<>(usageProfileObjectList, null);
        }

        CatalogToken catalogTokenNew = CatalogToken.buildCatalogToken(usageProfileStoreCheckSum, method,
            CodecUtil.bytes2Hex(continuation));
        return new TraverseCursorResult<>(usageProfileObjectList, catalogTokenNew);
    }

    private TraverseCursorResult<List<UsageProfileObject>> getTableUsageProfileList(String projectId, String catalogName,
                                                                                    List<String> opTypes, long startTime, long endTime, String pageToken) throws CatalogServerException {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            Optional<CatalogToken> catalogToken = CatalogToken.parseToken(pageToken, usageProfileStoreCheckSum);
            if (!catalogToken.isPresent()) {
                catalogToken = Optional.of(new CatalogToken(usageProfileStoreCheckSum, new HashMap<>()));
            }

            TraverseCursorResult<List<UsageProfileObject>> accessLogRecordsCursor =
                getTableUsageProfileRecords(context, projectId, catalogName, null, null, opTypes, startTime, endTime,
                catalogToken.orElse(new CatalogToken(usageProfileStoreCheckSum, new HashMap<>())));
            return accessLogRecordsCursor;
        }).getResult();
    }


    /**
     * getTopTablesByCount
     *
     * @param projectId                 projectId
     * @param catalogName               catalogName
     * @param topTableUsageProfileInput topTableUsageProfileInput
     * @return List<TableUsageProfile>
     */
    @Override
    public List<TableUsageProfile> getTopTablesByCount(String projectId, String catalogName,
        TopTableUsageProfileInput topTableUsageProfileInput) {
        /*
        // table usageProfile belong record of history snapshot
        CatalogName name = new CatalogName(projectId, catalogName);
        CatalogObject catalogRecord = CatalogObjectHelper.getCatalogObject(name);
        CheckUtil.assertNotNull(catalogRecord, ErrorCode.CATALOG_NOT_FOUND, catalogName);
        */
        checkTime(topTableUsageProfileInput.getStartTime(), topTableUsageProfileInput.getEndTime());
        String[] opTypes = topTableUsageProfileInput.getOpTypes().split(";");
        List<String> opTypeList = Arrays.asList(opTypes);
        Map<TableName, BigInteger> tableCountMap = new HashMap<>();
        String pageToken = null;
        TraverseCursorResult<List<UsageProfileObject>> cursorResult;
        while (true) {
            cursorResult = getTableUsageProfileList(
                projectId, catalogName, opTypeList, topTableUsageProfileInput.getStartTime(),
                topTableUsageProfileInput.getEndTime(), pageToken);
            // TODO In the future, it will be added whether to display historical deleted table records,
            //  which are currently displayed by default
            TableName tableName;
            BigInteger count;
            for (UsageProfileObject value : cursorResult.getResult()) {
                tableName = new TableName(projectId, value.getCatalogName(), value.getDatabaseName(), value.getTableName());
                count = new BigInteger("0");
                if (tableCountMap.containsKey(tableName)) {
                    count = tableCountMap.get(tableName);
                }
                tableCountMap.put(tableName, count.add(BigInteger.valueOf(value.getCount())));
            }

            if (!cursorResult.getContinuation().isPresent()) {
                break;
            }
            pageToken = cursorResult.getContinuation().get().toString();
        }

        List<TableUsageProfile> tableUsageProfileList = new ArrayList<>();
        TableName tn;
        for (Map.Entry<TableName, BigInteger> entry : tableCountMap.entrySet()) {
            tn = entry.getKey();
            if (topTableUsageProfileInput.getEndTime() == topTableUsageProfileInput.getStartTime()) {
                TableUsageProfile tableUsageProfile = new TableUsageProfile(
                    new TableSource(projectId, catalogName, tn.getDatabaseName(), tn.getTableName()), opTypeList,
                    entry.getValue(), entry.getValue().longValue());
                tableUsageProfileList.add(tableUsageProfile);
            } else {
                // Calculate the number of minutes.
                long minNum = (topTableUsageProfileInput.getEndTime() - topTableUsageProfileInput.getStartTime())
                    / minute;
                long avgCount = 0;
                if (minNum > 0) {
                    avgCount = (entry.getValue().divide(BigInteger.valueOf(minNum))).longValue();
                }
                TableUsageProfile tableUsageProfile = new TableUsageProfile(
                    new TableSource(projectId, catalogName, tn.getDatabaseName(), tn.getTableName()), opTypeList,
                    entry.getValue(), avgCount);
                tableUsageProfileList.add(tableUsageProfile);
            }
        }

        switch (TopTableUsageProfileType.getTopType(topTableUsageProfileInput.getUsageProfileType())) {
            case TOP_HOT_TABLES:
                tableUsageProfileList.sort(Comparator.comparing(TableUsageProfile::getSumCount).reversed());
                break;
            case TOP_COLD_TABLES:
                tableUsageProfileList.sort(Comparator.comparing(TableUsageProfile::getSumCount));
                break;
            default:
                return tableUsageProfileList.subList(0, 0);
        }

        if (tableUsageProfileList.size() < topTableUsageProfileInput.getTopNum()) {
            return tableUsageProfileList;
        }
        return tableUsageProfileList.subList(0, (int) topTableUsageProfileInput.getTopNum());
    }

    private void checkTime(long startTime, long endTime) {
        if (endTime - startTime < 0) {
            throw new RuntimeException("Requires end Time to be greater than start Time.");
        }
    }

    private TraverseCursorResult<List<UsageProfileObject>> getTableUsageProfileListByTableId(TransactionContext context, TableName tableName, long startTime, long endTime, String pageToken) throws CatalogServerException {
        Optional<CatalogToken> catalogToken = CatalogToken.parseToken(pageToken, usageProfileStoreCheckSum);

        if (!catalogToken.isPresent()) {
            catalogToken = Optional.of(new CatalogToken(usageProfileStoreCheckSum, new HashMap<>()));
        }
        TraverseCursorResult<List<UsageProfileObject>> accessLogRecordsCursor =
                getTableUsageProfileRecords(context, tableName.getProjectId(), tableName.getCatalogName(), tableName.getDatabaseName(), tableName.getTableName(), null, startTime, endTime, catalogToken.get());
        return accessLogRecordsCursor;
    }

    /**
     * get SourceTables by table
     *
     * @param tableName     tableName
     * @param startTime startTime
     * @param endTime   endTime
     * @param userId
     * @param taskId
     * @param tag
     * @return List<TableUsageProfile>
     */
    @Override
    public List<TableUsageProfile> getUsageProfileByTable(TableName tableName, long startTime, long endTime, String userId, String taskId, String tag) {
        List<TableUsageProfile> tableUsageProfileList = new ArrayList<>();
        Map<String, BigInteger> tableOpTypeCountMap = new HashMap<>();
        TransactionRunnerUtil.transactionRunThrow(context -> {
            CatalogStringUtils.tableNameNormalize(tableName);
            String pageToken = null;
            while (true) {
                TraverseCursorResult<List<UsageProfileObject>> cursorResult =
                        getTableUsageProfileListByTableId(context, tableName, startTime, endTime, pageToken);

                for (UsageProfileObject value : cursorResult.getResult()) {
                    BigInteger count = new BigInteger("0");
                    if (tableOpTypeCountMap.containsKey(value.getOpType())) {
                        count = tableOpTypeCountMap.get(value.getOpType());
                    }
                    BigInteger countSum = count.add(BigInteger.valueOf(value.getCount()));
                    tableOpTypeCountMap.put(value.getOpType(), countSum);
                }

                if (!cursorResult.getContinuation().isPresent()) {
                    break;
                }
                pageToken = cursorResult.getContinuation().get().toString();
            }
            return null;
        }).getResult();


        for (Map.Entry<String, BigInteger> entry : tableOpTypeCountMap.entrySet()) {
            List<String> opTypes = new ArrayList<>();
            opTypes.add(entry.getKey());
            TableUsageProfile tableUsageProfile = new TableUsageProfile(
                new TableSource(tableName.getProjectId(), tableName.getCatalogName(), tableName.getDatabaseName(), tableName.getTableName()), opTypes,
                entry.getValue(), entry.getValue().longValue());
            tableUsageProfileList.add(tableUsageProfile);
        }
        return tableUsageProfileList;
    }

    @Override
    public void deleteUsageProfilesByTime(String projectId, long startTime, long endTime) {
        TransactionRunnerUtil.transactionRunThrow(context -> {
            usageProfileStore.deleteUsageProfile(context, projectId, startTime, endTime);
            return null;
        });
    }

    @Override
    public List<TableUsageProfile> getUsageProfileGroupByUser(TableName tableName, long startTime, long endTime, String opTypes, boolean sortAsc) {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            CatalogStringUtils.tableNameNormalize(tableName);
            List<TableUsageProfile> list = new ArrayList<>();
            UsageProfileAccessStatObject accessStatObject = UsageProfileAccessStatObject.builder()
                    .catalogName(tableName.getCatalogName())
                    .databaseName(tableName.getDatabaseName())
                    .tableName(tableName.getTableName())
                    .startTime(startTime)
                    .endTime(endTime)
                    .build();
            Set<String> opTypesSet = getOpTypesSet(opTypes);
            List<UsageProfileAccessStatObject> accessStatObjects = usageProfileStore.getUsageProfileAccessStatList(tableName.getProjectId(), accessStatObject, opTypesSet, sortAsc);
            TableUsageProfile tup;
            for (UsageProfileAccessStatObject u: accessStatObjects) {
                tup = new TableUsageProfile();
                tup.setTable(new TableSource(tableName.getProjectId(), u.getCatalogName(), u.getDatabaseName(), u.getTableName()));
                tup.setSumCount(BigInteger.valueOf(u.getSumCount()));
                tup.setUserId(u.getUserId());
                tup.setCreateTimestamp(u.getEndTime());
                list.add(tup);
            }
            return list;
        }).getResult();
    }

    @Override
    public List<TableAccessUsers> getTableAccessUsers(String projectId, List<TableSource> tableSources) {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            List<TableAccessUsers> taus = new ArrayList<>();
            TableAccessUsers tau;
            for (TableSource ts: tableSources) {
                normalizeIdentifier(ts);
                List<String> userIds = usageProfileStore.getTableAccessUsers(context, projectId, ts.getCatalogName(), ts.getDatabaseName(), ts.getTableName());
                if (CollectionUtils.isNotEmpty(userIds)) {
                    tau = new TableAccessUsers();
                    tau.setUsers(userIds);
                    tau.setTable(ts);
                    taus.add(tau);
                } else {
                    log.warn("Get table access user not found: {}", ts);
                }
            }
            return taus;
        }).getResult();
    }

    @Override
    public List<TableUsageProfile> getUsageProfileDetailsByCondition(TableSource tableSource, long startTime, long endTime, String userId, String taskId, int rowCount, String tag) {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            normalizeIdentifier(tableSource);
            String projectId = tableSource.getProjectId();
            UsageProfileObject upo = UsageProfileObject.builder()
                    .catalogName(tableSource.getCatalogName())
                    .databaseName(tableSource.getDatabaseName())
                    .tableName(tableSource.getTableName())
                    .userId(userId)
                    .taskId(taskId)
                    .tag(tag)
                    .build();
            List<UsageProfileObject> details = usageProfileStore.getUsageProfileDetailsByCondition(context, projectId, upo, startTime, endTime, rowCount);
            return buildTableUsageProfile(projectId, details);
        }).getResult();
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
                tup.setTaskId(upo.getTaskId());
                tup.setCreateDayTimestamp(upo.getCreateDayTime());
                tup.setCreateTimestamp(upo.getCreateTime());
                list.add(tup);
            }
        }
        return list;
    }

    private static final String opTypesDelimiter = ";";

    private Set<String> getOpTypesSet(String opTypes) {
        Set<String> opTypesSet = new HashSet<>();
        if (StringUtils.isNotBlank(opTypes)) {
            opTypesSet.addAll(Arrays.asList(opTypes.split(opTypesDelimiter)));
        }
        return opTypesSet;
    }
}
