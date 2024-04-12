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

import java.util.*;
import java.util.stream.Collectors;

import com.google.protobuf.InvalidProtocolBufferException;
import io.polycat.catalog.common.model.DatabaseHistoryObject;
import io.polycat.catalog.common.model.DatabaseObject;
import io.polycat.catalog.common.model.DroppedDatabaseNameObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;
import io.polycat.catalog.store.api.DatabaseStore;
import io.polycat.catalog.store.common.StoreSqlConvertor;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.gaussdb.pojo.DatabaseHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.DatabaseRecord;
import io.polycat.catalog.store.gaussdb.pojo.DroppedDatabaseNameRecord;
import io.polycat.catalog.store.mapper.DatabaseMapper;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.protos.common.DatabaseInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class DatabaseStoreImpl implements DatabaseStore {

    @Autowired
    private DatabaseMapper databaseMapper;

    @Override
    public void createDatabaseSubspace(TransactionContext context, String projectId) {
        databaseMapper.createDatabaseSubspace(projectId);
    }


    @Override
    public void insertDatabase(TransactionContext ctx, DatabaseIdent databaseIdent, DatabaseObject databaseObject) {
       byte[] databaseInfo = getDatabaseInfoByteArray(databaseObject);
       databaseMapper.insertDatabase(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
           databaseIdent.getDatabaseId(), databaseObject.getName(), databaseInfo);
    }

    @Override
    public String generateDatabaseId(TransactionContext context, String projectId, String catalogId) throws MetaStoreException {
        return SequenceHelper.generateObjectId(context, projectId);
    }

    @Override
    public void deleteDatabase(TransactionContext ctx, DatabaseIdent databaseIdent) {
        databaseMapper
            .deleteDatabase(databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getDatabaseId());
    }

    @Override
    public DatabaseObject getDatabase(TransactionContext ctx, DatabaseIdent databaseIdent) {
        try {
            DatabaseRecord databaseRecord = databaseMapper.getDatabaseById(databaseIdent.getProjectId(),
                databaseIdent.getCatalogId(), databaseIdent.getDatabaseId());
            if (databaseRecord == null) {
                return null;
            }

            DatabaseInfo databaseInfo = DatabaseInfo.parseFrom(databaseRecord.getDatabaseInfo());
            return new DatabaseObject(databaseIdent, databaseRecord.getDatabaseName(), databaseInfo);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public List<DatabaseObject> getDatabaseByIds(TransactionContext ctx, String projectId, String catalogId, List<String> databaseIds) {
        try {
            String filterSql = StoreSqlConvertor.get().equals(DatabaseRecord.Fields.catalogId, catalogId).AND()
                    .in(DatabaseRecord.Fields.databaseId, databaseIds).getFilterSql();
            List<DatabaseRecord> databaseRecords = databaseMapper.getDatabasesByFilter(projectId, filterSql);
            List<DatabaseObject> databaseObjects = new ArrayList<>();
            DatabaseInfo databaseInfo;
            for (DatabaseRecord dr: databaseRecords) {
                databaseInfo = DatabaseInfo.parseFrom(dr.getDatabaseInfo());
                databaseObjects.add(new DatabaseObject(new DatabaseIdent(projectId, catalogId, dr.getDatabaseId()), dr.getDatabaseName(), databaseInfo));
            }
            return databaseObjects;
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void updateDatabase(TransactionContext context, DatabaseIdent databaseIdent,
        DatabaseObject newDatabaseObject) {
        try {
            byte[] bytes = getDatabaseInfoByteArray(newDatabaseObject);
            databaseMapper.updateDatabase(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
                databaseIdent.getDatabaseId(), newDatabaseObject.getName(), bytes);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void upsertDatabase(TransactionContext ctx, DatabaseIdent databaseIdent, DatabaseObject databaseObject) {
        byte[] databaseInfo = getDatabaseInfoByteArray(databaseObject);
        DatabaseRecord databaseRecord = databaseMapper.getDatabaseById(databaseIdent.getProjectId(),
            databaseIdent.getCatalogId(), databaseIdent.getDatabaseId());
        if (databaseRecord == null) {
            databaseMapper.insertDatabase(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
                databaseIdent.getDatabaseId(), databaseObject.getName(), databaseInfo);
        } else {
            databaseMapper.updateDatabase(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
                databaseIdent.getDatabaseId(), databaseObject.getName(), databaseInfo);
        }
    }


    /**
     * database history
     */

    @Override
    public void createDatabaseHistorySubspace(TransactionContext context, String projectId) throws MetaStoreException {
        try {
            databaseMapper.createDatabaseHistorySubspace(projectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void insertDatabaseHistory(TransactionContext context, DatabaseHistoryObject databaseHistoryObject,
        DatabaseIdent databaseIdent, boolean dropped, String version) {
        try {
            byte[] bytes = getDatabaseInfoByteArray(databaseHistoryObject);
            databaseMapper.insertDatabaseHistory(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
                databaseIdent.getDatabaseId(),
                UuidUtil.generateUUID32(), version, databaseHistoryObject.getName(), bytes);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }

    }

    @Override
    public Optional<DatabaseHistoryObject> getLatestDatabaseHistory(TransactionContext context, DatabaseIdent databaseIdent,
                                                                    String basedVersion) {
        try {
            DatabaseHistoryRecord databaseHistoryRecord = databaseMapper.getLatestDatabaseHistory(databaseIdent.getProjectId(),
                    databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), basedVersion);
            if (databaseHistoryRecord == null) {
                return Optional.empty();
            }
            DatabaseInfo databaseInfo = DatabaseInfo.parseFrom(databaseHistoryRecord.getDatabaseInfo());
            DatabaseHistoryObject databaseHistoryObject = new DatabaseHistoryObject(databaseHistoryRecord.getDbhId(),
                    databaseHistoryRecord.getDatabaseName(), databaseHistoryRecord.getVersion(), databaseInfo);
            return Optional.of(databaseHistoryObject);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Map<String, DatabaseHistoryObject> getLatestDatabaseHistoryByIds(TransactionContext context, String projectId, String catalogId,
                                                                     String basedVersion, List<String> databaseIds) {
        try {
            String filterSql = StoreSqlConvertor.get().equals(DatabaseHistoryRecord.Fields.catalogId, catalogId).AND()
                    .in(DatabaseHistoryRecord.Fields.databaseId, databaseIds).AND()
                    .lessThanOrEquals(DatabaseHistoryRecord.Fields.version, basedVersion).getFilterSql();
            List<DatabaseHistoryRecord> databaseHistoryRecords = databaseMapper.getLatestDatabaseHistoryByFilter(projectId, filterSql);
            Map<String, DatabaseHistoryObject> map = new HashMap<>();
            DatabaseInfo databaseInfo;
            for (DatabaseHistoryRecord record: databaseHistoryRecords) {
                databaseInfo = DatabaseInfo.parseFrom(record.getDatabaseInfo());
                map.put(record.getDatabaseId(), new DatabaseHistoryObject(record.getDbhId(), record.getDatabaseName(), record.getVersion(), databaseInfo));
            }
            return map;
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Optional<DatabaseHistoryObject> getDatabaseHistory(TransactionContext context, DatabaseIdent databaseIdent,
        String baseVersion) {
        try {
            DatabaseHistoryRecord databaseHistoryRecord = databaseMapper.getDatabaseHistory(databaseIdent.getProjectId(),
                databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), baseVersion);
            if (databaseHistoryRecord == null) {
                return Optional.empty();
            }
            DatabaseInfo databaseInfo = DatabaseInfo.parseFrom(databaseHistoryRecord.getDatabaseInfo());
            DatabaseHistoryObject databaseHistoryObject = new DatabaseHistoryObject(databaseHistoryRecord.getDbhId(),
                databaseHistoryRecord.getDatabaseName(), databaseHistoryRecord.getVersion(), databaseInfo);
            return Optional.of(databaseHistoryObject);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public ScanRecordCursorResult<List<DatabaseHistoryObject>> listDatabaseHistory(TransactionContext context, DatabaseIdent databaseIdent,
        int maxBatchRowNum, byte[] continuation, String baseVersion) {
        try {
            long offset = 0;
            if (continuation != null) {
                offset = CodecUtil.bytesToLong(continuation);
            }
            List<DatabaseHistoryRecord> databaseHistoryRecords = databaseMapper.listDatabaseHistory(databaseIdent.getProjectId(),
                databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), baseVersion, offset, maxBatchRowNum);
            List<DatabaseHistoryObject> databaseHistoryObjects = new ArrayList<>(databaseHistoryRecords.size());
            databaseHistoryRecords.forEach(databaseHistoryRecord -> {
                DatabaseInfo databaseInfo = null;
                try {
                    databaseInfo = DatabaseInfo.parseFrom(databaseHistoryRecord.getDatabaseInfo());
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                    log.info(e.getMessage());
                    throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
                }

                databaseHistoryObjects.add(new DatabaseHistoryObject(databaseHistoryRecord.getDbhId(),
                    databaseHistoryRecord.getDatabaseName(), databaseHistoryRecord.getVersion(), databaseInfo));
            });

            if (databaseHistoryRecords.size() < maxBatchRowNum) {
                return new ScanRecordCursorResult<>(databaseHistoryObjects, null);
            } else {
                return new ScanRecordCursorResult<>(databaseHistoryObjects, CodecUtil.longToBytes(offset + maxBatchRowNum));
            }
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }


    /**
     * database dropped object name
     */

    @Override
    public void createDroppedDatabaseNameSubspace(TransactionContext context, String projectId) {
        databaseMapper.createDroppedDatabaseNameSubspace(projectId);
    }

    @Override
    public void deleteDroppedDatabaseObjectName(TransactionContext ctx, DatabaseIdent databaseIdent,
        String databaseName) {
        databaseMapper.deleteDroppedDatabaseName(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
            databaseIdent.getDatabaseId(), databaseName);

    }

    @Override
    public DroppedDatabaseNameObject getDroppedDatabaseObjectName(TransactionContext context,
        DatabaseIdent databaseIdent,
        String databaseName) {
        DroppedDatabaseNameRecord droppedDatabaseNameRecord = databaseMapper
            .getDroppedDatabaseName(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
                databaseIdent.getDatabaseId(), databaseName);
        if (droppedDatabaseNameRecord != null) {
            return new DroppedDatabaseNameObject(databaseIdent.getCatalogId(), droppedDatabaseNameRecord.getDatabaseName(),
                droppedDatabaseNameRecord.getDatabaseId(), droppedDatabaseNameRecord.getDropTime());
        }

        return null;
    }

    @Override
    public ScanRecordCursorResult<List<DroppedDatabaseNameObject>> listDroppedDatabaseObjectName(TransactionContext context,
        CatalogIdent catalogIdent, int maxBatchRowNum, byte[] continuation, TransactionIsolationLevel isolationLevel) {
        try {
            long offset = 0;
            if (continuation != null) {
                offset = CodecUtil.bytesToLong(continuation);
            }
            List<DroppedDatabaseNameRecord> droppedDatabaseNameRecords = databaseMapper.listDroppedDatabaseName(catalogIdent.getProjectId(),
                catalogIdent.getCatalogId(), offset, maxBatchRowNum);
            List<DroppedDatabaseNameObject> droppedDatabaseNameObjects = droppedDatabaseNameRecords.stream().map(
                droppedDatabaseNameRecord -> new DroppedDatabaseNameObject(catalogIdent.getCatalogId(),
                    droppedDatabaseNameRecord.getDatabaseName(),
                    droppedDatabaseNameRecord.getDatabaseId(), droppedDatabaseNameRecord.getDropTime()))
                .collect(Collectors.toList());

            if (droppedDatabaseNameRecords.size() < maxBatchRowNum) {
                return new ScanRecordCursorResult<>(droppedDatabaseNameObjects, null);
            } else {
                return new ScanRecordCursorResult<>(droppedDatabaseNameObjects, CodecUtil.longToBytes(offset + maxBatchRowNum));
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void insertDroppedDatabaseObjectName(TransactionContext ctx, DatabaseIdent databaseIdent,
        String databaseName) {
        databaseMapper.insertDroppedDatabaseName(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
            databaseIdent.getDatabaseId(),
            databaseName, RecordStoreHelper.getCurrentTime());
    }

    /**
     * database object name
     */


    @Override
    public Optional<String> getDatabaseId(TransactionContext context, CatalogIdent catalogIdent,
        String databaseName) {
        String databaseId = databaseMapper
            .getDatabaseId(catalogIdent.getProjectId(), catalogIdent.getCatalogId(), databaseName);
        if (databaseId == null) {
            return Optional.empty();
        } else {
            return Optional.of(databaseId);
        }
    }

    @Override
    public ScanRecordCursorResult<List<String>> listDatabaseId(TransactionContext context,
        CatalogIdent catalogIdent, int maxBatchRowNum, byte[] continuation, TransactionIsolationLevel isolationLevel) {
        try {
            long offset = 0;
            if (continuation != null) {
                offset = CodecUtil.bytesToLong(continuation);
            }
            List<String> databaseIdRecords = databaseMapper.listDatabaseId(catalogIdent.getProjectId(),
                catalogIdent.getCatalogId(), offset, maxBatchRowNum);

            if (databaseIdRecords.size() < maxBatchRowNum) {
                return new ScanRecordCursorResult<>(databaseIdRecords, null);
            } else {
                return new ScanRecordCursorResult<>(databaseIdRecords, CodecUtil.longToBytes(offset + maxBatchRowNum));
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.warn(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void dropDatabaseSubspace(TransactionContext context, String projectId) {
        databaseMapper.dropDatabaseSubspace(projectId);
    }

    @Override
    public void dropDatabaseHistorySubspace(TransactionContext context, String projectId) {
        databaseMapper.dropDatabaseHistorySubspace(projectId);
    }

    @Override
    public void dropDroppedDatabaseNameSubspace(TransactionContext context, String projectId) {
        databaseMapper.dropDroppedDatabaseNameSubspace(projectId);
    }


    private byte[] getDatabaseInfoByteArray(DatabaseObject databaseObject) {
        DatabaseInfo databaseInfo = DatabaseInfo.newBuilder()
                .setCreateTime(databaseObject.getCreateTime())
                .setLocation(databaseObject.getLocation())
                .setDescription(databaseObject.getDescription())
                .setUserId(databaseObject.getUserId())
                .setOwner(databaseObject.getUserId())
                .setOwnerType(databaseObject.getOwnerType())
            .putAllProperties(databaseObject.getProperties())
            .build();
        return databaseInfo.toByteArray();
    }

    private byte[] getDatabaseInfoByteArray(DatabaseHistoryObject databaseHistoryObject) {
        DatabaseInfo databaseInfo = DatabaseInfo.newBuilder()
            .setCreateTime(databaseHistoryObject.getCreateTime())
            .setLocation(databaseHistoryObject.getLocation())
            .setDescription(databaseHistoryObject.getDescription())
            .putAllProperties(databaseHistoryObject.getProperties())
            .setDroppedTime(databaseHistoryObject.getDroppedTime())
            .build();
        return databaseInfo.toByteArray();
    }

}
