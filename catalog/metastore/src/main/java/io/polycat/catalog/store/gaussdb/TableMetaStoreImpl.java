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

import io.polycat.catalog.common.model.TableBaseHistoryObject;
import io.polycat.catalog.common.model.TableBaseObject;
import io.polycat.catalog.common.model.TableCommitObject;
import io.polycat.catalog.common.model.TableNameObject;
import io.polycat.catalog.common.model.TableObject;
import io.polycat.catalog.common.model.TableReferenceObject;
import io.polycat.catalog.common.model.TableSchemaHistoryObject;
import io.polycat.catalog.common.model.TableSchemaObject;
import io.polycat.catalog.common.model.TableStorageHistoryObject;
import io.polycat.catalog.common.model.TableStorageObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;
import io.polycat.catalog.store.gaussdb.pojo.DroppedTableRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableBaseHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableBaseRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableCommitRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableSchemaHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableSchemaRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableStorageHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.TableStorageRecord;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DroppedTableObject;
import io.polycat.catalog.common.model.OperationObject;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TableOperationType;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.api.TableMetaStore;
import io.polycat.catalog.store.common.StoreSqlConvertor;
import io.polycat.catalog.store.common.TableStoreConvertor;
import io.polycat.catalog.store.mapper.SequenceMapper;
import io.polycat.catalog.store.mapper.TableMetaMapper;
import io.polycat.catalog.store.protos.Operation;
import io.polycat.catalog.store.protos.OperationList;
import io.polycat.catalog.store.protos.common.SchemaInfo;
import io.polycat.catalog.store.protos.common.StorageInfo;
import io.polycat.catalog.store.protos.common.TableBaseInfo;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class TableMetaStoreImpl implements TableMetaStore {

    private static final String tableNotExist = "42P01";

    @Autowired
    TableMetaMapper tableMetaMapper;

    @Autowired
    private SequenceMapper sequenceMapper;

    /*
    table subspace
     */

    @Override
    public void createTableSubspace(TransactionContext context, CatalogIdent catalogIdent) throws MetaStoreException {
        try {
            tableMetaMapper.createTableSubspace(catalogIdent.getProjectId(), catalogIdent.getCatalogId());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void dropTableSubspace(TransactionContext context, CatalogIdent catalogIdent)
        throws MetaStoreException {
        try {
            tableMetaMapper.dropTableSubspace(catalogIdent.getProjectId(), catalogIdent.getCatalogId());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public String generateTableId(TransactionContext context, String projectId, String catalogId) throws MetaStoreException {
        return SequenceHelper.generateObjectId(context, projectId);
    }

    @Override
    public void insertTable(TransactionContext context, TableIdent tableIdent, String tableName,
        int historySubspaceFlag, TableBaseObject tableBaseObject, TableSchemaObject tableSchemaObject,
        TableStorageObject tableStorageObject)
        throws MetaStoreException {
        try {
            byte[] baseBytes = TableStoreConvertor.getTableBaseInfo(tableBaseObject).toByteArray();
            byte[] schemaBytes = TableStoreConvertor.getSchemaInfo(tableSchemaObject).toByteArray();
            byte[] storageBytes = TableStoreConvertor.getTableStorageInfo(tableStorageObject).toByteArray();
            TableRecord tableRecord = new TableRecord(tableIdent, tableName, historySubspaceFlag,
                baseBytes, schemaBytes, storageBytes);
            tableMetaMapper.insertTable(tableRecord);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void upsertTable(TransactionContext context, TableIdent tableIdent, String tableName,
        int historySubspaceFlag, TableBaseObject tableBaseObject, TableSchemaObject tableSchemaObject,
        TableStorageObject tableStorageObject)
        throws MetaStoreException {
        try {
            byte[] baseBytes = TableStoreConvertor.getTableBaseInfo(tableBaseObject).toByteArray();
            byte[] schemaBytes = TableStoreConvertor.getSchemaInfo(tableSchemaObject).toByteArray();
            byte[] storageBytes = TableStoreConvertor.getTableStorageInfo(tableStorageObject).toByteArray();
            TableRecord tableRecord = new TableRecord(tableIdent, tableName, historySubspaceFlag,
                baseBytes, schemaBytes, storageBytes);
            tableMetaMapper.updateTable(tableRecord);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public TableObject getTable(TransactionContext context, TableIdent tableIdent,
        TableName tableName) throws MetaStoreException {
        try {
            TableRecord tableRecord = tableMetaMapper.getTable(tableIdent.getProjectId(),
                tableIdent.getCatalogId(), tableIdent.getTableId());
            return convertTableObject(tableIdent, tableName, tableRecord);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    private TableObject convertTableObject(TableIdent tableIdent, TableName tableName, TableRecord tableRecord) {
        try {
            TableBaseObject tableBaseObject = new TableBaseObject(TableBaseInfo.parseFrom(tableRecord.getBase()));
            TableSchemaObject tableSchemaObject = new TableSchemaObject(SchemaInfo.parseFrom(tableRecord.getSchema()));
            TableStorageObject tableStorageObject = new TableStorageObject(StorageInfo.parseFrom(tableRecord.getStorage()));
            return new TableObject(tableIdent, tableName, tableRecord.getHistorySubspaceFlag(),
                    tableBaseObject, tableSchemaObject, tableStorageObject, 0);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            throw new MetaStoreException(e.getMessage());
        }
    }

    @Override
    public int getTableHistorySubspaceFlag(TransactionContext context, TableIdent tableIdent)
        throws MetaStoreException {
        try {
            Integer historySubspaceFlag = tableMetaMapper.getTableHistorySubspaceFlag(tableIdent.getProjectId(),
                tableIdent.getCatalogId(), tableIdent.getTableId());
            if (historySubspaceFlag == null) {
                return 0;
            }
            return historySubspaceFlag;
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void deleteTable(TransactionContext context, TableIdent tableIdent, String tableName)
        throws MetaStoreException {
        try {
            tableMetaMapper.deleteTable(tableIdent.getProjectId(), tableIdent.getCatalogId(), tableIdent.getTableId());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    /**
     * table object name
     */

    @Override
    public String getTableId(TransactionContext context, DatabaseIdent databaseIdent,
        String tableName) throws MetaStoreException {
        try {
            return tableMetaMapper.getTableId(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
                databaseIdent.getDatabaseId(), tableName);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public ScanRecordCursorResult<List<TableNameObject>> listTableName(TransactionContext context,
        DatabaseIdent databaseIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel)
        throws MetaStoreException {
        try {
            long offset = 0;
            if (continuation != null) {
                offset = CodecUtil.bytesToLong(continuation);
            }
            List<TableNameObject> tableNameObjectList = tableMetaMapper.listTableObjectName(databaseIdent.getProjectId(),
                databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), offset, maxNum);
            if (tableNameObjectList.size() < maxNum) {
                return new ScanRecordCursorResult<>(tableNameObjectList, null);
            } else {
                return new ScanRecordCursorResult<>(tableNameObjectList, CodecUtil.longToBytes(offset + maxNum));
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.warn(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }

    }

    @Override
    public String createTableObjectNameTmpTable(TransactionContext context, DatabaseIdent databaseIdent) throws MetaStoreException {
        try {
            String tmpTableName = "tmp_" + UuidUtil.generateUUID32();
            tableMetaMapper.createTableObjectNameTmpTable(databaseIdent.getProjectId(),
                    databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(), tmpTableName);
            return tmpTableName;
        } catch (Exception e) {
            log.warn(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    /**
     *  table reference
     */

    @Override
    public void createTableReferenceSubspace(TransactionContext context, CatalogIdent catalogIdent)
        throws MetaStoreException {
        try {
            tableMetaMapper.createTableReferenceSubspace(catalogIdent.getProjectId(), catalogIdent.getCatalogId());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void dropTableReferenceSubspace(TransactionContext context, CatalogIdent catalogIdent)
        throws MetaStoreException {
        try {
            tableMetaMapper.dropTableReferenceSubspace(catalogIdent.getProjectId(), catalogIdent.getCatalogId());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void insertTableReference(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {
        try {
            tableMetaMapper.insertTableReference(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getTableId(), System.currentTimeMillis());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void upsertTableReference(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {
        try {
            tableMetaMapper.updateTableReference(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getTableId(), System.currentTimeMillis());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public TableReferenceObject getTableReference(TransactionContext context, TableIdent tableIdent)
        throws MetaStoreException {
        try {
            long updateTime = tableMetaMapper.getTableReference(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getTableId());
            return new TableReferenceObject(updateTime, updateTime);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void deleteTableReference(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {
        try {
            tableMetaMapper.deleteTableReference(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getTableId());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    /**
     * table base
     */

    @Override
    public TableBaseObject getTableBase(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {
        try {
            TableBaseRecord tableBase = tableMetaMapper.getTableBase(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getTableId());
            if (tableBase == null) {
                return null;
            }
            return new TableBaseObject(TableBaseInfo.parseFrom(tableBase.getBase()));
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }



    /**
     * table schema
     */

    @Override
    public TableSchemaObject getTableSchema(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {
        try {
            TableSchemaRecord tableSchemaRecord = tableMetaMapper.getTableSchema(tableIdent.getProjectId(),
                tableIdent.getCatalogId(), tableIdent.getTableId());
            if (tableSchemaRecord == null) {
                return null;
            }
            SchemaInfo schemaInfo = SchemaInfo.parseFrom(tableSchemaRecord.getSchema());
            return new TableSchemaObject(schemaInfo);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    /**
     * table storage
     */

    @Override
    public TableStorageObject getTableStorage(TransactionContext context, TableIdent tableIdent) throws MetaStoreException {
        try {
            TableStorageRecord tableStorageRecord = tableMetaMapper.getTableStorage(tableIdent.getProjectId(),
                tableIdent.getCatalogId(), tableIdent.getTableId());
            if (tableStorageRecord == null) {
                return null;
            }
            return new TableStorageObject(StorageInfo.parseFrom(tableStorageRecord.getStorage()));
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    /**
     * table base history subspace
     */

    @Override
    public void createTableBaseHistorySubspace(TransactionContext context, String projectId)
        throws MetaStoreException {
        try {
            tableMetaMapper.createTableBaseHistorySubspace(projectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void dropTableBaseHistorySubspace(TransactionContext context, TableIdent tableIdent)
        throws MetaStoreException{
        try {
            tableMetaMapper.dropTableBaseHistorySubspace(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getTableId());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void insertTableBaseHistory(TransactionContext context, TableIdent tableIdent,
        String version, TableBaseObject tableBaseObject) throws MetaStoreException {
        try {
            byte[] baseBytes = TableStoreConvertor.getTableBaseInfo(tableBaseObject).toByteArray();
            tableMetaMapper.insertTableBaseHistory(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getTableId(), UuidUtil.generateUUID32(), version, baseBytes);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Optional<TableBaseHistoryObject> getLatestTableBase(TransactionContext context,
        TableIdent tableIdent, String basedVersion) throws MetaStoreException {
        try {
            if (!tableMetaMapper.tableBaseHistorySubspaceExist(tableIdent.getProjectId(),
                tableIdent.getCatalogId(), tableIdent.getTableId())) {
                return Optional.empty();
            }

            TableBaseHistoryRecord tableBaseHistoryRecord = tableMetaMapper.getLatestTableBaseHistory(
                tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getTableId(), basedVersion);
            if (tableBaseHistoryRecord == null) {
                return Optional.empty();
            }
            TableBaseInfo tableBaseInfo = TableBaseInfo.parseFrom(tableBaseHistoryRecord.getBase());
            TableBaseObject tableBaseObject = new TableBaseObject(tableBaseInfo);
            return Optional.of(new TableBaseHistoryObject(tableBaseHistoryRecord.getBaseHisId(),
                tableBaseHistoryRecord.getVersion(), tableBaseObject));
        } catch (Exception e) {
            log.warn(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Map<String, TableBaseHistoryObject> getLatestTableBaseByIds(TransactionContext context, String projectId, String catalogId, String databaseId, String basedVersion, List<String> tableIds) throws MetaStoreException {
            try {
                String filterSql = StoreSqlConvertor.get().equals(TableBaseHistoryRecord.Fields.catalogId, catalogId).AND()
                        .lessThanOrEquals(TableBaseHistoryRecord.Fields.version, basedVersion).AND()
                        .in(TableBaseHistoryRecord.Fields.tableId, tableIds).getFilterSql();
                List<TableBaseHistoryRecord> tableBaseHistoryRecords = tableMetaMapper.getLatestTableBaseHistoryByFilter(projectId, filterSql);
                Map<String, TableBaseHistoryObject> map = new HashMap<>();
                TableBaseInfo tableBaseInfo;
                for (TableBaseHistoryRecord record: tableBaseHistoryRecords) {
                    tableBaseInfo = TableBaseInfo.parseFrom(record.getBase());
                    map.put(record.getTableId(), new TableBaseHistoryObject(record.getBaseHisId(),
                            record.getVersion(), new TableBaseObject(tableBaseInfo)));
                }
                return map;
            } catch (Exception e) {
                log.warn(e.getMessage());
                throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
            }
    }

    @Override
    public TableBaseHistoryObject getLatestTableBaseOrElseThrow(TransactionContext context,
        TableIdent tableIdent, String basedVersion) throws MetaStoreException {
        Optional<TableBaseHistoryObject> tableBaseHistoryObjectOptional = getLatestTableBase(context, tableIdent,
            basedVersion);
        if (!tableBaseHistoryObjectOptional.isPresent()) {
            throw new MetaStoreException(ErrorCode.TABLE_BASE_NOT_FOUND, tableIdent.getTableId());
        }
        return tableBaseHistoryObjectOptional.get();
    }

    @Override
    public ScanRecordCursorResult<List<TableBaseHistoryObject>> listTableBaseHistory(
        TransactionContext context, TableIdent tableIdent, int maxNum, byte[] continuation,
        TransactionIsolationLevel isolationLevel, String baseVersion) throws MetaStoreException {
        try {
            long offset = 0;
            if (continuation != null) {
                offset = CodecUtil.bytesToLong(continuation);
            }
            List<TableBaseHistoryRecord> tableBaseHistoryRecords = tableMetaMapper.listTableBaseHistory(
                tableIdent.getProjectId(), tableIdent.getCatalogId(), tableIdent.getTableId(),
                baseVersion, offset, maxNum);
            List<TableBaseHistoryObject> tableBaseHistoryObjects = new ArrayList<>(tableBaseHistoryRecords.size());
            tableBaseHistoryRecords.forEach(tableBaseHistoryRecord -> {
                TableBaseInfo tableBaseInfo = null;
                try {
                    tableBaseInfo = TableBaseInfo.parseFrom(tableBaseHistoryRecord.getBase());
                } catch (InvalidProtocolBufferException e) {
                    log.info(e.getMessage());
                    throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
                }
                TableBaseObject tableBaseObject = new TableBaseObject(tableBaseInfo);
                tableBaseHistoryObjects.add(new TableBaseHistoryObject(tableBaseHistoryRecord.getBaseHisId(),
                    tableBaseHistoryRecord.getVersion(), tableBaseObject));
            });
            if (tableBaseHistoryRecords.size() < maxNum) {
                return new ScanRecordCursorResult<>(tableBaseHistoryObjects, null);
            } else {
                return new ScanRecordCursorResult<>(tableBaseHistoryObjects, CodecUtil.longToBytes(offset + maxNum));
            }
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public byte[] deleteTableBaseHistory(TransactionContext context, TableIdent tableIdent, String startVersion,
        String endVersion, byte[] continuation) throws MetaStoreException {
        throw new MetaStoreException(ErrorCode.FEATURE_NOT_SUPPORT, "deleteTableBaseHistory");
    }

    /**
     * table schema history subspace
     */

    @Override
    public void createTableSchemaHistorySubspace(TransactionContext context, String projectId)
        throws MetaStoreException {
        try {
            tableMetaMapper.createTableSchemaHistorySubspace(projectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void dropTableSchemaHistorySubspace(TransactionContext context, String projectId)
        throws MetaStoreException {
        try {
            tableMetaMapper.dropTableSchemaHistorySubspace(projectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void insertTableSchemaHistory(TransactionContext context, TableIdent tableIdent, String version,
        TableSchemaObject tableSchemaObject) throws MetaStoreException {
        try {
            byte[] bytes = TableStoreConvertor.getSchemaInfo(tableSchemaObject).toByteArray();
            tableMetaMapper.insertTableSchemaHistory(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getTableId(), UuidUtil.generateUUID32(), version, bytes);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void upsertTableSchemaHistory(TransactionContext context, TableIdent tableIdent, String version,
        TableSchemaObject tableSchemaObject) throws MetaStoreException {
        insertTableSchemaHistory(context, tableIdent, version, tableSchemaObject);
    }

    @Override
    public Optional<TableSchemaHistoryObject> getLatestTableSchema(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        try {
            if (!tableMetaMapper.tableSchemaHistorySubspaceExist(tableIdent.getProjectId(),
                tableIdent.getCatalogId(), tableIdent.getTableId())) {
                return Optional.empty();
            }

            TableSchemaHistoryRecord tableSchemaHistoryRecord = tableMetaMapper.getLatestTableSchemaHistory(
                tableIdent.getProjectId(), tableIdent.getCatalogId(), tableIdent.getTableId(), basedVersion);
            if (tableSchemaHistoryRecord == null) {
                return Optional.empty();
            }
            SchemaInfo schemaInfo = SchemaInfo.parseFrom(tableSchemaHistoryRecord.getSchema());
            TableSchemaObject tableSchemaObject = new TableSchemaObject(schemaInfo);
            TableSchemaHistoryObject tableSchemaHistoryObject = new TableSchemaHistoryObject(
                tableSchemaHistoryRecord.getSchemaHisId(), tableSchemaHistoryRecord.getVersion(), tableSchemaObject);
            return Optional.of(tableSchemaHistoryObject);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public TableSchemaHistoryObject getLatestTableSchemaOrElseThrow(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        Optional<TableSchemaHistoryObject> tableSchemaHistoryObjectOptional = getLatestTableSchema(context,
            tableIdent, basedVersion);
        if (!tableSchemaHistoryObjectOptional.isPresent()) {
            throw new MetaStoreException(ErrorCode.TABLE_SCHEMA_NOT_FOUND, tableIdent.getTableId());
        }
        return tableSchemaHistoryObjectOptional.get();
    }

    @Override
    public ScanRecordCursorResult<List<TableSchemaHistoryObject>> listTableSchemaHistory(TransactionContext context,
        TableIdent tableIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel,
        String baseVersion) throws MetaStoreException {
        try {
            long offset = 0;
            if (continuation != null) {
                offset = CodecUtil.bytesToLong(continuation);
            }
            List<TableSchemaHistoryRecord> tableSchemaHistoryRecords = tableMetaMapper.listTableSchemaHistory(
                tableIdent.getProjectId(), tableIdent.getCatalogId(), tableIdent.getTableId(),
                baseVersion, offset, maxNum);
            List<TableSchemaHistoryObject> tableSchemaHistoryObjects = new ArrayList<>(tableSchemaHistoryRecords.size());
            tableSchemaHistoryRecords.forEach(tableSchemaHistoryRecord -> {
                SchemaInfo schemaInfo = null;
                try {
                    schemaInfo = SchemaInfo.parseFrom(tableSchemaHistoryRecord.getSchema());
                } catch (InvalidProtocolBufferException e) {
                    log.info(e.getMessage());
                    throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
                }
                TableSchemaObject tableSchemaObject = new TableSchemaObject(schemaInfo);
                tableSchemaHistoryObjects.add(new TableSchemaHistoryObject(tableSchemaHistoryRecord.getSchemaHisId(),
                    tableSchemaHistoryRecord.getVersion(), tableSchemaObject));
            });
            if (tableSchemaHistoryRecords.size() < maxNum) {
                return new ScanRecordCursorResult<>(tableSchemaHistoryObjects, null);
            } else {
                return new ScanRecordCursorResult<>(tableSchemaHistoryObjects, CodecUtil.longToBytes(offset + maxNum));
            }
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public List<TableSchemaHistoryObject> listSchemaHistoryFromTimePoint(TransactionContext context, String fromTime,
        TableIdent tableIdent) throws MetaStoreException {
        throw new MetaStoreException(ErrorCode.FEATURE_NOT_SUPPORT, "listSchemaHistoryFromTimePoint");
    }


    @Override
    public byte[] deleteTableSchemaHistory(TransactionContext context, TableIdent tableIdent, String startVersion,
        String endVersion, byte[] continuation) throws MetaStoreException {
        throw new MetaStoreException(ErrorCode.FEATURE_NOT_SUPPORT, "deleteTableSchemaHistory");
    }

    /**
     *  table storage history subspace
     */

    @Override
    public void createTableStorageHistorySubspace(TransactionContext context, String projectId)
        throws MetaStoreException {
        try {
            tableMetaMapper.createTableStorageHistorySubspace(projectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void dropTableStorageHistorySubspace(TransactionContext context, String projectId)
        throws MetaStoreException {
        try {
            tableMetaMapper.dropTableStorageHistorySubspace(projectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void insertTableStorageHistory(TransactionContext context, TableIdent tableIdent,
        String version, TableStorageObject tableStorageObject) throws MetaStoreException {
        try {
            byte[] storage = TableStoreConvertor.getTableStorageInfo(tableStorageObject).toByteArray();
            tableMetaMapper.insertTableStorageHistory(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getTableId(), UuidUtil.generateUUID32(), version, storage);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Optional<TableStorageHistoryObject> getLatestTableStorage(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        try {
            if (!tableMetaMapper.tableStorageHistorySubspaceExist(tableIdent.getProjectId(),
                tableIdent.getCatalogId(), tableIdent.getTableId())) {
                return Optional.empty();
            }

            TableStorageHistoryRecord tableStorageHistoryRecord = tableMetaMapper.getLatestTableStorageHistory(
                tableIdent.getProjectId(), tableIdent.getCatalogId(), tableIdent.getTableId(), basedVersion);
            if (tableStorageHistoryRecord == null) {
                return Optional.empty();
            }
            StorageInfo storageInfo = StorageInfo.parseFrom(tableStorageHistoryRecord.getStorage());
            TableStorageObject tableStorageObject = new TableStorageObject(storageInfo);
            TableStorageHistoryObject tableStorageHistoryObject = new TableStorageHistoryObject(
                tableStorageHistoryRecord.getStorageHisId(), tableStorageHistoryRecord.getVersion(), tableStorageObject);
            return Optional.of(tableStorageHistoryObject);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public TableStorageHistoryObject getLatestTableStorageOrElseThrow(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        Optional<TableStorageHistoryObject> tableStorageHistoryObjectOptional = getLatestTableStorage(context,
            tableIdent, basedVersion);
        if (!tableStorageHistoryObjectOptional.isPresent()) {
            throw new MetaStoreException(ErrorCode.TABLE_STORAGE_NOT_FOUND, tableIdent.getTableId());
        }
        return tableStorageHistoryObjectOptional.get();
    }

    @Override
    public ScanRecordCursorResult<List<TableStorageHistoryObject>> listTableStorageHistory(TransactionContext context,
        TableIdent tableIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel,
        String baseVersion) throws MetaStoreException {
        try {
            long offset = 0;
            if (continuation != null) {
                offset = CodecUtil.bytesToLong(continuation);
            }
            List<TableStorageHistoryRecord> tableStorageHistoryRecords = tableMetaMapper.listTableStorageHistory(
                tableIdent.getProjectId(), tableIdent.getCatalogId(), tableIdent.getTableId(),
                baseVersion, offset, maxNum);
            List<TableStorageHistoryObject> tableStorageHistoryObjects = new ArrayList<>(tableStorageHistoryRecords.size());
            tableStorageHistoryRecords.forEach(tableStorageHistoryRecord -> {
                StorageInfo storageInfo = null;
                try {
                    storageInfo = StorageInfo.parseFrom(tableStorageHistoryRecord.getStorage());
                } catch (InvalidProtocolBufferException e) {
                    log.info(e.getMessage());
                    throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
                }
                tableStorageHistoryObjects.add(new TableStorageHistoryObject(tableStorageHistoryRecord.getStorageHisId(),
                    tableStorageHistoryRecord.getVersion(), new TableStorageObject(storageInfo)));
            });
            if (tableStorageHistoryRecords.size() < maxNum) {
                return new ScanRecordCursorResult<>(tableStorageHistoryObjects, null);
            } else {
                return new ScanRecordCursorResult<>(tableStorageHistoryObjects, CodecUtil.longToBytes(offset + maxNum));
            }
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public byte[] deleteTableStorageHistory(TransactionContext context, TableIdent tableIdent, String startVersion,
        String endVersion, byte[] continuation) {
        throw new MetaStoreException(ErrorCode.FEATURE_NOT_SUPPORT, "deleteTableStorageHistory");
    }

    /**
     * table commit subspace
     */

    @Override
    public void createTableCommitSubspace(TransactionContext context, String projectId) throws MetaStoreException {
        try {
            tableMetaMapper.createTableCommitSubspace(projectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void dropTableCommitSubspace(TransactionContext context, String projectId) throws MetaStoreException {
        try {
            tableMetaMapper.dropTableCommitSubspace(projectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    private Operation trans2Operation(OperationObject operationObject) {
        Operation operation = Operation.newBuilder()
            .setOperation(operationObject.getOperationType().name())
            .setAddedNums(operationObject.getAddedNums())
            .setDeletedNums(operationObject.getDeletedNums())
            .setUpdatedNums(operationObject.getUpdatedNums())
            .setFileCount(operationObject.getFileCount())
            .build();
        return operation;
    }

    @Override
    public void insertTableCommit(TransactionContext context, TableIdent tableIdent, TableCommitObject tableCommit)
            throws MetaStoreException {
        try {
            List<Operation> operations = new ArrayList<>(tableCommit.getOperations().size());
            tableCommit.getOperations().forEach(operationObject -> {
                operations.add(trans2Operation(operationObject));
            });
            OperationList operationList = OperationList.newBuilder().addAllOperation(operations).build();
            TableCommitRecord tableCommitRecord = new TableCommitRecord(tableCommit.getCommitId(), tableCommit.getVersion(), tableIdent.getCatalogId(),
                    tableIdent.getTableId(),
                    tableCommit.getCreateTime(), operationList.toByteArray(), tableCommit.getDroppedTime(),
                    tableCommit.getTableName(), tableCommit.getCreateTime());
            tableMetaMapper.insertTableCommit(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                    tableIdent.getTableId(), tableCommitRecord);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    private TableCommitObject trans2TableCommitObject(TableIdent tableIdent, TableCommitRecord tableCommitRecord) {
        OperationList operationList = null;
        try {
            operationList = OperationList.parseFrom(tableCommitRecord.getOperations());
        } catch (InvalidProtocolBufferException e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
        List<OperationObject> operationObjectList = new ArrayList<>(operationList.getOperationList().size());
        operationList.getOperationList().forEach(operation -> {
            operationObjectList.add(new OperationObject(TableOperationType.valueOf(operation.getOperation()),
                operation.getAddedNums(), operation.getDeletedNums(), operation.getUpdatedNums(),
                operation.getFileCount()));
        });
        TableCommitObject tableCommitObject = new TableCommitObject(tableIdent.getProjectId(),
            tableIdent.getCatalogId(), tableIdent.getDatabaseId(), tableIdent.getTableId(),
            tableCommitRecord.getTableName(), tableCommitRecord.getCreateTime(), tableCommitRecord.getCommitTime(),
            operationObjectList, tableCommitRecord.getDroppedTime(), tableCommitRecord.getVersion());
        return tableCommitObject;
    }

    @Override
    public Optional<TableCommitObject> getTableCommit(TransactionContext context, TableIdent tableIdent,
        String version) throws MetaStoreException {
        try {
            if (!tableMetaMapper.tableCommitSubspaceExist(tableIdent.getProjectId(),
                tableIdent.getCatalogId(), tableIdent.getTableId())) {
                return Optional.empty();
            }

            TableCommitRecord tableCommitRecord = tableMetaMapper.getTableCommit(tableIdent.getProjectId(),
                tableIdent.getCatalogId(), tableIdent.getTableId(), version);
            if (tableCommitRecord == null) {
                return Optional.empty();
            }

            TableCommitObject tableCommitObject = trans2TableCommitObject(tableIdent, tableCommitRecord);
            return Optional.of(tableCommitObject);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Optional<TableCommitObject> getLatestTableCommit(TransactionContext context, TableIdent tableIdent,
        String basedVersion) throws MetaStoreException {
        try {
            if (!tableMetaMapper.tableCommitSubspaceExist(tableIdent.getProjectId(),
                tableIdent.getCatalogId(), tableIdent.getTableId())) {
                return Optional.empty();
            }
            TableCommitRecord tableCommitRecord = tableMetaMapper.getLatestTableCommit(tableIdent.getProjectId(),
                tableIdent.getCatalogId(), tableIdent.getTableId(), basedVersion);
            if (tableCommitRecord == null) {
                return Optional.empty();
            }
            TableCommitObject tableCommitObject = trans2TableCommitObject(tableIdent, tableCommitRecord);
            return Optional.of(tableCommitObject);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public ScanRecordCursorResult<List<TableCommitObject>> listTableCommit(TransactionContext context,
        TableIdent tableIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel,
        String baseVersion) throws MetaStoreException {
        try {
            if (!tableMetaMapper.tableCommitSubspaceExist(tableIdent.getProjectId(),
                tableIdent.getCatalogId(), tableIdent.getTableId())) {
                return new ScanRecordCursorResult<>(new ArrayList<>(), null);
            }

            long offset = 0;
            if (continuation != null) {
                offset = CodecUtil.bytesToLong(continuation);
            }
            List<TableCommitRecord> tableCommitRecords = tableMetaMapper.listTableCommit(
                tableIdent.getProjectId(), tableIdent.getCatalogId(), tableIdent.getTableId(),
                baseVersion, offset, maxNum);
            List<TableCommitObject> tableCommitObjects = new ArrayList<>(tableCommitRecords.size());
            tableCommitRecords.forEach(tableCommitRecord -> {
                TableCommitObject tableCommitObject = trans2TableCommitObject(tableIdent, tableCommitRecord);
                tableCommitObjects.add(tableCommitObject);
            });
            if (tableCommitRecords.size() < maxNum) {
                return new ScanRecordCursorResult<>(tableCommitObjects, null);
            } else {
                return new ScanRecordCursorResult<>(tableCommitObjects, CodecUtil.longToBytes(offset + maxNum));
            }
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public List<TableCommitObject> listTableCommit(TransactionContext context, TableIdent tableIdent,
        String startVersion, String endVersion) throws MetaStoreException {
        try {
            List<TableCommitRecord> tableCommitRecords = tableMetaMapper.traverseTableCommit(
                tableIdent.getProjectId(), tableIdent.getCatalogId(), tableIdent.getTableId(),
                startVersion, endVersion);
            List<TableCommitObject> tableCommitObjects = new ArrayList<>(tableCommitRecords.size());
            tableCommitRecords.forEach(tableCommitRecord -> {
                TableCommitObject tableCommitObject = trans2TableCommitObject(tableIdent, tableCommitRecord);
                tableCommitObjects.add(tableCommitObject);
            });
            return tableCommitObjects;
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public byte[] deleteTableCommit(TransactionContext context, TableIdent tableIdent, String startVersion,
        String endVersion, byte[] continuation) {
        throw new MetaStoreException(ErrorCode.FEATURE_NOT_SUPPORT, "deleteTableCommit");
    }

    /**
     * table dropped object
     */

    @Override
    public void createDroppedTableSubspace(TransactionContext context, CatalogIdent catalogIdent)
        throws MetaStoreException {
        try {
            tableMetaMapper.createDroppedTableSubspace(catalogIdent.getProjectId(), catalogIdent.getCatalogId());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void dropDroppedTableSubspace(TransactionContext context, CatalogIdent catalogIdent)
        throws MetaStoreException {
        try {
            tableMetaMapper.dropDroppedTableSubspace(catalogIdent.getProjectId(), catalogIdent.getCatalogId());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void insertDroppedTable(TransactionContext context, TableIdent tableIdent, TableName tableName,
        long createTime, long droppedTime, boolean isPurge) {
        try {
            tableMetaMapper.insertDroppedTable(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getTableId(), tableName.getTableName(), createTime, droppedTime, isPurge);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public DroppedTableObject getDroppedTable(TransactionContext context, TableIdent tableIdent,
        String tableName) {
        try {
            DroppedTableRecord droppedTableRecord = tableMetaMapper.getDroppedTable(tableIdent.getProjectId(),
                tableIdent.getCatalogId(), tableIdent.getTableId(), tableName);
            return new DroppedTableObject(droppedTableRecord);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public ScanRecordCursorResult<List<DroppedTableObject>> listDroppedTable(TransactionContext context,
        DatabaseIdent databaseIdent, int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel) {
        try {
            long offset = 0;
            if (continuation != null) {
                offset = CodecUtil.bytesToLong(continuation);
            }
            List<DroppedTableRecord> droppedTableRecordList = tableMetaMapper.listDroppedTable(databaseIdent.getProjectId(),
                databaseIdent.getCatalogId(), offset, maxNum);
            List<DroppedTableObject> droppedTableObjectList = new ArrayList<>(droppedTableRecordList.size());
            droppedTableRecordList.forEach(droppedTableRecord -> {
                droppedTableObjectList.add(new DroppedTableObject(droppedTableRecord));
            });
            if (droppedTableObjectList.size() < maxNum) {
                return new ScanRecordCursorResult<>(droppedTableObjectList, null);
            } else {
                return new ScanRecordCursorResult<>(droppedTableObjectList, CodecUtil.longToBytes(offset + maxNum));
            }
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public List<DroppedTableObject> getDroppedTablesByName(TransactionContext context,
        DatabaseIdent databaseIdent, String tableName) {
        try {
            List<DroppedTableRecord> droppedTableRecordList = tableMetaMapper.getDroppedTablesByName(
                databaseIdent.getProjectId(), databaseIdent.getCatalogId(), tableName);
            List<DroppedTableObject> droppedTableObjectList = new ArrayList<>(droppedTableRecordList.size());
            droppedTableRecordList.forEach(droppedTableRecord -> {
                droppedTableObjectList.add(new DroppedTableObject(droppedTableRecord));
            });
            return droppedTableObjectList;
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void deleteDroppedTable(TransactionContext context, TableIdent tableIdent, TableName tableName) {
        try {
            tableMetaMapper.deleteDroppedTable(tableIdent.getProjectId(), tableIdent.getCatalogId(),
                tableIdent.getTableId(), tableName.getTableName());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Map<String, TableCommitObject> getLatestTableCommitByIds(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, List<String> tableIds) {
        try {
            String filterSql = StoreSqlConvertor.get().equals(TableCommitRecord.Fields.catalogId, catalogId).AND()
                    .lessThanOrEquals(TableCommitRecord.Fields.version, branchVersion).AND()
                    .in(TableCommitRecord.Fields.tableId, tableIds).getFilterSql();
            List<TableCommitRecord> records = tableMetaMapper.getLatestTableCommitByFilter(projectId, filterSql);
            Map<String, TableCommitObject> map = new HashMap<>();
            TableCommitObject tableCommitObject;
            for (TableCommitRecord record: records) {
                tableCommitObject = trans2TableCommitObject(new TableIdent(projectId, catalogId, databaseId, record.getTableId()), record);
                map.put(record.getTableId(), tableCommitObject);
            }
            return map;
        } catch (Exception e) {
            log.warn(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Map<String, TableSchemaHistoryObject> getLatestTableSchemaByIds(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, List<String> tableIds) {
        try {
            String filterSql = StoreSqlConvertor.get().equals(TableSchemaHistoryRecord.Fields.catalogId, catalogId).AND()
                    .lessThanOrEquals(TableSchemaHistoryRecord.Fields.version, branchVersion).AND()
                    .in(TableSchemaHistoryRecord.Fields.tableId, tableIds).getFilterSql();
            List<TableSchemaHistoryRecord> records = tableMetaMapper.getLatestTableSchemaByFilter(projectId, filterSql);
            Map<String, TableSchemaHistoryObject> map = new HashMap<>();
            TableSchemaHistoryObject tableSchemaObject;
            SchemaInfo schemaInfo;
            for (TableSchemaHistoryRecord record: records) {
                schemaInfo = SchemaInfo.parseFrom(record.getSchema());
                tableSchemaObject = new TableSchemaHistoryObject(
                        record.getSchemaHisId(), record.getVersion(), new TableSchemaObject(schemaInfo));
                map.put(record.getTableId(), tableSchemaObject);
            }
            return map;
        } catch (Exception e) {
            log.warn(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Map<String, TableStorageHistoryObject> getLatestTableStorageByIds(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, List<String> tableIds) {
        try {
            String filterSql = StoreSqlConvertor.get().equals(TableStorageHistoryRecord.Fields.catalogId, catalogId).AND()
                    .lessThanOrEquals(TableStorageHistoryRecord.Fields.version, branchVersion).AND()
                    .in(TableStorageHistoryRecord.Fields.tableId, tableIds).getFilterSql();
            List<TableStorageHistoryRecord> records = tableMetaMapper.getLatestTableStorageByFilter(projectId, filterSql);
            Map<String, TableStorageHistoryObject> map = new HashMap<>();
            StorageInfo storageInfo;
            TableStorageHistoryObject tableStorageHistoryObject;
            for (TableStorageHistoryRecord record: records) {
                storageInfo = StorageInfo.parseFrom(record.getStorage());
                tableStorageHistoryObject = new TableStorageHistoryObject(
                        record.getStorageHisId(), record.getVersion(), new TableStorageObject(storageInfo));
                map.put(record.getTableId(), tableStorageHistoryObject);
            }
            return map;
        } catch (Exception e) {
            log.warn(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Map<String, TableCommitObject> getLatestTableCommitByTmpTable(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, String tmpTable) {
        try {
            String filterSql = StoreSqlConvertor.get().equals(TableCommitRecord.Fields.catalogId, catalogId).AND()
                    .lessThanOrEquals(TableCommitRecord.Fields.version, branchVersion).getFilterSql();
            List<TableCommitRecord> records = tableMetaMapper.getLatestTableCommitByTmpFilter(projectId, filterSql, tmpTable);
            Map<String, TableCommitObject> map = new HashMap<>();
            TableCommitObject tableCommitObject;
            for (TableCommitRecord record: records) {
                tableCommitObject = trans2TableCommitObject(new TableIdent(projectId, catalogId, databaseId, record.getTableId()), record);
                map.put(record.getTableId(), tableCommitObject);
            }
            return map;
        } catch (Exception e) {
            log.warn(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Map<String, TableSchemaHistoryObject> getLatestTableSchemaByTmpTable(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, String tmpTable) {
        try {
            String filterSql = StoreSqlConvertor.get().equals(TableSchemaHistoryRecord.Fields.catalogId, catalogId).AND()
                    .lessThanOrEquals(TableSchemaHistoryRecord.Fields.version, branchVersion).getFilterSql();
            List<TableSchemaHistoryRecord> records = tableMetaMapper.getLatestTableSchemaByTmpFilter(projectId, filterSql, tmpTable);
            Map<String, TableSchemaHistoryObject> map = new HashMap<>();
            TableSchemaHistoryObject tableSchemaObject;
            SchemaInfo schemaInfo;
            for (TableSchemaHistoryRecord record: records) {
                schemaInfo = SchemaInfo.parseFrom(record.getSchema());
                tableSchemaObject = new TableSchemaHistoryObject(
                        record.getSchemaHisId(), record.getVersion(), new TableSchemaObject(schemaInfo));
                map.put(record.getTableId(), tableSchemaObject);
            }
            return map;
        } catch (Exception e) {
            log.warn(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Map<String, TableStorageHistoryObject> getLatestTableStorageByTmpTable(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, String tmpTable) {
        try {
            String filterSql = StoreSqlConvertor.get().equals(TableStorageHistoryRecord.Fields.catalogId, catalogId).AND()
                    .lessThanOrEquals(TableStorageHistoryRecord.Fields.version, branchVersion).getFilterSql();
            List<TableStorageHistoryRecord> records = tableMetaMapper.getLatestTableStorageByTmpFilter(projectId, filterSql, tmpTable);
            Map<String, TableStorageHistoryObject> map = new HashMap<>();
            StorageInfo storageInfo;
            TableStorageHistoryObject tableStorageHistoryObject;
            for (TableStorageHistoryRecord record: records) {
                storageInfo = StorageInfo.parseFrom(record.getStorage());
                tableStorageHistoryObject = new TableStorageHistoryObject(
                        record.getStorageHisId(), record.getVersion(), new TableStorageObject(storageInfo));
                map.put(record.getTableId(), tableStorageHistoryObject);
            }
            return map;
        } catch (Exception e) {
            log.warn(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Map<String, TableBaseHistoryObject> getLatestTableBaseByTmpTable(TransactionContext context, String projectId, String catalogId, String databaseId, String branchVersion, String tmpTable) {
        try {
            String filterSql = StoreSqlConvertor.get().equals(TableBaseHistoryRecord.Fields.catalogId, catalogId).AND()
                    .lessThanOrEquals(TableBaseHistoryRecord.Fields.version, branchVersion).getFilterSql();
            List<TableBaseHistoryRecord> tableBaseHistoryRecords = tableMetaMapper.getLatestTableBaseHistoryByTmpFilter(projectId, filterSql, tmpTable);
            Map<String, TableBaseHistoryObject> map = new HashMap<>();
            TableBaseInfo tableBaseInfo;
            for (TableBaseHistoryRecord record: tableBaseHistoryRecords) {
                tableBaseInfo = TableBaseInfo.parseFrom(record.getBase());
                map.put(record.getTableId(), new TableBaseHistoryObject(record.getBaseHisId(),
                        record.getVersion(), new TableBaseObject(tableBaseInfo)));
            }
            return map;
        } catch (Exception e) {
            log.warn(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

}
