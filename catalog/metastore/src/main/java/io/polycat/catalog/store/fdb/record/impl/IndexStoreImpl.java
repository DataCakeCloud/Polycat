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
package io.polycat.catalog.store.fdb.record.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.IndexIdent;
import io.polycat.catalog.common.model.IndexInfo;
import io.polycat.catalog.common.model.IndexName;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.plugin.request.input.IndexInput;
import io.polycat.catalog.common.plugin.request.input.IndexRefreshInput;
import io.polycat.catalog.common.plugin.request.input.TableNameInput;
import io.polycat.catalog.store.api.IndexStore;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.fdb.record.DirectoryStoreHelper;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.TransactionContextUtil;
import io.polycat.catalog.store.protos.DroppedObjectName;
import io.polycat.catalog.store.protos.IndexObjectName;
import io.polycat.catalog.store.protos.IndexPartition;
import io.polycat.catalog.store.protos.IndexRecord;
import io.polycat.catalog.store.protos.IndexSchema;
import io.polycat.catalog.store.protos.ParentTableName;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;


@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "fdb")
public class IndexStoreImpl implements IndexStore {

  @Override
  public void insertIndexObjectName(TransactionContext context, IndexIdent indexIdent,
      IndexName indexName, DatabaseIdent databaseIdent) {
    FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
    FDBRecordStore indexObjectNameStore =
        DirectoryStoreHelper.getIndexObjectNameStore(fdbRecordContext, databaseIdent);
    IndexObjectName indexObjectName = IndexObjectName.newBuilder()
        .setName(indexName.getIndexName())
        .setObjectId(indexIdent.getIndexId())
        .build();
    indexObjectNameStore.insertRecord(indexObjectName);
  }

  @Override
  public void insertIndexSchema(TransactionContext context, IndexIdent indexIdent,
      IndexInput indexInput) {
    FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
    Map<String, String> properties;
    if(null != indexInput.getProperties()) {
      properties = indexInput.getProperties();
    } else {
      properties = new HashMap<>();
    }
    IndexSchema indexSchema = IndexSchema.newBuilder()
        .setPrimaryKey(0).setIsTimeSeries(false)
        .setQuerySql(indexInput.getQuerySql())
        .putAllProperties(properties)
        .build();
    FDBRecordStore indexSchemaStore = DirectoryStoreHelper.getIndexSchemaStore(fdbRecordContext, indexIdent);
    indexSchemaStore.insertRecord(indexSchema);
  }

  @Override
  public void insertIndexRecord(TransactionContext context, IndexIdent indexIdent,
      IndexName indexName, IndexInput indexInput) {
    FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
    IndexRecord.Builder indexRecord =
        IndexRecord.newBuilder().setPrimaryKey(0).setProjectId(indexIdent.getProjectId())
            .setCatalogId(indexIdent.getCatalogId()).setDatabaseId(indexIdent.getDatabaseId())
            .setIndexId(indexIdent.getIndexId()).setName(indexName.getIndexName())
            .setCatalogName(indexName.getCatalogName())
            .setDatabaseName(indexName.getDatabaseName());
    for (TableNameInput tableNameInput : indexInput.getFactTables()) {
      ParentTableName parentTblName =
          ParentTableName.newBuilder().setProjectId(tableNameInput.getProjectId())
              .setCatalogName(tableNameInput.getCatalogName())
              .setDatabaseName(tableNameInput.getDatabaseName())
              .setTableName(tableNameInput.getTableName()).build();
      indexRecord.addParentTableName(parentTblName);
    }
    indexRecord.setCreatedTime(System.currentTimeMillis());
    FDBRecordStore indexRecordStore = DirectoryStoreHelper.getIndexRecordStore(fdbRecordContext, indexIdent);
    indexRecordStore.insertRecord(indexRecord.build());
  }

  private IndexRecord.Builder getIndexRecord(IndexIdent indexIdent, IndexName indexName) {
    return IndexRecord.newBuilder().setPrimaryKey(0).setProjectId(indexIdent.getProjectId())
        .setCatalogId(indexIdent.getCatalogId()).setDatabaseId(indexIdent.getDatabaseId())
        .setIndexId(indexIdent.getIndexId()).setName(indexName.getIndexName())
        .setCatalogName(indexName.getCatalogName()).setDatabaseName(indexName.getDatabaseName())
        .setCreatedTime(System.currentTimeMillis());
  }

  @Override
  public void updateIndexRecord(TransactionContext context, DatabaseIdent databaseIdent,
      IndexIdent indexIdent, IndexName indexName, IndexRefreshInput indexRefreshInput) {
    IndexRecord.Builder builder = getIndexRecord(indexIdent, indexName);
    List<ParentTableName> parentTableNameList =
        getIndexRecordById(context, indexIdent).getParentTableNameList();
    for (ParentTableName parentTableName : parentTableNameList) {
      ParentTableName parentTableNameNew =
          ParentTableName.newBuilder().setProjectId(parentTableName.getProjectId())
              .setCatalogName(parentTableName.getCatalogName())
              .setDatabaseName(parentTableName.getDatabaseName())
              .setTableName(parentTableName.getTableName()).setLastModifiedTime(
                  (indexRefreshInput.getTableLastMdtMap().get(parentTableName.getTableName())
                      .toString())).build();
      builder.addParentTableName(parentTableNameNew);
    }
    builder.setCreatedTime(System.currentTimeMillis());
    FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
    FDBRecordStore indexRecordStore = DirectoryStoreHelper.getIndexRecordStore(fdbRecordContext, indexIdent);
    indexRecordStore.updateRecord(builder.build());
  }

  @Override
  public void deleteIndexReference(TransactionContext context, IndexIdent indexIdent) {
    FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
    FDBRecordStore indexRecordStore =
        DirectoryStoreHelper.getIndexRecordStore(fdbRecordContext, indexIdent);
    FDBStoredRecord<Message> storedRecord =
        indexRecordStore.loadRecord(getIndexTuple());
    if (storedRecord != null) {
      indexRecordStore.deleteRecord(getIndexTuple());
    }
  }

  @Override
  public void deleteIndexSchema(TransactionContext context, IndexIdent indexIdent) {
    FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
    FDBRecordStore indexSchemaStore = DirectoryStoreHelper.getIndexSchemaStore(fdbRecordContext, indexIdent);
    FDBStoredRecord<Message> storedSchemaRecord =
        indexSchemaStore.loadRecord(getIndexTuple());
    if (storedSchemaRecord != null) {
      indexSchemaStore.deleteRecord(getIndexTuple());
    }
  }

  private Tuple getIndexTuple() {
    return Tuple.from(0);
  }

  private Tuple indexObjectName(String indexName) {
    return Tuple.from(indexName);
  }

  @Override
  public void deleteIndexObjectName(TransactionContext context, DatabaseIdent databaseIdent,
      IndexName indexName, IndexIdent indexIdent) {
    FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
    FDBRecordStore objectNameStore =
        DirectoryStoreHelper.getIndexObjectNameStore(fdbRecordContext, databaseIdent);
    FDBStoredRecord<Message> objectRecord =
        objectNameStore.loadRecord(indexObjectName(indexName.getIndexName()));
    IndexObjectName objectName =
        IndexObjectName.newBuilder().mergeFrom(objectRecord.getRecord()).build();
    objectNameStore.deleteRecord(indexObjectName(objectName.getName()));
    insertIndexToDroppedObjectStore(context, indexIdent, indexName);
  }

  private void insertIndexToDroppedObjectStore(TransactionContext context, IndexIdent indexIdent,
      IndexName indexName) {
    FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
    DatabaseIdent databaseIdent =
        StoreConvertor.databaseIdent(indexIdent);
    DroppedObjectName droppedObjectName = DroppedObjectName.newBuilder()
            .setType(ObjectType.MATERIALIZED_VIEW.name())
            .setParentId(indexIdent.getProjectId())
            .setName(indexName.getIndexName())
            .setObjectId(indexIdent.getIndexId())
            .setDroppedTime(RecordStoreHelper.getCurrentTime())
            .build();
    FDBRecordStore droppedObjectNameStore =
        DirectoryStoreHelper.getDroppedIndexObjectNameStore(fdbRecordContext, databaseIdent);
    droppedObjectNameStore.saveRecord(droppedObjectName);
  }

  @Override
  public Map<String, String> getIndexToObjectIdMap(TransactionContext context,
      DatabaseIdent databaseIdent, boolean includeDropped) {
    FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
    //Obtain indexId include dropped and in-use
    HashMap<String, String> currentIndexes = new HashMap<String, String>();

    TupleRange tupleRange = TupleRange.ALL;

    FDBRecordStore indexStore =
        DirectoryStoreHelper.getIndexObjectNameStore(fdbRecordContext, databaseIdent);
    // get lasted tableId, contains in-use and dropped
    List<FDBStoredRecord<Message>> scannedRecords =
        RecordStoreHelper.listStoreRecords(tupleRange, indexStore, Integer.MAX_VALUE, null,
            IsolationLevel.SERIALIZABLE);

    for (FDBStoredRecord<Message> i : scannedRecords) {
      IndexObjectName indexObjectName =
          IndexObjectName.newBuilder().mergeFrom(i.getRecord()).build();
      currentIndexes.put(indexObjectName.getObjectId(), databaseIdent.getCatalogId());
    }

    if (!includeDropped) {
      return currentIndexes;
    }

    indexStore = DirectoryStoreHelper.getDroppedIndexObjectNameStore(fdbRecordContext, databaseIdent);
    scannedRecords =
        RecordStoreHelper.listStoreRecords(tupleRange, indexStore, Integer.MAX_VALUE, null,
            IsolationLevel.SERIALIZABLE);
    for (FDBStoredRecord<Message> i : scannedRecords) {
      DroppedObjectName droppedObjectName =
          DroppedObjectName.newBuilder().mergeFrom(i.getRecord()).build();

      currentIndexes.put(droppedObjectName.getObjectId(), databaseIdent.getCatalogId());
    }

    return currentIndexes;
  }

  @Override
  public IndexIdent getIndexIdentByIndexName(TransactionContext context, DatabaseIdent databaseIdent,
                                             IndexName indexName) {
    FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
    FDBRecordStore objectNameStore =
        DirectoryStoreHelper.getIndexObjectNameStore(fdbRecordContext, databaseIdent);
    FDBStoredRecord<Message> storedRecord =
        objectNameStore.loadRecord(indexObjectName(indexName.getIndexName()));
    if (storedRecord == null) {
      return null;
    }
    IndexObjectName objectName =
        IndexObjectName.newBuilder().mergeFrom(storedRecord.getRecord()).build();
    return StoreConvertor.indexIdent(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
        databaseIdent.getDatabaseId(), objectName.getObjectId());
  }

  private IndexSchema getCurBranchIndexSchemaById(FDBRecordStore store) {
    FDBStoredRecord<Message> storedRecord = store.loadRecord(getIndexTuple());
    if (storedRecord == null) {
      return null;
    }
    return IndexSchema.newBuilder().mergeFrom(storedRecord.getRecord()).build();
  }

  private IndexRecord getCurBranchIndexRecordById(FDBRecordStore store) {
    FDBStoredRecord<Message> storedRecord = store.loadRecord(getIndexTuple());
    if (storedRecord == null) {
      return null;
    }
    return IndexRecord.newBuilder().mergeFrom(storedRecord.getRecord()).build();
  }

  private IndexPartition getCurBranchIndexPartitionById(FDBRecordStore store) {
    FDBStoredRecord<Message> storedRecord = store.loadRecord(getIndexTuple());
    if (storedRecord == null) {
      return null;
    }
    return IndexPartition.newBuilder().mergeFrom(storedRecord.getRecord()).build();
  }

  public IndexRecord getIndexRecordById(TransactionContext context, IndexIdent indexIdent)
      throws MetaStoreException {
    FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
    FDBRecordStore tableReferenceStore = DirectoryStoreHelper.getIndexRecordStore(fdbRecordContext, indexIdent);
    return getCurBranchIndexRecordById(tableReferenceStore);
  }

  public IndexSchema getIndexSchemaById(TransactionContext context, IndexIdent indexIdent)
      throws MetaStoreException {
    FDBRecordContext fdbRecordContext = TransactionContextUtil.getFDBRecordContext(context);
    FDBRecordStore indexSchemaStore = DirectoryStoreHelper.getIndexSchemaStore(fdbRecordContext, indexIdent);
    return getCurBranchIndexSchemaById(indexSchemaStore);
  }

  @Override
  public IndexInfo getIndexRecord(TransactionContext context, IndexIdent indexIdent) {
    // get table info instance by id
    IndexRecord indexRecord = getIndexRecordById(context, indexIdent);

    // get index schema instance by id
    IndexSchema indexSchema = getIndexSchemaById(context, indexIdent);

    return buildIndexInfo(indexRecord, indexSchema);
  }

  public IndexInfo buildIndexInfo(IndexRecord indexRecord, IndexSchema indexSchema) {
    IndexInfo indexInfo = new IndexInfo();
    indexInfo.setIndexId(indexRecord.getIndexId());
    indexInfo.setQuerySql(indexSchema.getQuerySql());
    indexInfo.setProperties(indexSchema.getPropertiesMap());
    indexInfo.setCreateTime(indexRecord.getCreatedTime());
    indexInfo.setProjectId(indexRecord.getProjectId());
    indexInfo.setCatalogId(indexRecord.getCatalogId());
    indexInfo.setDatabaseId(indexRecord.getDatabaseId());
    indexInfo.setDatabaseName(indexRecord.getDatabaseName());
    indexInfo.setCatalogName(indexRecord.getCatalogName());

    List<TableName> tableNames = new ArrayList<>();
    for (ParentTableName parentTableName : indexRecord.getParentTableNameList()) {
      TableName tableNameObj =
          new TableName(parentTableName.getProjectId(), parentTableName.getCatalogName(),
              parentTableName.getDatabaseName(), parentTableName.getTableName());
      if (!parentTableName.getLastModifiedTime().isEmpty()) {
        tableNameObj.setLastModifiedTime(
            Long.parseLong(parentTableName.getLastModifiedTime()));
      }
      tableNames.add(tableNameObj);
    }
    indexInfo.setIndexName(indexRecord.getName());
    indexInfo.setTableNames(tableNames);
    return indexInfo;
  }
}
