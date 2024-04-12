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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.polycat.catalog.server.util.TransactionRunner;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.CatalogCommitObject;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.IndexIdent;
import io.polycat.catalog.common.model.IndexInfo;
import io.polycat.catalog.common.model.IndexName;
import io.polycat.catalog.common.model.TableIndexInfo;
import io.polycat.catalog.common.model.TableIndexInfoObject;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TableOperationType;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.IndexInput;
import io.polycat.catalog.common.plugin.request.input.IndexRefreshInput;
import io.polycat.catalog.common.plugin.request.input.TableNameInput;
import io.polycat.catalog.common.utils.CatalogToken;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.service.api.IndexService;
import io.polycat.catalog.store.api.*;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import static io.polycat.catalog.common.Operation.ALTER_MATERIALIZED_VIEW;
import static io.polycat.catalog.common.Operation.CREATE_MATERIALIZED_VIEW;
import static io.polycat.catalog.common.Operation.DROP_MATERIALIZED_VIEW;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class MaterializedViewServiceImpl implements IndexService {

  @Autowired
  private Transaction storeTransaction;
  @Autowired
  private StoreBase storeBase;
  @Autowired
  private IndexStore indexStore;
  @Autowired
  private CatalogStore catalogStore;
  @Autowired
  private RetriableException retriableException;

  private static final String INDEX_STORE_CHECKSUM = "catalogIndexStore";
  private static final int INDEX_STORE_MAX_RETRY_NUM = 256;


  /**
   * create materialized view
   */
  @Override
  public String createIndex(DatabaseName databaseName, IndexInput indexInput) {
    // generate catalog commit id and index Id
    String catalogCommitEventId = UuidUtil.generateId();
    String indexId = UuidUtil.generateId();

    // build indexName object
    IndexName indexName =
        StoreConvertor.indexName(databaseName.getProjectId(), databaseName.getCatalogName(),
            databaseName.getDatabaseName(), indexInput.getName());

    // check if database specified exists and build database ident
    DatabaseIdent databaseIdent;
    try (TransactionContext context = storeTransaction.openTransaction()) {
      databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseName);
      if (null == databaseIdent) {
        throw new CatalogServerException(ErrorCode.DATABASE_NOT_FOUND,
            databaseName.getCatalogName() + "." + databaseName.getDatabaseName());
      }
    }

    IndexIdent indexIdent =
        StoreConvertor.indexIdent(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
            databaseIdent.getDatabaseId(), indexId);

    // start loading mv metadata to underlying db store
    try (TransactionRunner runner = new TransactionRunner()) {
      runner.setMaxAttempts(INDEX_STORE_MAX_RETRY_NUM);
      return runner.run(
          context -> createIndexInternal(context, databaseIdent, indexName, indexIdent, indexInput,
              catalogCommitEventId));
    } catch (RuntimeException e) {
      // Retry interval, the DB may have been deleted, table exist
      // and the previous transaction that created the table may have succeeded
      CatalogIdent catalogIdent =
          StoreConvertor.catalogIdent(indexIdent.getProjectId(), indexIdent.getCatalogId(), indexIdent.getRootCatalogId());
      Optional<CatalogCommitObject> catalogCommit = CatalogCommitHelper
          .getCatalogCommit(catalogIdent, catalogCommitEventId);
      if (catalogCommit.isPresent()) {
        return indexIdent.getIndexId();
      }
      throw e;
    }
  }

  @Override
  public void dropIndex(IndexName indexName, boolean isHMSTable) {
    String catalogCommitEventId = UuidUtil.generateId();

    DatabaseName databaseName =
        StoreConvertor.databaseName(indexName.getProjectId(), indexName.getCatalogName(),
            indexName.getDatabaseName());

    // check if database specified exists and build database ident
    DatabaseIdent databaseIdent;
    try (TransactionContext context = storeTransaction.openTransaction()) {
      databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseName);
      if (databaseIdent == null) {
        throw new MetaStoreException(ErrorCode.DATABASE_NOT_FOUND,
            databaseName.getCatalogName() + "." + databaseName.getDatabaseName());
      }
    }

    try (TransactionRunner runner = new TransactionRunner()) {
      runner.setMaxAttempts(INDEX_STORE_MAX_RETRY_NUM);
      runner.run(context -> {
        dropIndexInternal(context, databaseIdent, indexName, catalogCommitEventId, isHMSTable);
        return null;
      });
    } catch (MetaStoreException e) {
      throw new CatalogServerException(e.getMessage(), e.getErrorCode());
    }
  }

  @Override
  public IndexInfo getIndexByName(IndexName indexName) {
    // check if database specified exists and build database ident
    DatabaseName databaseName =
        StoreConvertor.databaseName(indexName.getProjectId(), indexName.getCatalogName(),
            indexName.getDatabaseName());

    DatabaseIdent databaseIdent;
    try (TransactionContext context = storeTransaction.openTransaction()) {
      databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseName);
      if (databaseIdent == null) {
        throw new MetaStoreException(ErrorCode.DATABASE_NOT_FOUND,
            databaseName.getCatalogName() + "." + databaseName.getDatabaseName());
      }
    }

    // get index information based on index name
    try (TransactionContext context = storeTransaction.openTransaction()) {
      IndexIdent indexIdent =
          indexStore.getIndexIdentByIndexName(context, databaseIdent, indexName);
      if (null == indexIdent) {
        return null;
      }
      // get index schema and index record and build Index
      return indexStore.getIndexRecord(context, indexIdent);
    }
  }

  @Override
  public TraverseCursorResult<List<IndexInfo>> listIndexes(TableName tableName, boolean includeDrop,
      Integer maximumToScan, String pageToken, Boolean isHMSTable, DatabaseName databaseName) {
    int remainResults = maximumToScan;
    if (remainResults <= 0) {
      return new TraverseCursorResult(Collections.emptyList(), null);
    }

    DatabaseIdent databaseIdent;
    String latestVersion;
    try (TransactionContext context = storeTransaction.openTransaction()) {
      databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseName);
      if (databaseIdent == null) {
        throw new MetaStoreException(ErrorCode.DATABASE_NOT_FOUND,
            databaseName.getCatalogName() + "." + databaseName.getDatabaseName());
      }
      latestVersion = VersionManagerHelper.getLatestVersion(context, databaseIdent.getProjectId(),
          databaseIdent.getRootCatalogId());
    }

    String method = Thread.currentThread().getStackTrace()[1].getMethodName();
    Optional<CatalogToken> catalogToken = CatalogToken.parseToken(pageToken, INDEX_STORE_CHECKSUM);
    if (!catalogToken.isPresent()) {

      String version = CodecUtil.bytes2Hex(latestVersion.getBytes());
      catalogToken = Optional.of(
          CatalogToken.buildCatalogToken(INDEX_STORE_CHECKSUM, method, String.valueOf(false),
              version));
    }

    List<IndexInfo> indexList = new ArrayList<>();
    TraverseCursorResult<List<IndexInfo>> intermediateResult;

    CatalogToken nextCatalogToken = catalogToken.get();
    boolean dropped = nextCatalogToken.getContextMapValue(method) != null && Boolean.parseBoolean(
        nextCatalogToken.getContextMapValue(method));
    String readVersion = nextCatalogToken.getReadVersion();
    while (true) {
      intermediateResult = listIndexesWithToken(databaseIdent, databaseName, tableName, remainResults,
          nextCatalogToken, dropped, isHMSTable);
      indexList.addAll(intermediateResult.getResult());
      break;

      // TODO: Handle this

      //      nextCatalogToken = new CatalogToken(INDEX_STORE_CHECKSUM, readVersion);
      //      indexList.addAll(intermediateResult.getResult());
      //      remainResults = remainResults - intermediateResult.getResult().size();
      //      if (remainResults == 0) {
      //        break;
      //      }
      //
      //      if ((dropped == includeDrop) && (!intermediateResult.getContinuation().isPresent())) {
      //        break;
      //      }

      //      dropped = true;
    }
    //    String stepValue = String.valueOf(dropped);
    //    CatalogToken catalogTokenNew = intermediateResult.getContinuation().map(token -> {
    //      token.putContextMap(method, stepValue);
    //      return token;
    //    }).orElse(null);

    TraverseCursorResult<List<IndexInfo>> indexRecordList =
        new TraverseCursorResult(indexList, catalogToken.get());
    if (indexRecordList.getResult().isEmpty()) {
      return new TraverseCursorResult(Collections.emptyList(), null);
    }

    return new TraverseCursorResult(indexRecordList.getResult(),
        indexRecordList.getContinuation().orElse(null));
  }

  @Override
  public void alterIndexByName(IndexName indexName, IndexRefreshInput indexRefreshInput) {
    String catalogCommitEventId = UuidUtil.generateCatalogCommitId();
    DatabaseName databaseName =
        StoreConvertor.databaseName(indexName.getProjectId(), indexName.getCatalogName(),
            indexName.getDatabaseName());

    // check if database specified exists and get database ident
    DatabaseIdent databaseIdent;
    try (TransactionContext context = storeTransaction.openTransaction()) {
      databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseName);
      if (null == databaseIdent) {
        throw new CatalogServerException(ErrorCode.DATABASE_NOT_FOUND,
            databaseName.getCatalogName() + "." + databaseName.getDatabaseName());
      }
    }

    try (TransactionRunner runner = new TransactionRunner()) {
      runner.setMaxAttempts(INDEX_STORE_MAX_RETRY_NUM);
      runner.run ( context-> {
        alterIndexInternal(context, databaseIdent, indexName, indexRefreshInput, catalogCommitEventId);
        return null;
      });
    }
  }

  private String createIndexInternal(TransactionContext context, DatabaseIdent databaseIdent,
      IndexName indexName, IndexIdent indexIdent, IndexInput indexInput,
      String catalogCommitEventId) throws MetaStoreException {
    // insert index into object namespace
    indexStore.insertIndexObjectName(context, indexIdent, indexName, databaseIdent);

    // insert index record
    indexStore.insertIndexRecord(context, indexIdent, indexName, indexInput);

    // insert index schema
    indexStore.insertIndexSchema(context, indexIdent, indexInput);

    // insert index info to parent table
    if(!Boolean.parseBoolean(indexInput.getIsHMSTable())) {
    insertIndexInfoToParentTables(context, indexName, indexIdent, indexInput.getFactTables());
    }

    // insert catalog commit
    String detail = "project id: " + indexName.getProjectId() + ", " + "catalog name: "
        + indexName.getCatalogName() + ", " + "database name: " + indexName.getDatabaseName() + ", "
        + "index name: " + indexName.getIndexName();
    long commitTime = RecordStoreHelper.getCurrentTime();
    catalogStore.insertCatalogCommit(context, indexName.getProjectId(),
        indexIdent.getCatalogId(), catalogCommitEventId, commitTime, CREATE_MATERIALIZED_VIEW, detail
    );
    return indexIdent.getIndexId();
  }

  private void dropIndexInternal(TransactionContext context, DatabaseIdent databaseIdent,
      IndexName indexName, String catalogCommitEventId, boolean isHMSTable) {
    // get index ident and index record
    IndexIdent indexIdent = indexStore.getIndexIdentByIndexName(context, databaseIdent, indexName);
    if (indexIdent == null) {
      throw new MetaStoreException(ErrorCode.MV_NOT_FOUND, indexName.getIndexName());
    }
    List<TableName> parentTableSources =
        indexStore.getIndexRecord(context, indexIdent).getTableNames();

    // delete index reference
    indexStore.deleteIndexReference(context, indexIdent);

    // delete index schema
    indexStore.deleteIndexSchema(context, indexIdent);

    // drop index from ObjectName Subspace
    indexStore.deleteIndexObjectName(context, databaseIdent, indexName, indexIdent);

    // drop index from parent tables index Subspace
    if (!isHMSTable) {
      dropIndexInfoFromParentTables(context, indexName, indexIdent, parentTableSources);
    }

    // insert CatalogCommit subspace
    String msg = new StringBuilder().append("database name: ").append(indexName.getDatabaseName())
        .append(", ").append("index name: ").append(indexName.getIndexName()).toString();
    long commitTime = RecordStoreHelper.getCurrentTime();
    catalogStore.insertCatalogCommit(context, indexIdent.getProjectId(),
        indexIdent.getCatalogId(), catalogCommitEventId, commitTime, DROP_MATERIALIZED_VIEW, msg);
  }

  private TableIndexInfoObject getTableIndexInfo(IndexName indexName, IndexIdent indexIdent) {
    TableIndexInfoObject tableIndexInfo = new TableIndexInfoObject();
    tableIndexInfo.setDatabaseId(indexIdent.getDatabaseId());
    tableIndexInfo.setIndexId(indexIdent.getIndexId());
    tableIndexInfo.setMV(true);
    return tableIndexInfo;
  }

  private void alterIndexInternal(TransactionContext context, DatabaseIdent databaseIdent,
      IndexName indexName, IndexRefreshInput indexRefreshInput, String catalogCommitEventId)
      throws MetaStoreException {
    IndexIdent indexIdent = indexStore.getIndexIdentByIndexName(context, databaseIdent, indexName);
    if (null == indexIdent) {
      throw new MetaStoreException(ErrorCode.MV_NOT_FOUND);
    }
    // build new Index record with updating last modified time of parent tables and update index record
    indexStore.updateIndexRecord(context, databaseIdent, indexIdent, indexName,  indexRefreshInput);

    // insert catalog commit
    String detail = "project id: " + indexName.getProjectId() + ", " + "catalog name: "
        + indexName.getCatalogName() + ", " + "database name: " + indexName.getDatabaseName() + ", "
        + "index name: " + indexName.getIndexName();
    long commitTime = RecordStoreHelper.getCurrentTime();
    catalogStore.insertCatalogCommit(context, indexName.getProjectId(),
        indexIdent.getCatalogId(), catalogCommitEventId, commitTime, ALTER_MATERIALIZED_VIEW, detail
    );
  }

  private void insertIndexInfoToParentTables(TransactionContext context, IndexName indexName,
      IndexIdent indexIdent, List<TableNameInput> tableNameInputList) {
    TableIndexInfoObject tableIndexInfo = getTableIndexInfo(indexName, indexIdent);
    TableOperationType operationType = TableOperationType.DDL_CREATE_INDEX;
    // insert the index information to its corresponding base table stores
    for (TableNameInput tableNameInput : tableNameInputList) {
      TableName tableName =
          StoreConvertor.tableName(tableNameInput.getProjectId(), tableNameInput.getCatalogName(),
              tableNameInput.getDatabaseName(), tableNameInput.getTableName());
      TableIndexHelper.addOrModifyIndexInfo(context, tableName, tableIndexInfo, operationType,
          indexName.getIndexName());
    }
  }

  private void dropIndexInfoFromParentTables(TransactionContext context, IndexName indexName,
      IndexIdent indexIdent, List<TableName> parentTableNameList) {
    TableIndexInfoObject tableIndexInfo = getTableIndexInfo(indexName, indexIdent);
    TableOperationType operationType = TableOperationType.DDL_DROP_INDEX;
    // delete the related index info from its base table stores.
    for (TableName tableName : parentTableNameList) {
      TableIndexHelper.addOrModifyIndexInfo(context, tableName, tableIndexInfo, operationType,
          indexName.getIndexName());
    }
  }

  private TraverseCursorResult<List<IndexInfo>> listIndexesWithToken(DatabaseIdent databaseIdent,
      DatabaseName databaseName, TableName tableName, int maxResultNum, CatalogToken catalogToken,
      Boolean dropped, boolean isHMSTable) {
        List<IndexInfo> indexList = new ArrayList<>();
    if (isHMSTable || tableName.getTableName().isEmpty()) {
      return getAllIndexes(databaseIdent, tableName);
    }
    // Get the index info from the base table
    try (TransactionContext context = storeTransaction.openTransaction()) {
      List<TableIndexInfo> tableIndexInfoList = TableIndexHelper.getTableIndexesByName(context, tableName);
      for (TableIndexInfo tableIndexInfo : tableIndexInfoList) {
        IndexIdent indexIdent =
            StoreConvertor.indexIdent(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
                tableIndexInfo.getDatabaseId(), tableIndexInfo.getIndexId());
        indexList.add(indexStore.getIndexRecord(context, indexIdent));
      }
    }
    return new TraverseCursorResult<>(indexList, catalogToken);
  }

  public TraverseCursorResult<List<IndexInfo>> getAllIndexes(DatabaseIdent databaseIdent,
      TableName tableName) throws MetaStoreException {
    List<IndexInfo> indexList = new ArrayList<>();
    try (TransactionContext context = storeTransaction.openTransaction()) {
      Map<String, String> indexIdMap =
          indexStore.getIndexToObjectIdMap(context, databaseIdent, false);

      // get index information for the index Id's
      for (String indexId : indexIdMap.keySet()) {
        IndexIdent indexIdent =
            StoreConvertor.indexIdent(databaseIdent.getProjectId(), databaseIdent.getCatalogId(),
                databaseIdent.getDatabaseId(), indexId);

        IndexInfo indexInfo = indexStore.getIndexRecord(context, indexIdent);

        if (null != indexInfo) {
          if (!tableName.getTableName().isEmpty()) {
            boolean tableMatched = indexInfo.getTableNames().stream()
                .anyMatch(parentTab -> isSameTable(tableName, parentTab));
            if (tableMatched) {
              indexList.add(indexInfo);
            }
          } else {
            indexList.add(indexInfo);
          }
        }
      }
    }
    return new TraverseCursorResult(indexList, null);
  }

  private boolean isSameTable(TableName tableName, TableName parentTab) {
    return parentTab.getProjectId().equalsIgnoreCase(tableName.getProjectId())
        && parentTab.getCatalogName().equalsIgnoreCase(tableName.getCatalogName())
        && parentTab.getDatabaseName().equalsIgnoreCase(tableName.getDatabaseName())
        && parentTab.getTableName().equalsIgnoreCase(tableName.getTableName());
  }

}
