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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import io.polycat.catalog.common.model.CatalogCommitObject;
import io.polycat.catalog.common.model.CatalogId;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TransactionIsolationLevel;
import io.polycat.catalog.store.api.CatalogStore;
import io.polycat.catalog.store.common.CatalogStoreConvertor;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.gaussdb.pojo.BranchRecord;
import io.polycat.catalog.store.gaussdb.pojo.CatalogCommitRecord;
import io.polycat.catalog.store.gaussdb.pojo.CatalogHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.CatalogRecord;
import io.polycat.catalog.store.mapper.CatalogMapper;
import io.polycat.catalog.util.CheckUtil;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.CatalogHistoryObject;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.mapper.SequenceMapper;
import io.polycat.catalog.store.protos.common.CatalogInfo;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class CatalogStoreImpl implements CatalogStore {

    @Autowired
    private CatalogMapper catalogMapper;

    @Autowired
    private SequenceMapper sequenceMapper;

    @Override
    public void createCatalogSubspace(TransactionContext context, String projectId) throws MetaStoreException {
        try {
            catalogMapper.createCatalogSubspace(projectId);
            sequenceMapper.createSequenceSubspaceCache(projectId, SequenceHelper.getObjectIdSequenceName(projectId),
                SequenceHelper.cache);
        } catch (Exception e) {
            log.warn(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void dropCatalogSubspace(TransactionContext context, String projectId) throws MetaStoreException {
        try {
            sequenceMapper.dropSequenceSubspace(projectId, SequenceHelper.getObjectIdSequenceName(projectId));
            catalogMapper.dropCatalogSubspace(projectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public String generateCatalogId(TransactionContext context, String projectId) throws MetaStoreException {
        return SequenceHelper.generateObjectId(context, projectId);
    }

    @Override
    public CatalogId getCatalogId(TransactionContext context, String projectId, String catalogName)
        throws MetaStoreException {
        try {
            return catalogMapper.getCatalogId(projectId, catalogName);
        } catch (Exception e) {
            log.warn(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void insertCatalog(TransactionContext context, CatalogIdent catalogIdent, CatalogObject catalogObject)
        throws MetaStoreException {
        try {
            byte[] bytes = CatalogStoreConvertor.getCatalogInfo(catalogObject).toByteArray();
            catalogMapper.insertCatalog(catalogIdent.getProjectId(), catalogIdent.getCatalogId(),
                catalogObject.getName(), catalogObject.getRootCatalogId(), bytes);
        } catch (Exception e) {
            e.printStackTrace();
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void updateCatalog(TransactionContext context, CatalogObject catalogObject,
        CatalogObject newCatalogObject) {
        try {
            byte[] bytes = CatalogStoreConvertor.getCatalogInfo(newCatalogObject).toByteArray();
            catalogMapper.updateCatalog(catalogObject.getProjectId(), catalogObject.getCatalogId(),
                newCatalogObject.getName(), bytes);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void deleteCatalog(TransactionContext context, CatalogName catalogName, CatalogIdent catalogIdent) {
        try {
            catalogMapper.deleteCatalog(catalogIdent.getProjectId(), catalogIdent.getCatalogId());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public ScanRecordCursorResult<List<CatalogObject>> listCatalog(TransactionContext context, String projectId,
        int maxNum, byte[] continuation, TransactionIsolationLevel isolationLevel) {
        try {
            long offset = 0;
            if (continuation != null) {
                offset = CodecUtil.bytesToLong(continuation);
            }
            List<CatalogRecord> catalogRecordList = catalogMapper.listCatalog(projectId, offset, maxNum);
            List<CatalogObject> catalogObjectList = new ArrayList<>(catalogRecordList.size());
            catalogRecordList.forEach(catalogRecord -> {
                CatalogInfo catalogInfo = null;
                try {
                    catalogInfo = CatalogInfo.parseFrom(catalogRecord.getCatalogInfo());
                } catch (InvalidProtocolBufferException e) {
                    //e.printStackTrace();
                    log.info(e.getMessage());
                    throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
                }
                CatalogObject catalogObject = new CatalogObject(projectId, catalogRecord.getCatalogId(),
                    catalogRecord.getCatalogName(), catalogRecord.getRootCatalogId(), catalogInfo);
                catalogObjectList.add(catalogObject);
            });
            if (catalogRecordList.size() < maxNum) {
                return new ScanRecordCursorResult<>(catalogObjectList, null);
            } else {
                return new ScanRecordCursorResult<>(catalogObjectList, CodecUtil.longToBytes(offset + maxNum));
            }
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public CatalogObject getCatalogByName(TransactionContext context, CatalogName catalogName)
        throws MetaStoreException {
        CatalogRecord catalogRecord = catalogMapper.getCatalogByName(catalogName.getProjectId(),
                catalogName.getCatalogName());
        CheckUtil.assertNotNull(catalogRecord, ErrorCode.CATALOG_NOT_FOUND, catalogName.getCatalogName());
        try {
            CatalogInfo catalogInfo = CatalogInfo.parseFrom(catalogRecord.getCatalogInfo());
            return new CatalogObject(catalogName.getProjectId(), catalogRecord.getCatalogId(),
                    catalogRecord.getCatalogName(), catalogRecord.getRootCatalogId(), catalogInfo);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }

    }

    @Override
    public CatalogObject getCatalogById(TransactionContext context, CatalogIdent catalogIdent) {
        try {
            CatalogRecord catalogRecord = catalogMapper.getCatalogById(catalogIdent.getProjectId(),
                catalogIdent.getCatalogId());
            CatalogInfo catalogInfo = CatalogInfo.parseFrom(catalogRecord.getCatalogInfo());
            return new CatalogObject(catalogIdent.getProjectId(), catalogRecord.getCatalogId(),
                catalogRecord.getCatalogName(), catalogRecord.getRootCatalogId(), catalogInfo);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void createCatalogHistorySubspace(TransactionContext context, String projectId) throws MetaStoreException {
        try {
            catalogMapper.createCatalogHistorySubspace(projectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void dropCatalogHistorySubspace(TransactionContext context, String projectId)
        throws MetaStoreException {
        try {
            catalogMapper.dropCatalogHistorySubspace(projectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void deleteCatalogHistory(TransactionContext context, CatalogIdent catalogIdent) throws MetaStoreException {
        catalogMapper.deleteCatalogHistory(catalogIdent.getProjectId(), catalogIdent.getCatalogId());
    }

    @Override
    public void insertCatalogHistory(TransactionContext context, CatalogIdent catalogIdent,
        CatalogObject catalogObject, String version) throws MetaStoreException {
        try {
            byte[] bytes = CatalogStoreConvertor.getCatalogInfo(catalogObject).toByteArray();
            catalogMapper.insertCatalogHistory(catalogIdent.getProjectId(), catalogIdent.getCatalogId(),
                UuidUtil.generateUUID32(), version, catalogObject.getName(), catalogObject.getRootCatalogId(),
                bytes);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }



    @Override
    public CatalogHistoryObject getLatestCatalogHistory(TransactionContext context, CatalogIdent catalogIdent,
        String latestVersion) throws MetaStoreException {
        try {
            CatalogHistoryRecord catalogHistoryRecord = catalogMapper.getLatestCatalogHistory(catalogIdent.getProjectId(),
                catalogIdent.getCatalogId());
            CatalogInfo catalogInfo = CatalogInfo.parseFrom(catalogHistoryRecord.getCatalogInfo());
            return CatalogStoreConvertor.trans2CatalogHistoryObject(catalogIdent, catalogHistoryRecord.getCcId(),
                catalogHistoryRecord.getVersion(), catalogHistoryRecord.getCatalogName(),
                catalogHistoryRecord.getRootCatalogId(), catalogInfo);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public CatalogHistoryObject getCatalogHistoryByVersion(TransactionContext context, CatalogIdent catalogIdent,
        String version) throws MetaStoreException {
        try {
            CatalogHistoryRecord catalogHistoryRecord = catalogMapper.getCatalogHistory(catalogIdent.getProjectId(),
                catalogIdent.getCatalogId(), version);
            CatalogInfo catalogInfo = CatalogInfo.parseFrom(catalogHistoryRecord.getCatalogInfo());
            return CatalogStoreConvertor.trans2CatalogHistoryObject(catalogIdent, catalogHistoryRecord.getCcId(),
                catalogHistoryRecord.getVersion(), catalogHistoryRecord.getCatalogName(),
                catalogHistoryRecord.getRootCatalogId(), catalogInfo);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void createCatalogCommitSubspace(TransactionContext context, String projectId)
        throws MetaStoreException {
        try {
            catalogMapper.createCatalogCommitSubspace(projectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void dropCatalogCommitSubspace(TransactionContext context, String projectId)
        throws MetaStoreException {
        try {
            catalogMapper.dropCatalogCommitSubspace(projectId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public void deleteCatalogCommit(TransactionContext context, CatalogIdent catalogIdent) throws MetaStoreException {
        catalogMapper.deleteCatalogCommit(catalogIdent.getProjectId(), catalogIdent.getCatalogId());
    }

    @Override
    public void insertCatalogCommit(TransactionContext context, String projectId, String catalogId, String commitId,
        long commitTime, Operation operation, String detail, String version) throws MetaStoreException {
        try {
            catalogMapper.insertCatalogCommit(projectId, catalogId, commitId, version, commitTime,
                operation.getPrintName(), detail);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    // todo : after all user insertCatalogCommit with version, this interface will delete
    @Override
    public void insertCatalogCommit(TransactionContext context, String projectId, String catalogId, String commitId,
        long commitTime, Operation operation, String detail) throws MetaStoreException {
        insertCatalogCommit(context, projectId, catalogId, commitId, commitTime, operation, detail, "");
    }

    @Override
    public Boolean catalogCommitExist(TransactionContext context, CatalogIdent catalogIdent, String commitId)
        throws MetaStoreException {
        try {
            return catalogMapper.catalogCommitExist(catalogIdent.getProjectId(), catalogIdent.getCatalogId(), commitId);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public Optional<CatalogCommitObject> getCatalogCommit(TransactionContext context, CatalogIdent catalogIdent,
                                                          String commitId) throws MetaStoreException {
        CatalogCommitRecord catalogCommitRecord = catalogMapper.getCatalogCommit(catalogIdent.getProjectId(), catalogIdent.getCatalogId(), commitId);
        if (catalogCommitRecord == null) {
            return Optional.empty();
        }

        return Optional.of(new CatalogCommitObject(catalogIdent, catalogCommitRecord));
    }

    @Override
    public CatalogCommitObject getLatestCatalogCommit(TransactionContext context, CatalogIdent catalogIdent,
        String baseVersion) throws MetaStoreException {
        try {
            CatalogCommitRecord catalogCommitRecord = catalogMapper.getLatestCatalogCommit(catalogIdent.getProjectId(),
                catalogIdent.getCatalogId());
            return new CatalogCommitObject(catalogIdent, catalogCommitRecord);
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }

    @Override
    public ScanRecordCursorResult<List<CatalogCommitObject>> listCatalogCommit(TransactionContext context,
        CatalogIdent catalogIdent, int maxNum, byte[] continuation, byte[] version) {
        try {
            long offset = 0;
            if (continuation != null) {
                offset = CodecUtil.bytesToLong(continuation);
            }
            List<CatalogCommitRecord> catalogCommitRecords = catalogMapper.listCatalogCommit(catalogIdent.getProjectId(),
                catalogIdent.getCatalogId(), offset, maxNum);
            List<CatalogCommitObject> catalogCommitObjects = new ArrayList<>(catalogCommitRecords.size());
            catalogCommitRecords.forEach(catalogCommitRecord -> {
                catalogCommitObjects.add(new CatalogCommitObject(catalogIdent, catalogCommitRecord));
            });

            if (catalogCommitRecords.size() < maxNum) {
                return new ScanRecordCursorResult<>(catalogCommitObjects, null);
            } else {
                return new ScanRecordCursorResult<>(catalogCommitObjects, CodecUtil.longToBytes(offset + maxNum));
            }
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
        }
    }


    @Override
    public boolean hasSubBranchCatalog(TransactionContext context, CatalogIdent parentBranchCatalogIdent) {
        List<BranchRecord> branchRecords = catalogMapper.listBranch(parentBranchCatalogIdent.getProjectId(),
                parentBranchCatalogIdent.getCatalogId(), 1);
        return branchRecords.size() > 0;
    }

    @Override
    public void insertCatalogSubBranch(TransactionContext context, CatalogIdent catalogIdent, String subBranchCatalogId,
                                       String parentVersion) {
        catalogMapper.insertBranch(catalogIdent.getProjectId(), catalogIdent.getCatalogId(), subBranchCatalogId,
                parentVersion, ObjectType.CATALOG.name());
    }

    @Override
    public List<CatalogObject> getParentBranchCatalog(TransactionContext context,
        CatalogIdent subbranchCatalogIdent) {
        List<CatalogObject> catalogList = new ArrayList<>();
        CatalogObject currentCatalog = getCatalogById(context, subbranchCatalogIdent);
        if (currentCatalog == null) {
            return catalogList;
        }

        while (currentCatalog.hasParentId()) {
            CatalogIdent parentCatalogIdent = StoreConvertor.catalogIdent(currentCatalog.getProjectId(),
                currentCatalog.getParentId(), currentCatalog.getRootCatalogId());
            CatalogObject parentCatalog = getCatalogById(context, parentCatalogIdent);
            if (parentCatalog == null) {
                return catalogList;
            }

            catalogList.add(parentCatalog);

            currentCatalog = parentCatalog;
        }

        return catalogList;
    }

    @Override
    public List<CatalogObject> getNextLevelSubBranchCatalogs(TransactionContext context,
                                                             CatalogIdent parentBranchCatalogIdent) {
        List<BranchRecord> branchRecords = catalogMapper.listBranch(parentBranchCatalogIdent.getProjectId(),
                parentBranchCatalogIdent.getCatalogId(), Integer.MAX_VALUE);

        List<CatalogObject> catalogObjects = branchRecords.stream().map(branchRecord -> {
            CatalogRecord catalogRecord = catalogMapper.getCatalogById(parentBranchCatalogIdent.getProjectId(),
                    branchRecord.getBranchCatalogId());
            if (catalogRecord == null) {
                return null;
            }

            CatalogInfo catalogInfo;
            try {
                catalogInfo = CatalogInfo.parseFrom(catalogRecord.getCatalogInfo());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
                log.info(e.getMessage());
                throw new MetaStoreException(ErrorCode.INNER_SERVER_ERROR);
            }

            return new CatalogObject(parentBranchCatalogIdent.getProjectId(), catalogRecord.getCatalogId(),
                    catalogRecord.getCatalogName(), catalogRecord.getRootCatalogId(), catalogInfo);
        }).collect(Collectors.toList());
        return catalogObjects;
    }

    @Override
    public void createBranchSubspace(TransactionContext context, String projectId) {
        catalogMapper.createBranchSubspace(projectId);
    }

    @Override
    public void dropBranchSubspace(TransactionContext context, String projectId) {
        catalogMapper.dropBranchSubspace(projectId);
    }

    @Override
    public void deleteBranch(TransactionContext context, CatalogIdent catalogIdent) {
        catalogMapper.deleteBranch(catalogIdent.getProjectId(), catalogIdent.getCatalogId());
    }
}
