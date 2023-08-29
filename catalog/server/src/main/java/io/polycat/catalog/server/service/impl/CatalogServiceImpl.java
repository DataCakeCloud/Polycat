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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.MergeBranchInput;
import io.polycat.catalog.common.utils.CatalogToken;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.server.util.TransactionFrameRunner;
import io.polycat.catalog.service.api.CatalogService;
import io.polycat.catalog.store.api.CatalogStore;
import io.polycat.catalog.store.api.DatabaseStore;
import io.polycat.catalog.store.api.TableMetaStore;
import io.polycat.catalog.store.api.UserPrivilegeStore;
import io.polycat.catalog.store.api.VersionManager;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.common.StoreValidator;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.util.CheckUtil;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import static io.polycat.catalog.common.Operation.ALTER_CATALOG;
import static io.polycat.catalog.common.Operation.CREATE_CATALOG;
import static io.polycat.catalog.common.Operation.MERGE_BRANCH;
import static io.polycat.catalog.server.service.impl.ObjectNameMapHelper.LMS_KEY;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class CatalogServiceImpl implements CatalogService {
    private static final Logger log = Logger.getLogger(CatalogServiceImpl.class);
    private static final String dateFormat = "yyyy-MM-dd HH:mm:ss";
    private static final SimpleDateFormat SDF = new SimpleDateFormat(dateFormat);

    private static final int CATALOG_STORE_MAX_RETRY_NUM = 256;
    private final String catalogStoreCheckSum = "catalogStore";
    private final int maxBatchRowNum = 1024;

    @Autowired
    private UserPrivilegeStore userPrivilegeStore;

    @Autowired
    private CatalogStore catalogStore;

    @Autowired
    private DatabaseStore databaseStore;

    @Autowired
    private TableMetaStore tableMetaStore;

    @Autowired
    private VersionManager versionManager;

    private String createCommitDetail(CatalogObject catalogObject) {
        return new StringBuilder()
            .append("catalog name: ")
            .append(catalogObject.getName())
            .toString();
    }

    private CatalogObject getCatalogObject(TransactionContext context, String projectId, CatalogInput catalogInput) {
        String catalogId = catalogStore.generateCatalogId(context, projectId);
        CatalogObject catalogObject = new CatalogObject();
        catalogObject.setProjectId(projectId);
        catalogObject.setCatalogId(catalogId);
        catalogObject.setCreateTime(System.currentTimeMillis());
        catalogObject.setUserId(catalogInput.getOwner());
        catalogObject.setName(catalogInput.getCatalogName());
        catalogObject.setRootCatalogId(catalogId);
        if (catalogInput.getDescription() != null) {
            catalogObject.setDescriptions(catalogInput.getDescription());
        }

        if (null != catalogInput.getParentName()) {
            CatalogObject parentObj = CatalogObjectHelper.getCatalogObject(context,
                    StoreConvertor.catalogName(projectId, catalogInput.getParentName()));
            catalogObject.setParentId(parentObj.getCatalogId());
            catalogObject.setParentVersion(catalogInput.getParentVersion());
            catalogObject.setParentType(ObjectType.CATALOG);
            catalogObject.setRootCatalogId(parentObj.getRootCatalogId());
        }
        return catalogObject;
    }

    private CatalogObject createCatalogInternal(TransactionContext context, String projectId, CatalogInput catalogInput,
        String catalogCommitId) {
        if (catalogStore.getCatalogId(context, projectId, catalogInput.getCatalogName()) != null) {
            throw new CatalogServerException(ErrorCode.CATALOG_ALREADY_EXIST, catalogInput.getCatalogName());
        }

        CatalogObject catalogObject = getCatalogObject(context, projectId, catalogInput);

        CatalogIdent catalogIdent = StoreConvertor.catalogIdent(catalogObject.getProjectId(),
            catalogObject.getCatalogId(), catalogObject.getRootCatalogId());

        tableMetaStore.createTableSubspace(context, catalogIdent);

        tableMetaStore.createTableReferenceSubspace(context, catalogIdent);

        tableMetaStore.createDroppedTableSubspace(context, catalogIdent);

        String version;
        if (catalogObject.hasParentId()) {
            if (null == catalogObject.getParentVersion()) {
                catalogObject.setParentVersion(VersionManagerHelper.getLatestVersion(context, catalogIdent.getProjectId(),
                    catalogIdent.getRootCatalogId()));
            }

            CatalogIdent parentCatalogId = StoreConvertor
                .catalogIdent(catalogObject.getProjectId(), catalogObject.getParentId(), catalogObject.getRootCatalogId());
            catalogStore.insertCatalogSubBranch(context, parentCatalogId, catalogIdent.getCatalogId(),
                catalogObject.getParentVersion());
        } else {
            // create version subspace
            versionManager.createVersionSubspace(context, catalogObject.getProjectId(), catalogObject.getCatalogId());
        }

        // insert catalog record subspace
        catalogStore.insertCatalog(context, catalogIdent, catalogObject);

        // insert a catalog history
        version = versionManager.getLatestVersion(context, catalogIdent.getProjectId(), catalogIdent.getRootCatalogId());
        catalogStore.insertCatalogHistory(context, catalogIdent, catalogObject, version);

        // update CatalogCommit
        long commitTime = RecordStoreHelper.getCurrentTime();
        catalogStore.insertCatalogCommit(context, catalogObject.getProjectId(),
            catalogObject.getCatalogId(), catalogCommitId, commitTime, CREATE_CATALOG,
            createCommitDetail(catalogObject), version);

        // insert user privilege table
        userPrivilegeStore.insertUserPrivilege(context, catalogObject.getProjectId(), catalogObject.getUserId(),
            ObjectType.CATALOG.name(), catalogObject.getCatalogId(), true, 0);

        CatalogName catalogName = StoreConvertor.catalogName(projectId, catalogInput.getCatalogName());
        createDefaultDatabase(context, catalogIdent, catalogName,
                appendBackslashForPath(catalogInput.getLocation()) + "default");

        return catalogObject;
    }

    private String appendBackslashForPath(String path) {
        if (StringUtils.isEmpty(path)) {
            return "/";
        }

        return path.charAt(path.length() - 1) == '/' ? path : path + "/";
    }


    private void createDefaultDatabase(TransactionContext context, CatalogIdent catalogIdent, CatalogName catalogName, String location) {
        DatabaseObject databaseObject = new DatabaseObject();
        databaseObject.setName("default");
        databaseObject.setCreateTime(System.currentTimeMillis());
        databaseObject.setLocation(location);
        databaseObject.setUserId("public");
        databaseObject.setOwnerType(PrincipalType.ROLE.name());
        databaseObject.setProperties(Collections.singletonMap(LMS_KEY, catalogName.getCatalogName()));
        String databaseId = databaseStore.generateDatabaseId(context, catalogIdent.getProjectId(),
                catalogIdent.getCatalogId());
        DatabaseIdent databaseIdent = StoreConvertor.databaseIdent(catalogIdent, databaseId);
        DatabaseName databaseName = StoreConvertor.databaseName(catalogName, databaseObject.getName());

        DatabaseObjectHelper.insertDatabaseObject(context, databaseIdent, databaseObject, databaseName, userPrivilegeStore, UuidUtil.generateCatalogCommitId());

    }

    /**
     * 创建catalog
     *
     * @param projectId
     * @param catalogInput
     * @return
     */
    @Override
    public Catalog createCatalog(String projectId, CatalogInput catalogInput) {

        String catalogCommitId = UuidUtil.generateUUID32();

        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(CATALOG_STORE_MAX_RETRY_NUM);
        CatalogObject catalogObject = runner.run(context -> {
            return createCatalogInternal(context, projectId, catalogInput, catalogCommitId);
        }).getResultAndCheck(ret -> {
            CatalogIdent catalogIdent = StoreConvertor.catalogIdent(ret.getProjectId(), ret.getCatalogId(), ret.getRootCatalogId());
            return CatalogCommitHelper.catalogCommitExist(catalogIdent, catalogCommitId);
        });

        return CatalogObjectConvertHelper.toCatalog(catalogObject);
    }

    private void dropCatalogInternal(TransactionContext context, CatalogName catalogName) {
        // check whether the catalog exists.
        CatalogId catalogId = catalogStore.getCatalogId(context, catalogName.getProjectId(), catalogName.getCatalogName());
        if (catalogId == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_NOT_FOUND, catalogName.getCatalogName());
        }

        CatalogIdent catalogIdent = StoreConvertor.catalogIdent(catalogName.getProjectId(), catalogId.getCatalogId(),
            catalogId.getRootCatalogId());

        // delete catalog from Catalog subspace
        catalogStore.deleteCatalog(context, catalogName, catalogIdent);

        // drop catalog history
        catalogStore.deleteCatalogHistory(context, catalogIdent);

        // drop catalog commit
        catalogStore.deleteCatalogCommit(context, catalogIdent);

        // drop catalog version subspace
        versionManager.dropVersionSubspace(context, catalogName.getProjectId(), catalogId.getCatalogId());
    }

    /**
     * drop catalog by name
     *
     * @param catalogName catalogName
     */
    @Override
    public void dropCatalog(CatalogName catalogName) {
        CheckUtil.checkStringParameter(catalogName.getCatalogName());
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(CATALOG_STORE_MAX_RETRY_NUM);
        runner.run(context -> {
            dropCatalogInternal(context, catalogName);
            return null;
        }).getResult();
    }

    /**
     * get catalog by name
     *
     * @param catalogName
     * @return catalog
     */
    @Override
    public Catalog getCatalog(CatalogName catalogName) {
        CheckUtil.checkStringParameter(catalogName.getCatalogName());
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(CATALOG_STORE_MAX_RETRY_NUM);
        CatalogObject catalogObject = runner.run(context -> {
            return CatalogObjectHelper.getCatalogObject(context, catalogName);
        }).getResult();
        if (catalogObject == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_NOT_FOUND, catalogName.getCatalogName());
        }
        return CatalogObjectConvertHelper.toCatalog(catalogObject);
    }

    private CatalogIdent alterCatalogInternal(TransactionContext context, CatalogName catalogName,
        CatalogObject newCatalog, String catalogCommitId) {

        final CatalogName newCatalogName = StoreConvertor.catalogName(newCatalog.getProjectId(), newCatalog.getName());
        CatalogObject catalogToUpdate = null;
        try {
            catalogToUpdate = catalogStore.getCatalogByName(context, newCatalogName);
        } catch (MetaStoreException e) {
            if (!e.getErrorCode().equals(ErrorCode.CATALOG_NOT_FOUND)) {
                throw e;
            }
        }
        if (catalogToUpdate != null && !catalogName.getCatalogName().equals(newCatalogName.getCatalogName())) {
            throw new MetaStoreException(ErrorCode.CATALOG_ALREADY_EXIST, newCatalogName.getCatalogName());
        }

        CatalogObject currentCatalog = catalogStore.getCatalogByName(context, catalogName);
        if (currentCatalog == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_NOT_FOUND, catalogName.getCatalogName());
        }
        String version = VersionManagerHelper.getNextVersion(context, catalogName.getProjectId(), currentCatalog.getRootCatalogId());
        CatalogObject updatedCatalog = new CatalogObject(currentCatalog);
        updatedCatalog.setName(newCatalog.getName());
        final String userId = newCatalog.getUserId();
        if (StringUtils.isNotEmpty(userId)) {
            updatedCatalog.setUserId(newCatalog.getUserId());
        }
        final String descriptions = newCatalog.getDescriptions();
        if (StringUtils.isNotEmpty(descriptions)) {
            updatedCatalog.setDescriptions(newCatalog.getDescriptions());
        }
        final String locationUri = newCatalog.getLocationUri();
        if (StringUtils.isNotEmpty(locationUri)) {
            updatedCatalog.setLocationUri(locationUri);
        }
        CatalogIdent catalogIdent = StoreConvertor.catalogIdent(catalogName.getProjectId(),
            currentCatalog.getCatalogId(), currentCatalog.getRootCatalogId());

        catalogStore.updateCatalog(context, currentCatalog, updatedCatalog);

        // insert a catalog history
        catalogStore.insertCatalogHistory(context, catalogIdent, updatedCatalog, version);

        // update CatalogCommit
        long commitTime = RecordStoreHelper.getCurrentTime();
        catalogStore.insertCatalogCommit(context, updatedCatalog.getProjectId(),
            updatedCatalog.getCatalogId(), catalogCommitId, commitTime, ALTER_CATALOG, createCommitDetail(newCatalog),
            version);

        return catalogIdent;
    }


    private void alterCatalog(CatalogName catalogName, CatalogObject newCatalog)
        throws CatalogServerException {
        StoreValidator.validate(newCatalog);
        String catalogCommitId = UuidUtil.generateUUID32();
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(CATALOG_STORE_MAX_RETRY_NUM);
        CatalogIdent catalogIdent = runner.run(context -> {
            return alterCatalogInternal(context, catalogName, newCatalog, catalogCommitId);
        }).getResultAndCheck(ret -> CatalogCommitHelper.catalogCommitExist(ret, catalogCommitId));
    }

    @Override
    public void alterCatalog(CatalogName catalogName, CatalogInput catalogInput) {
        CatalogObject catalogRecord = new CatalogObject();
        catalogRecord.setProjectId(catalogName.getProjectId());
        catalogRecord.setName(catalogInput.getCatalogName());
        catalogRecord.setUserId(catalogInput.getOwner());
        catalogRecord.setLocationUri(catalogInput.getLocation());
        catalogRecord.setDescriptions(catalogInput.getDescription());
        alterCatalog(catalogName, catalogRecord);
    }

    /**
     * get latest version for catalog
     *
     * @param catalogName
     * @return
     */
    @Override
    public CatalogHistoryObject getLatestCatalogVersion(CatalogName catalogName) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        return runner.run(context -> {
            return getLatestCatalogVersionInner(context, catalogName);
        }).getResult();
    }

    private CatalogHistoryObject getLatestCatalogVersionInner(TransactionContext context, CatalogName catalogName) {
        CatalogIdent catalogIdent = CatalogObjectHelper.getCatalogIdent(context, catalogName);
        CatalogObject catalog = catalogStore.getCatalogById(context, catalogIdent);
        if (catalog == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_ID_NOT_FOUND, catalogIdent.getCatalogId());
        }

        String latestVersion = VersionManagerHelper.getLatestVersion(context, catalogIdent.getProjectId(),
            catalogIdent.getRootCatalogId());
        return catalogStore.getLatestCatalogHistory(context, catalogIdent, latestVersion);
    }

    private CatalogHistoryObject getCatalogByVersion(TransactionContext context, CatalogIdent catalogIdent,
        String version) throws CatalogServerException {
        return catalogStore.getCatalogHistoryByVersion(context, catalogIdent, version);
    }

    /**
     * get catalog for version
     *
     * @param catalogName
     * @param version
     * @return
     */
    @Override
    public CatalogHistoryObject getCatalogByVersion(CatalogName catalogName, String version) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        return runner.run(context -> {
            CatalogIdent catalogIdent = CatalogObjectHelper.getCatalogIdent(context, catalogName);
            return getCatalogByVersion(context, catalogIdent, version);
        }).getResult();
    }


    private CatalogObject trans2CatalogRecordObject(CatalogHistoryObject catalogHistory) {
        CatalogObject catalogRecord = new CatalogObject();
        catalogRecord.setProjectId(catalogHistory.getProjectId());
        catalogRecord.setCatalogId(catalogHistory.getCatalogId());
        catalogRecord.setName(catalogHistory.getName());
        catalogRecord.getProperties().putAll(catalogHistory.getProperties());

        catalogRecord.setParentId(catalogHistory.getParentId());
        catalogRecord.setParentType(catalogHistory.getParentType());
        catalogRecord.setParentVersion(catalogHistory.getParentVersion());

        return catalogRecord;
    }

    private TraverseCursorResult<List<CatalogObject>> listValidCatalogWithToken(TransactionContext context,
        String projectId, int maxResultNum, String pageToken) {
        List<CatalogObject> catalogRecords = new ArrayList<>();
        int remainNum = maxResultNum;
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();

        Optional<CatalogToken> catalogTokenOptional = CatalogToken.parseToken(pageToken, catalogStoreCheckSum);
        if (!catalogTokenOptional.isPresent()) {
            catalogTokenOptional = Optional.of(new CatalogToken(catalogStoreCheckSum, ""));
        }
        CatalogToken catalogToken = catalogTokenOptional.get();

        byte[] continuation = (catalogToken.getContextMapValue(method) == null)
            ? null : CodecUtil.hex2Bytes(catalogToken.getContextMapValue(method));

        while (true) {
            int batchNum = Math.min(remainNum, maxBatchRowNum);
            ScanRecordCursorResult<List<CatalogObject>> listScanRecordCursorResult = catalogStore
                .listCatalog(context, projectId, batchNum, continuation, TransactionIsolationLevel.SNAPSHOT);
            List<CatalogObject> catalogFilterList = listScanRecordCursorResult.getResult().stream()
                .filter(catalogRecord -> !catalogRecord.hasParentId()).collect(Collectors.toList());

            catalogRecords.addAll(catalogFilterList);

            remainNum = remainNum - catalogFilterList.size();
            continuation = listScanRecordCursorResult.getContinuation().orElse(null);
            if ((continuation == null) || (remainNum == 0)) {
                break;
            }
        }

        if (continuation == null) {
            return new TraverseCursorResult(catalogRecords, null);
        }

        CatalogToken catalogTokenNew = CatalogToken.buildCatalogToken(catalogStoreCheckSum, method,
            CodecUtil.bytes2Hex(continuation), catalogToken.getReadVersion());
        return new TraverseCursorResult(catalogRecords, catalogTokenNew);
    }

    private TraverseCursorResult<List<CatalogObject>> listCatalogWithToken(String projectId, int maxResultNum,
        String pageToken) {

        TraverseCursorResult<List<CatalogObject>> result = null;
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(CATALOG_STORE_MAX_RETRY_NUM);
        result = runner.run(context -> {
            return listValidCatalogWithToken(context, projectId, maxResultNum, pageToken);
        }).getResult();

        return result;
    }

    private TraverseCursorResult<List<CatalogObject>> listCatalogs(String projectId,
        Integer maxResults, String pageToken, String pattern) throws CatalogServerException {
        int remainResults = maxResults;
        List<CatalogObject> listSum = new ArrayList<>();
        TraverseCursorResult<List<CatalogObject>> stepResult;
        while (true) {
            stepResult = listCatalogWithToken(projectId, remainResults, pageToken);

            listSum.addAll(stepResult.getResult());
            remainResults = remainResults - stepResult.getResult().size();
            if (remainResults == 0) {
                break;
            }

            if (!stepResult.getContinuation().isPresent()) {
                break;
            }
        }

        return new TraverseCursorResult(listSum, stepResult.getContinuation().orElse(null));
    }

    /**
     * show Catalog
     *
     * @param projectId
     * @param pattern
     * @return
     */
    @Override
    public TraverseCursorResult<List<Catalog>> getCatalogs(String projectId, Integer maxResults,
        String pageToken, String pattern) {
        TraverseCursorResult<List<CatalogObject>> catalogs = listCatalogs(projectId,
            maxResults, pageToken, pattern);
        List<Catalog> catalogModels = new ArrayList<>(catalogs.getResult().size());
        for (CatalogObject catalog : catalogs.getResult()) {
            Catalog catalogModel = CatalogObjectConvertHelper.toCatalog(catalog);
            catalogModels.add(catalogModel);
        }
        return new TraverseCursorResult(catalogModels, catalogs.getContinuation().orElse(null));
    }


    private TraverseCursorResult<List<CatalogCommitObject>> getCatalogCommitsWithToken(TransactionContext context,
        CatalogName catalogName, int maxResultNum, String pageToken) {
        List<CatalogCommitObject> catalogCommitList = new ArrayList<>();
        int remainNum = maxResultNum;

        byte[] continuation = null;
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();

        CatalogIdent catalogIdent = CatalogObjectHelper.getCatalogIdent(context, catalogName);

        Optional<CatalogToken> catalogTokenOptional = CatalogToken.parseToken(pageToken, catalogStoreCheckSum);
        if (!catalogTokenOptional.isPresent()) {
            String latestVersion = VersionManagerHelper.getLatestVersion(context, catalogIdent.getProjectId(),
                catalogIdent.getRootCatalogId());
            catalogTokenOptional = Optional.of(new CatalogToken(catalogStoreCheckSum, latestVersion));
        }
        CatalogToken catalogToken = catalogTokenOptional.get();

        if (catalogToken.getContextMapValue(method) != null) {
            continuation = CodecUtil.hex2Bytes(catalogToken.getContextMapValue(method));
        }
        byte[] readVersion = CodecUtil.hex2Bytes(catalogToken.getReadVersion());


        while (true) {
            int batchNum = Math.min(remainNum, maxBatchRowNum);
            ScanRecordCursorResult<List<CatalogCommitObject>> batchCatalogCommitList = catalogStore
                .listCatalogCommit(context, catalogIdent, batchNum,
                    continuation, readVersion);
            catalogCommitList.addAll(batchCatalogCommitList.getResult());

            remainNum = remainNum - batchCatalogCommitList.getResult().size();
            continuation = batchCatalogCommitList.getContinuation().orElse(null);
            if ((continuation == null) || (remainNum == 0)) {
                break;
            }
        }

        if (continuation == null) {
            return new TraverseCursorResult(catalogCommitList, null);
        }

        CatalogToken catalogTokenNew = CatalogToken.buildCatalogToken(catalogStoreCheckSum, method,
            CodecUtil.bytes2Hex(continuation), catalogToken.getReadVersion());

        return new TraverseCursorResult(catalogCommitList, catalogTokenNew);
    }

    private TraverseCursorResult<List<CatalogCommitObject>> listCatalogCommitWithToken(CatalogName catalogName,
        int maxResults, String pageToken) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(CATALOG_STORE_MAX_RETRY_NUM);
        TraverseCursorResult<List<CatalogCommitObject>> catalogCommits = runner.run(context -> {
            return getCatalogCommitsWithToken(context, catalogName, maxResults, pageToken);
        }).getResult();
        return catalogCommits;
    }

    private TraverseCursorResult<List<CatalogCommitObject>> listCatalogCommits(CatalogName catalogName,
        Integer maxResults, String pageToken) throws CatalogServerException {
        TraverseCursorResult<List<CatalogCommitObject>> traverseCursorResult = listCatalogCommitWithToken(catalogName,
            maxResults, pageToken);

        return traverseCursorResult;
    }

    @Override
    public TraverseCursorResult<List<io.polycat.catalog.common.model.CatalogCommit>> getCatalogCommits(
        CatalogName catalogName, Integer maxResults, String pageToken) {
        TraverseCursorResult<List<CatalogCommitObject>> catalogCommits = listCatalogCommits(catalogName,
            maxResults, pageToken);
        List<io.polycat.catalog.common.model.CatalogCommit> result = new ArrayList<>(catalogCommits.getResult().size());

        for (CatalogCommitObject commit : catalogCommits.getResult()) {
            io.polycat.catalog.common.model.CatalogCommit catalogCommit = new io.polycat.catalog.common.model.CatalogCommit();
            catalogCommit.setProjectId(commit.getProjectId());
            catalogCommit.setCatalogId(commit.getCatalogId());
            catalogCommit.setCommitVersion(commit.getVersion());
            catalogCommit.setOperation(commit.getOperation());
            catalogCommit.setDetail(commit.getDetail());
            Date date = new Date(commit.getCommitTime());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            catalogCommit.setCommitTime(sdf.format(date));
            result.add(catalogCommit);
        }

        return new TraverseCursorResult(result, catalogCommits.getContinuation().orElse(null));
    }

    private List<CatalogObject> getSubBranchCatalogs(TransactionContext context, CatalogIdent parentBranchCatalogIdent) {
        List<CatalogObject> subBranchCatalogList = new ArrayList<>();
        ScanRecordCursorResult<List<CatalogObject>> catalogRecordList = catalogStore
            .listCatalog(context, parentBranchCatalogIdent.getProjectId(),
                Integer.MAX_VALUE, null, TransactionIsolationLevel.SERIALIZABLE);

        for (CatalogObject catalogRecord : catalogRecordList.getResult()) {
            if (catalogRecord.hasParentId()) {
                if (catalogRecord.getParentId().equals(parentBranchCatalogIdent.getCatalogId())) {
                    subBranchCatalogList.add(catalogRecord);
                    CatalogIdent catalogIdent = StoreConvertor.catalogIdent(catalogRecord.getProjectId(),
                        catalogRecord.getCatalogId(), catalogRecord.getRootCatalogId());
                    subBranchCatalogList.addAll(getSubBranchCatalogs(context, catalogIdent));
                }
            }
        }

        return subBranchCatalogList;
    }

    private List<CatalogObject> listSubBranch(TransactionContext context, CatalogName parentBranchCatalogName) {
        // get catalog id by name
        CatalogIdent parentCatalogIdent = CatalogObjectHelper.getCatalogIdent(context, parentBranchCatalogName);
        if (parentCatalogIdent == null) {
            return null;
        }

        return getSubBranchCatalogs(context, parentCatalogIdent);
    }

    @Override
    public List<Catalog> listSubBranchCatalogs(CatalogName catalogName) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(CATALOG_STORE_MAX_RETRY_NUM);
        List<CatalogObject> subBranchCatalogs = runner.run(context -> {
            return listSubBranch(context, catalogName);
        }).getResult();
        if (subBranchCatalogs.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        List<Catalog> catalogModels = new ArrayList<>(subBranchCatalogs.size());
        for (CatalogObject catalog : subBranchCatalogs) {
            Catalog catalogModel = CatalogObjectConvertHelper.toCatalog(catalog);
            catalogModels.add(catalogModel);
        }

        return catalogModels;
    }

    /**
     *
     * Obtaining the Maximum Same Versions of the Source and Target Branches
     * <p>
     * sample: branch2(parent->branch1)                  |--------------V9------------------>
     * branch1(parent->mainBranch)               |---V4-------------V7---------------------->
     * mainBranch                   ---V1-----V2---V3------V5------------------------------->
     * branch3(parent->mainBranch)                    |------V6-----------V10--------------->
     * branch4(parent->branch3)                                  |----V8----V11------------->
     * <p>
     * branch2 and branch4 ,the sameMaxVersion is V2
     * branch4 and branch3, the sameMaxVersion is V6
     * mainBranch and branch3, the samMaxVersion is V3
     */
    private String getSameMaxVersion(TransactionContext context, CatalogIdent srcCatalogIdent,
        CatalogIdent destCatalogIdent) throws CatalogServerException {
        CatalogObject srcCatalogRecord = catalogStore.getCatalogById(context, srcCatalogIdent);
        if (srcCatalogRecord == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_ID_NOT_FOUND, srcCatalogIdent.getCatalogId());
        }

        CatalogObject destCatalogRecord = catalogStore.getCatalogById(context, destCatalogIdent);
        if (destCatalogRecord == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_ID_NOT_FOUND, destCatalogIdent.getCatalogId());
        }

        //record previous version
        String srcLastVersion = VersionManagerHelper.getLatestVersion(context, srcCatalogIdent.getProjectId(),
            srcCatalogIdent.getRootCatalogId());
        String destLastVersion = VersionManagerHelper.getLatestVersion(context, destCatalogIdent.getProjectId(),
            destCatalogIdent.getRootCatalogId());

        while (!srcCatalogRecord.getParentId().isEmpty() || !destCatalogRecord.getParentId().isEmpty()) {
            if (srcCatalogRecord.getCatalogId().equals(destCatalogRecord.getCatalogId())) {
                if (srcLastVersion.compareTo(destLastVersion) > 0) {
                    return destLastVersion;
                } else {
                    return srcLastVersion;
                }
            }

            String srcVersion = srcCatalogRecord.getParentVersion();
            String destVersion = destCatalogRecord.getParentVersion();
            int compareValue = srcVersion.compareTo(destVersion);

            //update previous version
            if (compareValue > 0) {
                srcLastVersion = srcVersion;
                CatalogIdent catalogIdent = StoreConvertor.catalogIdent(srcCatalogRecord.getProjectId(),
                    srcCatalogRecord.getParentId(), srcCatalogRecord.getRootCatalogId());
                srcCatalogRecord = catalogStore.getCatalogById(context, catalogIdent);
                if (srcCatalogRecord == null) {
                    throw new CatalogServerException(ErrorCode.CATALOG_ID_NOT_FOUND, srcCatalogIdent.getCatalogId());
                }
            } else if (compareValue < 0) {
                destLastVersion = destVersion;
                CatalogIdent catalogIdent = StoreConvertor.catalogIdent(destCatalogRecord.getProjectId(),
                    destCatalogRecord.getParentId(), destCatalogRecord.getRootCatalogId());
                destCatalogRecord = catalogStore.getCatalogById(context, catalogIdent);
                if (destCatalogRecord == null) {
                    throw new CatalogServerException(ErrorCode.CATALOG_ID_NOT_FOUND, destCatalogRecord.getCatalogId());
                }
            } else {
                throw new CatalogServerException(ErrorCode.INNER_SERVER_ERROR);
            }
        }

        if (srcCatalogRecord.getCatalogId().equals(destCatalogRecord.getCatalogId())) {
            if (srcLastVersion.compareTo(destLastVersion) > 0) {
                return destLastVersion;
            } else {
                return srcLastVersion;
            }
        }

        return null;
    }


    private void mergeDifferentCatalogBranch(TransactionContext context, String userId,
        List<DatabaseRefObject> srcDatabaseInfoList, CatalogIdent destCatalogIdent) throws CatalogServerException {

        CatalogObject destCatalogRecord = catalogStore.getCatalogById(context, destCatalogIdent);
        if (destCatalogRecord == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_ID_NOT_FOUND, destCatalogRecord.getCatalogId());
        }

        for (DatabaseRefObject databaseInfo : srcDatabaseInfoList) {

            DatabaseIdent srcDatabaseIdent = StoreConvertor.databaseIdent(databaseInfo.getProjectId(),
                databaseInfo.getCatalogId(), databaseInfo.getDatabaseId(), null);

            DatabaseBranchHelper.mergeNewDatabase(context, userId, srcDatabaseIdent, destCatalogRecord);
        }
    }

    private void mergeSameCatalogBranch(TransactionContext context, String userId,
        List<DatabaseRefObject> srcDatabaseInfoList, CatalogIdent destCatalogIdent,
        CatalogName destCatalogName, String sameMaxVersion) throws CatalogServerException {

        List<DatabaseRefObject> destDatabaseInfoList = DatabaseObjectHelper.listDatabases(context,
            destCatalogIdent, destCatalogName, Integer.MAX_VALUE, false);

        Map<String, DatabaseRefObject> destDatabaseInfoMap = destDatabaseInfoList.stream().collect(
            Collectors.toMap(DatabaseRefObject::getDatabaseId, Function.identity()));
        CatalogObject destCatalogRecord = catalogStore.getCatalogById(context, destCatalogIdent);

        if (destCatalogRecord == null) {
            throw new CatalogServerException(ErrorCode.CATALOG_ID_NOT_FOUND, destCatalogRecord.getCatalogId());
        }

        for (DatabaseRefObject databaseInfo : srcDatabaseInfoList) {
            DatabaseIdent srcDatabaseIdent = StoreConvertor.databaseIdent(databaseInfo.getProjectId(),
                databaseInfo.getCatalogId(), databaseInfo.getDatabaseId(), destCatalogIdent.getRootCatalogId());

            DatabaseRefObject destDatabaseInfo = destDatabaseInfoMap.get(databaseInfo.getDatabaseId());
            if (destDatabaseInfo != null) {
                // same database merge
                if (!databaseInfo.getName().equals(destDatabaseInfo.getName())) {
                    throw new CatalogServerException(ErrorCode.DATABASE_ALREADY_EXIST, databaseInfo.getName());
                }
                DatabaseIdent destDatabaseIdent = StoreConvertor.databaseIdent(destDatabaseInfo.getProjectId(),
                    destDatabaseInfo.getCatalogId(), destDatabaseInfo.getDatabaseId(), destCatalogIdent.getRootCatalogId());
                DatabaseBranchHelper.mergeDatabase(context, userId,
                    srcDatabaseIdent, destDatabaseIdent, sameMaxVersion);
            } else {
                // new database merge
                DatabaseBranchHelper.mergeNewDatabase(context, userId, srcDatabaseIdent, destCatalogRecord);
            }
        }
    }

    private String createCommitDetail(MergeBranchInput mergeBranchInput, byte[] srcVersion) {
        return new StringBuilder().append("merge srcBranch(name:")
            .append(mergeBranchInput.getSrcBranchName())
            .append(",version:")
            .append(CodecUtil.bytes2Hex(srcVersion))
            .append(") to ")
            .append(mergeBranchInput.getDestBranchName())
            .toString();
    }

    private CatalogIdent mergeBranchInternal(TransactionContext context, String projectId, MergeBranchInput mergeBranchInput,
        String catalogCommitId) {
        CatalogName srcCatalogName = new CatalogName(projectId, mergeBranchInput.getSrcBranchName());
        CatalogObject catalogRecord = catalogStore.getCatalogByName(context, srcCatalogName);
        CatalogIdent srcCatalogIdent = StoreConvertor.catalogIdent(projectId, catalogRecord.getCatalogId(),
            catalogRecord.getRootCatalogId());

        CatalogName destCatalogName = new CatalogName(projectId, mergeBranchInput.getDestBranchName());
        catalogRecord = catalogStore.getCatalogByName(context, destCatalogName);
        CatalogIdent destCatalogIdent = StoreConvertor.catalogIdent(projectId, catalogRecord.getCatalogId(),
            catalogRecord.getRootCatalogId());

        String version = VersionManagerHelper.getNextVersion(context, projectId, destCatalogIdent.getRootCatalogId());

        String sameMaxVersion = getSameMaxVersion(context, srcCatalogIdent, destCatalogIdent);

        List<DatabaseRefObject> srcDatabaseInfoList = DatabaseObjectHelper.listDatabases(context,
            srcCatalogIdent, srcCatalogName, Integer.MAX_VALUE, false);

        if (sameMaxVersion == null) {
            mergeDifferentCatalogBranch(context, mergeBranchInput.getOwner(), srcDatabaseInfoList,
                destCatalogIdent);
        } else {
            mergeSameCatalogBranch(context, mergeBranchInput.getOwner(), srcDatabaseInfoList,
                destCatalogIdent, destCatalogName, sameMaxVersion);
        }

        CatalogCommitObject catalogCommit = catalogStore
            .getLatestCatalogCommit(context, srcCatalogIdent,
                    VersionManagerHelper.getLatestVersion(context, projectId, srcCatalogIdent.getRootCatalogId()));

        long commitTime = RecordStoreHelper.getCurrentTime();
        catalogStore.insertCatalogCommit(context, destCatalogIdent.getProjectId(),
            destCatalogIdent.getCatalogId(), catalogCommitId, commitTime, MERGE_BRANCH,
            createCommitDetail(mergeBranchInput, CodecUtil.hex2Bytes(catalogCommit.getVersion())), version);

        return destCatalogIdent;
    }

    @Override
    public void mergeBranch(String projectId, MergeBranchInput mergeBranchInput) {
        CheckUtil.checkStringParameter(mergeBranchInput.getSrcBranchName(),
            mergeBranchInput.getDestBranchName());

        if (mergeBranchInput.getSrcBranchName().equalsIgnoreCase(mergeBranchInput.getDestBranchName())) {
            throw new CatalogServerException(ErrorCode.CATALOG_BRANCH_SAME, mergeBranchInput.getSrcBranchName());
        }

        String catalogCommitId = UuidUtil.generateCatalogCommitId();
        TransactionFrameRunner runner = new TransactionFrameRunner();
        runner.setMaxAttempts(CATALOG_STORE_MAX_RETRY_NUM);
        CatalogIdent catalogIdent = runner.run(context -> {
            return mergeBranchInternal(context, projectId, mergeBranchInput, catalogCommitId);
        }).getResultAndCheck(ret -> CatalogCommitHelper.catalogCommitExist(ret, catalogCommitId));
    }



}
