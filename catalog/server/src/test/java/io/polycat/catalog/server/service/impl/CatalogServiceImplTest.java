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
import java.util.UUID;
import java.util.List;

import java.util.HashMap;

import com.apple.foundationdb.tuple.Tuple;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogCommit;
import io.polycat.catalog.common.model.CatalogHistoryObject;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Column;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.FilterInput;
import io.polycat.catalog.common.plugin.request.input.MergeBranchInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.utils.CodecUtil;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;

import io.polycat.catalog.common.model.TableName;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class CatalogServiceImplTest extends TestUtil {

    private static final Logger log = Logger.getLogger(CatalogServiceImpl.class);

    @Test
    public void createEmptyCatalogTest() {
        String catalogName = UuidUtil.generateUUID32();

        CatalogInput catalogInput = getCatalogInput(catalogName);

        catalog = catalogService.createCatalog(projectId, catalogInput);

        Catalog catalog1 = catalogService.getCatalog(StoreConvertor.catalogName(projectId, catalogName));
        assertEquals(catalog1.getCatalogName(), catalogName);
        assertEquals(catalog1.getParentName(), "");
        assertNotEquals(0, catalog1.getCreateTime());
    }

    @Test
    public void createMultiCatalogTest() {
        String catalogName = UuidUtil.generateUUID32();

        for (int idx = 0; idx < 3; ++idx) {
            String curName = catalogName + idx;
            CatalogInput catalogInput = getCatalogInput(curName);

            assertNotNull(catalogService.createCatalog(projectId, catalogInput));
        }
    }

    @Test
    public void createCatalogRepeatTest() {
        String catalogName = UuidUtil.generateUUID32();

        CatalogInput catalogInput = getCatalogInput(catalogName);

        Catalog catalog1 = catalogService.createCatalog(projectId, catalogInput);
        assertNotNull(catalog1);

        CatalogServerException e = assertThrows(CatalogServerException.class, () -> {
            assertNull(catalogService.createCatalog(projectId, catalogInput));
        });
        assertEquals(e.getErrorCode(), ErrorCode.CATALOG_ALREADY_EXIST);
        assertEquals(e.getMessage(), String.format("Catalog [%s] already exists", catalogInput.getCatalogName()));
    }

    @Test
    public void createCatalogOnMultiProjectTest() {
        String catalogName = UuidUtil.generateUUID32();
        String projectId = UuidUtil.generateUUID32();

        for (int idx = 0; idx < 3; ++idx) {
            String curProjectId = projectId + idx;

            CatalogInput catalogInput = getCatalogInput(catalogName);

            catalogResourceService.createResource(curProjectId);
            assertNotNull(catalogService.createCatalog(curProjectId, catalogInput));
            catalogResourceService.dropResource(curProjectId);
        }
    }

    @Test
    public void createCatalogSameDatabaseTest() {
        String catalogName = UUID.randomUUID().toString().toLowerCase();
        String databaseName = UUID.randomUUID().toString().toLowerCase();

        CatalogInput catalogInput1 = getCatalogInput(catalogName);

        Catalog catalog1 = catalogService.createCatalog(projectId, catalogInput1);
        assertNotNull(catalog1);

        DatabaseInput dataBaseInput = getDatabaseDTO(projectId, catalogName, databaseName);
        CatalogName catalogNameObj = StoreConvertor.catalogName(projectId, catalogName);
        databaseService.createDatabase(catalogNameObj, dataBaseInput);

        CatalogInput catalogInput2 = getCatalogInput(databaseName);

        Catalog catalog2 = catalogService.createCatalog(projectId, catalogInput2);
        assertNotNull(catalog2);
    }

    @Test
    public void createCatalogAlreadyDropTest() {
        String catalogName = UuidUtil.generateUUID32();

        CatalogInput catalogInput = getCatalogInput(catalogName);

        Catalog catalog1 = catalogService.createCatalog(projectId, catalogInput);
        assertNotNull(catalog1);

        catalogService.dropCatalog(StoreConvertor
                .catalogName(projectId, catalogName));

        Catalog catalog2 = catalogService.createCatalog(projectId, catalogInput);
        assertNotNull(catalog2);
    }

    @Test
    public void createCatalogAlreadyRenameTest() {
        String catalogName = UuidUtil.generateUUID32();

        CatalogInput catalogInput = getCatalogInput(catalogName);

        Catalog catalog1 = catalogService.createCatalog(projectId, catalogInput);
        assertNotNull(catalog1);

        CatalogInput catalogInput1 = getCatalogInput(UuidUtil.generateUUID32());
        catalogService.alterCatalog(StoreConvertor
                .catalogName(projectId, catalogName), catalogInput1);

        Catalog catalog2 = catalogService.createCatalog(projectId, catalogInput);
        assertNotNull(catalog2);
    }

    @Test
    public void dropCatalogByNameTest() {
        String name = UuidUtil.generateUUID32();
        CatalogInput catalogInput = getCatalogInput(name);

        Catalog catalog = catalogService.createCatalog(projectId, catalogInput);
        assertNotNull(catalog);

        CatalogName catalogName = StoreConvertor.catalogName(projectId, name);
        catalogService.dropCatalog(catalogName);

        CatalogServerException e = assertThrows(CatalogServerException.class,
            () -> catalogService.getCatalog(catalogName));
        assertEquals(e.getErrorCode(), ErrorCode.CATALOG_NOT_FOUND);

    }

    @Test
    public void dropMultiCatalogByNameTest() {

        for (int idx = 0; idx < 3; ++idx) {
            String name = UuidUtil.generateUUID32();
            CatalogInput catalogInput = getCatalogInput(name);

            catalogService.createCatalog(projectId, catalogInput);

            CatalogName catalogName = StoreConvertor.catalogName(projectId, name);
            catalogService.dropCatalog(catalogName);

            CatalogServerException e = assertThrows(CatalogServerException.class,
                () -> catalogService.getCatalog(catalogName));
            assertEquals(e.getErrorCode(), ErrorCode.CATALOG_NOT_FOUND);
        }
    }

    @Test
    public void dropSameCatalogByNameTest() {
        String name = UuidUtil.generateUUID32();
        CatalogInput catalogInput = getCatalogInput(name);

        for (int idx = 0; idx < 3; ++idx) {
            String curProjectId = projectId + idx;
            catalogResourceService.createResource(curProjectId);
            catalogService.createCatalog(curProjectId, catalogInput);
        }

        int dropIdx = 1;
        String dropProjectId = projectId + dropIdx;

        CatalogName catalogName = StoreConvertor.catalogName(dropProjectId, name);
        catalogService.dropCatalog(catalogName);

        for (int idx = 0; idx < 3; ++idx) {
            String curProjectId = projectId + idx;
            CatalogName curCatalogName =
                    StoreConvertor.catalogName(curProjectId, name);

            if (idx == dropIdx) {
                CatalogServerException e = assertThrows(CatalogServerException.class,
                    () -> catalogService.getCatalog(curCatalogName));
                assertEquals(e.getErrorCode(), ErrorCode.CATALOG_NOT_FOUND);
            } else {
                Catalog catalog = catalogService.getCatalog(curCatalogName);
                assertNotNull(catalog);
            }

            catalogResourceService.dropResource(curProjectId);
        }
    }

    @Test
    public void alterCatalogNameTest() {
        String name = UuidUtil.generateUUID32();
        String name2 = UuidUtil.generateUUID32();
        CatalogInput catalogInput = getCatalogInput(name);
        CatalogInput renameCatalogInput = getCatalogInput(name2);

        Catalog catalog = catalogService.createCatalog(projectId, catalogInput);
        assertNotNull(catalog);

        CatalogName catalogName = StoreConvertor.catalogName(projectId, name);

        Catalog catalog1 = catalogService.getCatalog(catalogName);
        assertNotNull(catalog1);

        catalogService.alterCatalog(catalogName, renameCatalogInput);

        CatalogServerException e = assertThrows(CatalogServerException.class,
            () -> catalogService.getCatalog(catalogName));
        assertEquals(e.getErrorCode(), ErrorCode.CATALOG_NOT_FOUND);

        CatalogName catalogNameNew = StoreConvertor.catalogName(projectId, name2);
        Catalog catalog3 = catalogService.getCatalog(catalogNameNew);
        assertNotNull(catalog3);

        TraverseCursorResult<List<CatalogCommit>> catalogCommitList =
                catalogService.getCatalogCommits(catalogNameNew, Integer.MAX_VALUE,"");

        assertEquals(catalogCommitList.getResult().size(), 2);

        String version0 = catalogCommitList.getResult().get(0).getCommitVersion();
        String version1 = catalogCommitList.getResult().get(1).getCommitVersion();

        assertTrue(version0.compareTo(version1) > 0);
    }

    @Test
    public void getNotExistCatalogTest() {
        String catalogName = UuidUtil.generateUUID32();

        CatalogServerException e = assertThrows(CatalogServerException.class,
            () ->  catalogService.getCatalog(
                StoreConvertor.catalogName(projectId, catalogName)));
        assertEquals(e.getErrorCode(), ErrorCode.CATALOG_NOT_FOUND);
    }

    @Test
    public void getLatestCatalogVersionTest() {
        String catalogName = UuidUtil.generateUUID32();

        CatalogInput catalogInput = getCatalogInput(catalogName);

        Catalog catalog = catalogService.createCatalog(projectId, catalogInput);

        CatalogName catalogNameObject = StoreConvertor.catalogName(projectId, catalogName);
        Catalog catalogRecord = catalogService.getCatalog(catalogNameObject);
        assertNotNull(catalogRecord);
        CatalogHistoryObject catalogHistory = catalogService.getLatestCatalogVersion(catalogNameObject);
        assertNotNull(catalogHistory);

        assertNotNull(catalogHistory.getVersion());
        assertEquals(catalogRecord.getCatalogName(), catalogHistory.getName());

        long commitVersion1 = getCommitVersion(CodecUtil.hex2Bytes(catalogHistory.getVersion()));

        String catalogName1 = UUID.randomUUID().toString().toLowerCase();
        CatalogInput catalogInput1 = getCatalogInput(catalogName1);
        catalogService.alterCatalog(catalogNameObject, catalogInput1);
        catalogNameObject.setCatalogName(catalogName1);
        CatalogHistoryObject catalogHistory1 = catalogService.getLatestCatalogVersion(catalogNameObject);
        assertNotNull(catalogHistory1);
        long commitVersion2 = getCommitVersion(CodecUtil.hex2Bytes(catalogHistory1.getVersion()));
        assertTrue(commitVersion1 < commitVersion2);

        log.error("version1:{}, version2:{}", commitVersion1, commitVersion2);
    }

    @Test
    public void createOneLevelBranchCatalogTest() {

        String catalogName = UuidUtil.generateUUID32();

        CatalogInput catalogInput = getCatalogInput(catalogName);

        Catalog catalog = catalogService.createCatalog(projectId, catalogInput);

        Catalog parentCatalogRecord = catalogService.getCatalog(
                StoreConvertor.catalogName(projectId, catalogName));
        assertEquals(parentCatalogRecord.getCatalogName(), catalogName);
        assertEquals(parentCatalogRecord.getParentName(), "");

        //create branch
        String subCatalogName = UuidUtil.generateUUID32();
        CatalogName parentCatalogName = StoreConvertor.catalogName(projectId,
                parentCatalogRecord.getCatalogName());
        CatalogHistoryObject parentCatalogHistory = catalogService.getLatestCatalogVersion(parentCatalogName);
        assertNotNull(parentCatalogHistory);

        String version = parentCatalogHistory.getVersion();
        catalogInput = getCatalogInput(subCatalogName, parentCatalogRecord.getCatalogName(), version);

        Catalog subBranchCatalog = catalogService.createCatalog(projectId, catalogInput);

        Catalog subBranchCatalogRecord = catalogService.getCatalog(
                StoreConvertor.catalogName(projectId, subCatalogName));
        assertEquals(subBranchCatalogRecord.getCatalogName(), subCatalogName);
        assertEquals(subBranchCatalogRecord.getParentName(), parentCatalogRecord.getCatalogName());
        String subBranchVersion = subBranchCatalogRecord.getParentVersion();
        String parentBranchVersion = parentCatalogHistory.getVersion();
        assertEquals(subBranchVersion, parentBranchVersion);
    }

    @Test
    public void createTwoLevelBranchCatalogTest() {
        String mainCatalogName = UuidUtil.generateUUID32();
        String projectIdTest = UuidUtil.generateUUID32();
        catalogResourceService.createResource(projectIdTest);
        CatalogInput catalogInput = getCatalogInput(mainCatalogName);
        Catalog mainCatalog = catalogService.createCatalog(projectIdTest, catalogInput);

        Catalog mainCatalogRecord = catalogService.getCatalog(
                StoreConvertor.catalogName(projectIdTest, mainCatalogName));
        assertEquals(mainCatalogRecord.getCatalogName(), mainCatalogName);

        //create branch level1
        Catalog subBranchCatalog1 = createBranchLevel1(projectIdTest, mainCatalog);

        //create branch level2
        createBranchLevel2(projectIdTest, subBranchCatalog1);

        TraverseCursorResult<List<Catalog>> catalogs = catalogService.getCatalogs(projectIdTest,
            Integer.MAX_VALUE, "", "");

        assertEquals(catalogs.getResult().size(), 1);

        catalogResourceService.dropResource(projectIdTest);
    }

    private void createBranchLevel2(String projectId, Catalog subBranchCatalog1) {
        String subCatalogName2 = UuidUtil.generateUUID32();
        String parentVersion = catalogService.getLatestCatalogVersion(
            StoreConvertor.catalogName(projectId, subBranchCatalog1.getCatalogName()))
            .getVersion();
        catalogService.createCatalog(projectId,
            getCatalogInput(subCatalogName2, subBranchCatalog1.getCatalogName(), parentVersion));

        Catalog subBranchCatalogRecord2 = catalogService.getCatalog(
            StoreConvertor.catalogName(projectId, subCatalogName2));
        assertEquals(subBranchCatalogRecord2.getCatalogName(), subCatalogName2);
        assertEquals(subBranchCatalogRecord2.getParentName(), subBranchCatalog1.getCatalogName());
        assertEquals(subBranchCatalogRecord2.getParentVersion(), parentVersion);
    }

    private Catalog createBranchLevel1(String projectId, Catalog mainCatalog) {
        CatalogInput catalogInput;
        String subCatalogName1 = UuidUtil.generateUUID32();
        Catalog parentCatalog = mainCatalog;
        CatalogName parentCatalogName = StoreConvertor.catalogName(projectId,
                parentCatalog.getCatalogName());
        CatalogHistoryObject parentCatalogHistory = catalogService.getLatestCatalogVersion(parentCatalogName);

        String version = parentCatalogHistory.getVersion();
        catalogInput = getCatalogInput(subCatalogName1, parentCatalog.getCatalogName(), version);

        Catalog subBranchCatalog1 = catalogService.createCatalog(projectId, catalogInput);

        Catalog subBranchCatalogRecord1 = catalogService.getCatalog(
                StoreConvertor.catalogName(projectId, subCatalogName1));
        assertEquals(subBranchCatalogRecord1.getCatalogName(), subCatalogName1);

        String subBranchVersion = subBranchCatalogRecord1.getParentVersion();
        String parentBranchVersion = parentCatalogHistory.getVersion();
        assertEquals(subBranchVersion, parentBranchVersion);
        assertEquals(subBranchCatalog1.getCatalogName(), subBranchCatalogRecord1.getCatalogName());
        return subBranchCatalog1;
    }

    @Test
    public void listSubBranchCatalogsTest() {

        String mainCatalogName = UuidUtil.generateUUID32();

        HashMap<String, Catalog> branchCatalogRecords = new HashMap<String, Catalog>();

        CatalogInput catalogInput = getCatalogInput(mainCatalogName);

        Catalog mainCatalog = catalogService.createCatalog(projectId, catalogInput);

        Catalog mainCatalogRecord = catalogService.getCatalog(
                StoreConvertor.catalogName(projectId, mainCatalogName));
        assertEquals(mainCatalogRecord.getCatalogName(), mainCatalogName);
        assertEquals(mainCatalogRecord.getParentName(), "");

        //create sub-branch level1
        String subCatalogName1 = UuidUtil.generateUUID32();
        Catalog parentCatalog = mainCatalog;
        CatalogName parentCatalogName = StoreConvertor.catalogName(projectId,
                parentCatalog.getCatalogName());
        CatalogHistoryObject parentCatalogHistory = catalogService.getLatestCatalogVersion(parentCatalogName);

        String version = parentCatalogHistory.getVersion();
        catalogInput = getCatalogInput(subCatalogName1, parentCatalog.getCatalogName(), version);

        Catalog subBranchCatalog1 = catalogService.createCatalog(projectId, catalogInput);

        Catalog subBranchCatalogRecord1 = catalogService.getCatalog(
                StoreConvertor.catalogName(projectId, subCatalogName1));
        assertEquals(subBranchCatalogRecord1.getCatalogName(), subCatalogName1);
        assertEquals(subBranchCatalogRecord1.getParentName(), parentCatalog.getCatalogName());
        branchCatalogRecords.put(subBranchCatalogRecord1.getCatalogName(), subBranchCatalogRecord1);

        String subBranchVersion = subBranchCatalogRecord1.getParentVersion();
        String parentBranchVersion = parentCatalogHistory.getVersion();
        assertEquals(subBranchVersion, parentBranchVersion);
        assertEquals(subBranchCatalog1.getCatalogName(), subBranchCatalogRecord1.getCatalogName());

        //create branch1 level2
        String subCatalogName2 = UuidUtil.generateUUID32();
        parentCatalog = subBranchCatalog1;
        parentCatalogName = StoreConvertor.catalogName(projectId, parentCatalog.getCatalogName());
        parentCatalogHistory = catalogService.getLatestCatalogVersion(parentCatalogName);

        version = parentCatalogHistory.getVersion();
        catalogInput = getCatalogInput(subCatalogName2, parentCatalog.getCatalogName(), version);

        Catalog subBranchCatalog2 = catalogService.createCatalog(projectId, catalogInput);
        assertNotNull(subBranchCatalog2);
        Catalog subBranchCatalogRecord2 = catalogService.getCatalog(
                StoreConvertor.catalogName(projectId, subCatalogName2));
        assertNotNull(subBranchCatalogRecord2);
        branchCatalogRecords.put(subBranchCatalogRecord2.getCatalogName(), subBranchCatalogRecord2);

        //create branch2 level2
        String subCatalogName3 = UuidUtil.generateUUID32();
        parentCatalog = subBranchCatalog1;
        parentCatalogName = StoreConvertor.catalogName(projectId, parentCatalog.getCatalogName());
        parentCatalogHistory = catalogService.getLatestCatalogVersion(parentCatalogName);

        version = parentCatalogHistory.getVersion();
        catalogInput = getCatalogInput(subCatalogName3, parentCatalog.getCatalogName(), version);

        Catalog subBranchCatalog3 = catalogService.createCatalog(projectId, catalogInput);
        assertNotNull(subBranchCatalog3);
        Catalog subBranchCatalogRecord3 = catalogService.getCatalog(
                StoreConvertor.catalogName(projectId, subCatalogName3));
        assertNotNull(subBranchCatalogRecord3);
        branchCatalogRecords.put(subBranchCatalogRecord3.getCatalogName(), subBranchCatalogRecord3);

        long versionLong = RecordStoreHelper.convertVersion(CodecUtil.hex2Bytes(subBranchCatalogRecord1.getParentVersion()));
        log.info("lv1 branch " + subBranchCatalogRecord1.getCatalogName() + " parentName "
                + subBranchCatalogRecord1.getParentName()
                + " versionLong " + versionLong);

        versionLong = RecordStoreHelper.convertVersion(CodecUtil.hex2Bytes(subBranchCatalogRecord2.getParentVersion()));
        log.info("lv2 branch " + subBranchCatalogRecord2.getCatalogName() + " parentName "
                + subBranchCatalogRecord2.getParentName()
                + " versionLong " + versionLong);

        versionLong = RecordStoreHelper.convertVersion(CodecUtil.hex2Bytes(subBranchCatalogRecord3.getParentVersion()));
        log.info("lv2 branch " + subBranchCatalogRecord3.getCatalogName() + " parentName "
                + subBranchCatalogRecord3.getParentName()
                + " versionLong " + versionLong);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainCatalogName);
        List<Catalog> subBranchCatalogs = catalogService.listSubBranchCatalogs(mainCatalogPathName);

        assertEquals(subBranchCatalogs.size(), 3);

        for (Catalog catalog : subBranchCatalogs) {

            assertEquals(branchCatalogRecords.containsKey(catalog.getCatalogName()), true);
            Catalog catalogRecord = branchCatalogRecords.get(catalog.getCatalogName());

            assertEquals(catalogRecord.getCatalogName(), catalog.getCatalogName());

        }
    }

    @Test
    public void listCatalogTest() {
        String mainCatalogName = UuidUtil.generateUUID32();
        String projectIdTest = UuidUtil.generateUUID32();
        catalogResourceService.createResource(projectIdTest);
        HashMap<String, Catalog> branchCatalogRecords = new HashMap<String, Catalog>();

        CatalogInput catalogInput = getCatalogInput(mainCatalogName);

        Catalog mainCatalog = catalogService.createCatalog(projectIdTest, catalogInput);

        Catalog mainCatalogRecord = catalogService.getCatalog(StoreConvertor.catalogName(projectIdTest, mainCatalogName));
        assertEquals(mainCatalogRecord.getCatalogName(), mainCatalogName);
        assertEquals(mainCatalogRecord.getParentName(), "");

        //create sub-branch level1
        String subCatalogName1 = UuidUtil.generateUUID32();
        Catalog parentCatalog = mainCatalog;
        CatalogName parentCatalogName = StoreConvertor.catalogName(projectIdTest,
                parentCatalog.getCatalogName());
        CatalogHistoryObject parentCatalogHistory = catalogService.getLatestCatalogVersion(parentCatalogName);

        String version = parentCatalogHistory.getVersion();
        Catalog subBranchCatalog1 = catalogService.createCatalog(projectIdTest,
            getCatalogInput(subCatalogName1, parentCatalog.getCatalogName(), version));

        Catalog subBranchCatalogRecord1 = catalogService.getCatalog(
                StoreConvertor.catalogName(projectIdTest, subCatalogName1));
        assertEquals(subBranchCatalogRecord1.getCatalogName(), subCatalogName1);

        branchCatalogRecords.put(subBranchCatalogRecord1.getCatalogName(), subBranchCatalogRecord1);

        String subBranchVersion = subBranchCatalogRecord1.getParentVersion();
        String parentBranchVersion = parentCatalogHistory.getVersion();
        assertEquals(subBranchVersion, parentBranchVersion);
        assertEquals(subBranchCatalog1.getCatalogName(), subBranchCatalogRecord1.getCatalogName());

        //create branch1 level2
        String subCatalogName2 = UuidUtil.generateUUID32();
        parentCatalog = subBranchCatalog1;
        parentCatalogName = StoreConvertor.catalogName(projectIdTest, parentCatalog.getCatalogName());
        parentCatalogHistory = catalogService.getLatestCatalogVersion(parentCatalogName);

        catalogInput = getCatalogInput(subCatalogName2, parentCatalog.getCatalogName(),
           parentCatalogHistory.getVersion());

        Catalog subBranchCatalog2 = catalogService.createCatalog(projectIdTest, catalogInput);
        assertNotNull(subBranchCatalog2);
        Catalog subBranchCatalogRecord2 = catalogService.getCatalog(
                StoreConvertor.catalogName(projectIdTest, subCatalogName2));
        assertNotNull(subBranchCatalogRecord2);

        branchCatalogRecords.put(subBranchCatalogRecord2.getCatalogName(), subBranchCatalogRecord2);

        //create branch2 level2
        String subCatalogName3 = UuidUtil.generateUUID32();
        parentCatalogName = StoreConvertor.catalogName(projectIdTest, parentCatalog.getCatalogName());
        parentCatalogHistory = catalogService.getLatestCatalogVersion(parentCatalogName);

        catalogInput = getCatalogInput(subCatalogName3, parentCatalog.getCatalogName(),
            parentCatalogHistory.getVersion());

        Catalog subBranchCatalog3 = catalogService.createCatalog(projectIdTest, catalogInput);
        assertNotNull(subBranchCatalog3);

        Catalog subBranchCatalogRecord3 = catalogService.getCatalog(
                StoreConvertor.catalogName(projectIdTest, subCatalogName3));
        assertNotNull(subBranchCatalogRecord3);

        branchCatalogRecords.put(subBranchCatalogRecord3.getCatalogName(), subBranchCatalogRecord3);

        log.info("main branch " + mainCatalogRecord.getCatalogName());

        long versionLong = RecordStoreHelper.convertVersion(CodecUtil.hex2Bytes(subBranchCatalogRecord1.getParentVersion()));
        log.info("lv1 branch " + subBranchCatalogRecord1.getCatalogName() + " parentName "
                + subBranchCatalogRecord1.getParentName()
                + " versionLong " + versionLong);

        versionLong = RecordStoreHelper.convertVersion(CodecUtil.hex2Bytes(subBranchCatalogRecord2.getParentVersion()));
        log.info("lv2 branch " + subBranchCatalogRecord2.getCatalogName() + " parentName "
                + subBranchCatalogRecord2.getParentName()
                + " versionLong " + versionLong);

        versionLong = RecordStoreHelper.convertVersion(CodecUtil.hex2Bytes(subBranchCatalogRecord3.getParentVersion()));
        log.info("lv2 branch " + subBranchCatalogRecord3.getCatalogName() + " parentName "
                + subBranchCatalogRecord3.getParentName()
                + " versionLong " + versionLong);

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectIdTest, mainCatalogName);
        List<Catalog> subBranchCatalogs = catalogService.listSubBranchCatalogs(mainCatalogPathName);

        assertEquals(subBranchCatalogs.size(), 3);

        for (Catalog catalog : subBranchCatalogs) {
            assertTrue(branchCatalogRecords.containsKey(catalog.getCatalogName()));
            Catalog catalogRecord = branchCatalogRecords.get(catalog.getCatalogName());
            assertEquals(catalogRecord.getCatalogName(), catalog.getCatalogName());
        }

        TraverseCursorResult<List<Catalog>> rootCatalogList = catalogService.getCatalogs(projectIdTest,
            Integer.MAX_VALUE, "", "");

        assertEquals(rootCatalogList.getResult().size(), 1);
        assertEquals(rootCatalogList.getResult().get(0).getCatalogName(), mainCatalog.getCatalogName());

        catalogResourceService.dropResource(projectIdTest);
    }

    @Test
    public void listCatalogCommitTest() {
        String name = UuidUtil.generateUUID32();
        CatalogInput catalogInput = getCatalogInput(name);

        Catalog catalog = catalogService.createCatalog(projectId, catalogInput);
        assertNotNull(catalog);
        CatalogName catalogName = StoreConvertor.catalogName(projectId, name);
        TraverseCursorResult<List<CatalogCommit>> catalogCommitList =
            catalogService.getCatalogCommits(catalogName, Integer.MAX_VALUE, "");
        assertEquals(1, catalogCommitList.getResult().size());
    }

    public static MergeBranchInput createMergeBranchInput(String userId,
        String srcBranchName, String destBranchName) {
        MergeBranchInput mergeBranchInput = new MergeBranchInput();

        mergeBranchInput.setOwner(userId);
        mergeBranchInput.setSrcBranchName(srcBranchName);
        mergeBranchInput.setDestBranchName(destBranchName);

        return mergeBranchInput;
    }

    @Test
    public void mergeBranchWithNewTable() {
        // create main catalog
        String mainCatalogName = "mainCatalogName" + UuidUtil.generateId();
        Catalog mainCatalog = catalogService.createCatalog(projectId, getCatalogInput(mainCatalogName));

        // create database
        String dbName = "db_name" + UuidUtil.generateId();
        DatabaseInput databaseInput = getDatabaseDTO(projectId, mainCatalogName, dbName);
        CatalogName catalogName = StoreConvertor.catalogName(projectId, mainCatalogName);
        Database database = databaseService.createDatabase(catalogName, databaseInput);

        TraverseCursorResult<List<Database>> databaseList = databaseService
            .listDatabases(StoreConvertor.catalogName(projectId, mainCatalogName),
                false, 10, "", "");
        assertEquals(databaseList.getResult().size(), 1);
        assertEquals(databaseList.getResult().get(0).getDatabaseName(), dbName.toLowerCase());

        // create dev branch
        String devBranchName = "dev_branch_name" + UuidUtil.generateId();
        String version =  VersionManagerHelper.getLatestVersionByName(projectId, mainCatalogName);
        Catalog devBranch = catalogService.createCatalog(projectId,
                getCatalogInput(devBranchName, mainCatalog.getCatalogName(), version));

        databaseList = databaseService.listDatabases(StoreConvertor.catalogName(projectId, devBranchName),
            false, 10, "", "");
        assertEquals(databaseList.getResult().size(), 1);
        assertEquals(databaseList.getResult().get(0).getDatabaseName(), dbName.toLowerCase());

        // create table
        String newTableName = "new_table_name" + UuidUtil.generateId();
        TableInput tableInput = getTableDTO(newTableName, 3);
        DatabaseName databaseName = StoreConvertor.databaseName(projectId, devBranchName, dbName);
        tableService.createTable(databaseName, tableInput);

        // mergeTable
        MergeBranchInput mergeBranchInput = createMergeBranchInput(userId, devBranchName, mainCatalogName);
        catalogService.mergeBranch(projectId, mergeBranchInput);

        databaseName = StoreConvertor.databaseName(projectId, mainCatalogName, dbName);
        TraverseCursorResult<List<Table>> tableList = tableService.listTable(databaseName, false,
                Integer.MAX_VALUE, "", "");
        assertEquals(tableList.getResult().size(), 1);
        assertEquals(tableList.getResult().get(0).getCatalogName(), mainCatalogName);
        assertEquals(tableList.getResult().get(0).getDatabaseName(), database.getDatabaseName());
    }

    private List<Catalog> prepareTestCreateCatalog(String projectIdTest, int catalogNum) {
        List<Catalog> catalogList = new ArrayList<>();
        //create main branch table
        for (int i = 0; i < catalogNum; i++) {
            String catalogName = UuidUtil.generateUUID32();
            CatalogInput catalogInput = getCatalogInput(catalogName);
            Catalog catalog = catalogService.createCatalog(projectIdTest, catalogInput);
            catalogList.add(catalog);
        }

        return  catalogList;
    }

    private void listCatalogAndCheckResult(int maxBatchNum, String projectId, int checkNum, Boolean includeDropped) {
        for (int i = 1; i <= maxBatchNum; i++) {
            List<Catalog> catalogRecordList = new ArrayList<>();
            String nextToken = "";

            while (true) {
                TraverseCursorResult<List<Catalog>> catalogs = catalogService.getCatalogs(projectId,
                    i, nextToken, "");
                catalogRecordList.addAll(catalogs.getResult());

                if (catalogs.getResult().size() > 0) {
                    nextToken = catalogs.getContinuation().map(catalogToken -> {
                        return catalogToken.toString();
                    }).orElse(null);
                    if (nextToken == null) {
                        break;
                    }
                }
                if (catalogs.getResult().size() == 0) {
                    break;
                }
            }
            assertEquals(catalogRecordList.size(), checkNum);
        }
    }

    @Test
    public void listCatalogWitTokenTest() {
        int mainCatalogNum = 2;
        String projectIdTest = UuidUtil.generateUUID32();
        catalogResourceService.createResource(projectIdTest);

        List<Catalog> catalogList = prepareTestCreateCatalog(projectIdTest, mainCatalogNum);

        //create sub-branch level1
        String subCatalogName1 = UuidUtil.generateUUID32();
        Catalog parentCatalog = catalogList.get(0);
        CatalogName parentCatalogIdent = StoreConvertor.catalogName(projectIdTest,
            parentCatalog.getCatalogName());
        CatalogHistoryObject parentCatalogHistory = catalogService.getLatestCatalogVersion(parentCatalogIdent);

        String version = parentCatalogHistory.getVersion();
        CatalogInput subCatalogInput = getCatalogInput(subCatalogName1, parentCatalog.getCatalogName(), version);

        Catalog subBranchCatalog1 = catalogService.createCatalog(projectIdTest, subCatalogInput);
        assertNotNull(subBranchCatalog1);

        //drop catalog
        Catalog droppedCatalog = catalogList.get(1);
        CatalogName droppedCatalogName = StoreConvertor.catalogName(projectIdTest, droppedCatalog.getCatalogName());
        catalogService.dropCatalog(droppedCatalogName);

        listCatalogAndCheckResult(12, projectIdTest, mainCatalogNum - 1, true);
        listCatalogAndCheckResult(12, projectIdTest, mainCatalogNum - 1, false);

        catalogResourceService.dropResource(projectIdTest);
    }

    private List<Database> createDatabasePrepareTest(String projectId, CatalogName catalogName,
        int databaseNum) {
        List<Database> databaseList = new ArrayList<>(databaseNum);

        //create main branch table
        for (int i = 0; i < databaseNum; i++) {
            String databaseName = UUID.randomUUID().toString().toLowerCase();

            DatabaseInput DatabaseInput = getDatabaseDTO(projectId, catalogName.getCatalogName(),
                databaseName);
            Database createDB = databaseService.createDatabase(catalogName, DatabaseInput);
            assertNotNull(createDB);
            assertEquals(createDB.getDatabaseName(), databaseName);

            databaseList.add(createDB);

        }
        return databaseList;
    }

    private List<Database> prepareOneLevelBranchTest(String mainBranchCatalogName, String subBranchCatalogName,
        int mainBranchDatabaseNum) {
        //create main branch
        Catalog mainCatalog = createCatalogPrepareTest(projectId, mainBranchCatalogName);

        //create main branch database
        CatalogName mainCatalogName = StoreConvertor
            .catalogName(projectId, mainCatalog.getCatalogName());
        List<Database> mainDatabaseList = createDatabasePrepareTest(projectId, mainCatalogName,
            mainBranchDatabaseNum);

        //create branch
        String subCatalogName = subBranchCatalogName;
        String version = VersionManagerHelper.getLatestVersionByName(projectId, mainCatalog.getCatalogName());
        CatalogInput catalogInput = getCatalogInput(subCatalogName, mainCatalog.getCatalogName(), version);
        Catalog subBranchCatalog = catalogService.createCatalog(projectId, catalogInput);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(
            StoreConvertor.catalogName(projectId, subCatalogName));
        assertEquals(subBranchCatalogRecord.getCatalogName(), subCatalogName);
        assertEquals(subBranchCatalogRecord.getParentName(), mainCatalog.getCatalogName());
        String subBranchVersion = subBranchCatalogRecord.getParentVersion();
        String parentBranchVersion = version;
        assertEquals(subBranchVersion, parentBranchVersion);
        assertEquals(subBranchCatalogRecord.getCatalogName(), subBranchCatalog.getCatalogName());

        log.info("main catalog " + mainCatalog);
        log.info("sub catalog " + subBranchCatalog);

        for (Database database : mainDatabaseList) {
            log.info("main table  " + database);
        }

        return mainDatabaseList;
    }

    private void listCatalogCommitAndCheckResult(int maxBatchNum, CatalogName catalogName, int checkNum) {
        for (int i = 1; i <= maxBatchNum; i++) {
            List<CatalogCommit> catalogCommitList = new ArrayList<>();
            String nextToken = "";

            while (true) {
                TraverseCursorResult<List<CatalogCommit>> catalogs = catalogService.getCatalogCommits(catalogName,
                    i, nextToken);
                catalogCommitList.addAll(catalogs.getResult());

                if (catalogs.getResult().size() > 0) {
                    nextToken = catalogs.getContinuation().map(catalogToken -> {
                        return catalogToken.toString();
                    }).orElse(null);
                    if (nextToken == null) {
                        break;
                    }
                }
                if (catalogs.getResult().size() == 0) {
                    break;
                }
            }
            assertEquals(catalogCommitList.size(), checkNum);
        }
    }

    @Test
    public void listCatalogCommitTokenTest() {
        String mainBranchCatalogName = UuidUtil.generateUUID32();
        String subBranchCatalogName = UuidUtil.generateUUID32();
        int mainBranchDatabaseNum = 7;

        CatalogName mainCatalogPathName = StoreConvertor.catalogName(projectId, mainBranchCatalogName);
        List<Database> databaseList = prepareOneLevelBranchTest(mainBranchCatalogName, subBranchCatalogName,
            mainBranchDatabaseNum);
        assertEquals(databaseList.size(), mainBranchDatabaseNum);

        //create database at sub branch
        CatalogName subCatalogPathName = StoreConvertor.catalogName(projectId, subBranchCatalogName);
        Catalog subBranchCatalogRecord = catalogService.getCatalog(subCatalogPathName);
        assertNotNull(subBranchCatalogRecord);
        List<Database> subDatabaseList = createDatabasePrepareTest(projectId, subCatalogPathName,
            mainBranchDatabaseNum);
        assertEquals(subDatabaseList.size(), mainBranchDatabaseNum);

        //list main branch
        listCatalogCommitAndCheckResult(12, mainCatalogPathName, mainBranchDatabaseNum + 1);

        //list sub branch
        listCatalogCommitAndCheckResult(12, subCatalogPathName, mainBranchDatabaseNum + 1);
    }

    @Test
    public void mergeBranchWithNewEmptyDB() {
        CatalogName catalogName1 = new CatalogName(projectId ,"catalog" + UuidUtil.generateId());
        CatalogInput catalogInput = getCatalogInput(catalogName1.getCatalogName());
        Catalog catalogRecord1 = catalogService.createCatalog(projectId, catalogInput);

        CatalogName catalogName2 = new CatalogName(projectId, "catalog" + UuidUtil.generateId());
        catalogInput = getCatalogInput(catalogName2.getCatalogName());
        Catalog catalogRecord2 = catalogService.createCatalog(projectId, catalogInput);

        DatabaseName databaseName = new DatabaseName(catalogName2.getProjectId(), catalogName2.getCatalogName() ,
            "database" + UuidUtil.generateId());
        DatabaseInput databaseInput = getDatabaseDTO(databaseName.getProjectId(), databaseName.getCatalogName(),
            databaseName.getDatabaseName());
        Database databaseRecord = databaseService.createDatabase(catalogName2, databaseInput);

        MergeBranchInput mergeBranchInput = createMergeBranchInput(userId, catalogRecord2.getCatalogName(),
            catalogRecord1.getCatalogName());
        catalogService.mergeBranch(projectId, mergeBranchInput);

        Tuple pattern = Tuple.from(false, Integer.MAX_VALUE, "", "");
        TraverseCursorResult<List<Database>> databaseInfoList = databaseService.listDatabases(
            catalogName1,
            false,
            1000,
            "",
            "");
        assertEquals(1, databaseInfoList.getResult().size());
        assertEquals(catalogRecord1.getCatalogName(), databaseInfoList.getResult().get(0).getCatalogName());
        assertEquals(databaseRecord.getDatabaseName(), databaseInfoList.getResult().get(0).getDatabaseName());
        assertEquals(0, databaseInfoList.getResult().get(0).getDroppedTime(), "database is dropped");
    }

    @Test
    public void mergeBranchWithNewDB() {
        CatalogName catalogName1 = new CatalogName(projectId, "catalog1" + UuidUtil.generateId());
        CatalogInput catalogInput = getCatalogInput(catalogName1.getCatalogName());
        Catalog catalogRecord1 = catalogService.createCatalog(catalogName1.getProjectId(), catalogInput);

        CatalogName catalogName2 = new CatalogName(projectId, "catalog2" + UuidUtil.generateId());
        catalogInput = getCatalogInput(catalogName2.getCatalogName());
        Catalog catalogRecord2 = catalogService.createCatalog(catalogName2.getProjectId(), catalogInput);

        DatabaseName databaseName = new DatabaseName(catalogName2.getProjectId(), catalogName2.getCatalogName(),
            "database" + UuidUtil.generateId());
        DatabaseInput databaseInput = getDatabaseDTO(databaseName.getProjectId(), databaseName.getCatalogName(),
            databaseName.getDatabaseName());
        Database databaseRecord = databaseService.createDatabase(catalogName2, databaseInput);

        String tableName1 = "table1" + UuidUtil.generateId();
        TableInput tableInput = getTableDTO(tableName1, 3);
        tableService.createTable(databaseName, tableInput);
        String tableName2 = "table2" + UuidUtil.generateId();
        tableInput = getTableDTO(tableName2, 3);
        tableService.createTable(databaseName, tableInput);

        MergeBranchInput mergeBranchInput = createMergeBranchInput(userId, catalogRecord2.getCatalogName(),
            catalogRecord1.getCatalogName());
        catalogService.mergeBranch(projectId, mergeBranchInput);

        TraverseCursorResult<List<Database>> databaseInfoList = databaseService.listDatabases(
            catalogName1,
            false,
            1000,
            "",
            "");
        assertEquals(1, databaseInfoList.getResult().size());

        DatabaseName databaseName1 = StoreConvertor.databaseName(projectId,
            databaseInfoList.getResult().get(0).getCatalogName(), databaseInfoList.getResult().get(0).getDatabaseName());
        TraverseCursorResult<List<Table>> tableRecordList = tableService.listTable(databaseName1,
            false, Integer.MAX_VALUE, "", "");
        assertEquals(tableRecordList.getResult().size(), 2);

        HashMap<String, String> tableRecordsMap = new HashMap<String, String>();
        tableRecordsMap.put(tableName1, tableName1);
        tableRecordsMap.put(tableName2, tableName1);

        assertEquals(tableRecordList.getResult().get(0).getCatalogName(), catalogRecord1.getCatalogName());
        assertEquals(tableRecordList.getResult().get(0).getDatabaseName(),
            databaseInfoList.getResult().get(0).getDatabaseName());

        assertTrue(tableRecordsMap.containsKey(tableRecordList.getResult().get(0).getTableName()));
        assertTrue(tableRecordsMap.containsKey(tableRecordList.getResult().get(1).getTableName()));
    }

    @Test
    public void mergeBranchWithTable() {
        CatalogName catalogName = new CatalogName(projectId, "catalog1" + UuidUtil.generateId());
        CatalogInput catalogInput = getCatalogInput(catalogName.getCatalogName());
        Catalog catalogRecord1 = catalogService.createCatalog(catalogName.getProjectId(), catalogInput);

        DatabaseName databaseName1 = new DatabaseName(catalogName.getProjectId(), catalogName.getCatalogName(),
            "database" + UuidUtil.generateId());
        DatabaseInput databaseInput = getDatabaseDTO(databaseName1.getProjectId(), databaseName1.getCatalogName(),
            databaseName1.getDatabaseName());
        Database databaseRecord = databaseService.createDatabase(catalogName, databaseInput);

        String tableName = "table" + UuidUtil.generateId();
        TableInput tableInput = getTableDTO(tableName, 3);
        tableInput.setLmsMvcc(true);
        tableInput.setPartitionKeys(Collections.singletonList(new Column("part", "String")));
        tableService.createTable(databaseName1, tableInput);

        TableName tableName1 = StoreConvertor.tableName(projectId, databaseName1.getCatalogName(),
            databaseName1.getDatabaseName(), tableName);
        Table getTbl = tableService.getTableByName(tableName1);
        String partitionName1 = "part=1";
        partitionService.addPartition(tableName1, buildPartition(getTbl, partitionName1, partitionName1 + "path"));

        TraverseCursorResult<List<CatalogCommit>> catalogCommitList = catalogService
            .getCatalogCommits(catalogName, Integer.MAX_VALUE, "");
        assertEquals(catalogCommitList.getResult().size(), 4);

        String branchName = "branch" + UuidUtil.generateId();
        Catalog catalogRecord2 = createBranch(catalogName, branchName, projectId);

        DatabaseName databaseName2 = StoreConvertor.databaseName(projectId,
            branchName, databaseName1.getDatabaseName());
        TraverseCursorResult<List<Table>> tableRecordList1 = tableService.listTable(databaseName2,
            false, Integer.MAX_VALUE, "", "");
        assertEquals(tableRecordList1.getResult().size(), 1);
        assertEquals(tableRecordList1.getResult().get(0).getTableName(), tableName);

        TableName tableName2 = StoreConvertor.tableName(projectId, branchName,
            databaseName1.getDatabaseName(), tableName);
        getTbl = tableService.getTableByName(tableName2);
        String partitionName2 = "part=2";
        partitionService.addPartition(tableName2, buildPartition(getTbl, partitionName2, partitionName2 + "path"));

        MergeBranchInput mergeBranchInput = createMergeBranchInput(userId, catalogRecord2.getCatalogName(),
            catalogRecord1.getCatalogName());
        catalogService.mergeBranch(projectId, mergeBranchInput);

        TraverseCursorResult<List<Table>> tableRecordList = tableService.listTable(databaseName1,
            false, Integer.MAX_VALUE, "", "");
        assertEquals(tableRecordList.getResult().size(), 1);

        Partition[] partitionList = partitionService.listPartitions(StoreConvertor.tableName(projectId,
            databaseName1.getCatalogName(), databaseName1.getDatabaseName(), tableName), (FilterInput)null);
        assertEquals(partitionList.length, 2);
    }

}
