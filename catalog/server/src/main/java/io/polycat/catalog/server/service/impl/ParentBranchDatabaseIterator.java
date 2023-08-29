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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.TableCommitObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.CatalogStore;
import io.polycat.catalog.store.api.TableMetaStore;
import io.polycat.catalog.store.common.StoreConvertor;

public class ParentBranchDatabaseIterator implements Iterator<Map<DatabaseIdent, String>> {
    private final static TableMetaStore tableMetaStore = ParentBranchCatalogIteratorHelper.getTableMetaStore();
    private final static CatalogStore catalogStore = ParentBranchCatalogIteratorHelper.getCatalogStore();
    private Iterator<Map<DatabaseIdent, String>> parentDatabaseIterator;
    Map<DatabaseIdent, String> nextMap = Collections.emptyMap();

    public ParentBranchDatabaseIterator(TransactionContext ctx, TableIdent subBranchTableIdent, Boolean dropped) {
        parentDatabaseIterator = getParentDatabaseList(ctx, subBranchTableIdent, dropped).listIterator();
    }

    public ParentBranchDatabaseIterator(TransactionContext ctx, TableIdent subBranchTableIdent) {
        parentDatabaseIterator = getParentDatabaseList(ctx, subBranchTableIdent).listIterator();
    }

    public ParentBranchDatabaseIterator(TransactionContext ctx, DatabaseIdent subBranchDatabaseIdent) {
        parentDatabaseIterator = getParentDatabaseList(ctx, subBranchDatabaseIdent).listIterator();
    }

    public ParentBranchDatabaseIterator(TransactionContext ctx, TableIdent subBranchTableIdent, String version) {
        parentDatabaseIterator = getParentDatabaseList(ctx, subBranchTableIdent, version).listIterator();
    }

    public boolean hasNext() {
        return parentDatabaseIterator.hasNext();
    }

    public Map<DatabaseIdent, String> next() {
        return parentDatabaseIterator.next();
    }

    public DatabaseIdent nextDatabase() {
        nextMap = parentDatabaseIterator.next();
        Iterator<DatabaseIdent> iterator = nextMap.keySet().iterator();
        return iterator.next();
    }

    public String nextBranchVersion(DatabaseIdent databaseIdent) {
        return nextMap.get(databaseIdent);
    }

    public void remove() {
        throw new UnsupportedOperationException("This Iterator does not implement the remove method");
    }

    private List<Map<DatabaseIdent, String>> getDroppedParentDatabaseList(TransactionContext ctx,
        TableIdent subBranchTableIdent, List<Map<DatabaseIdent, String>> parentDatabaseList) {
        if (parentDatabaseList.isEmpty()) {
            return Collections.emptyList();
        }

        List<Map<DatabaseIdent, String>> parentDroppedCatalogList = new ArrayList<>();
        Map<DatabaseIdent, String> map = parentDatabaseList.get(0);
        Iterator<Map.Entry<DatabaseIdent, String>> iterator = map.entrySet().iterator();
        Map.Entry<DatabaseIdent, String> entry = iterator.next();
        DatabaseIdent parentDatabaseIdent = entry.getKey();
        String subBranchVersion = entry.getValue();
        TableIdent parentTableIdent = StoreConvertor.tableIdent(parentDatabaseIdent,
            subBranchTableIdent.getTableId());
        Optional<TableCommitObject> tableCommit = tableMetaStore.getLatestTableCommit(ctx, parentTableIdent, subBranchVersion);
        if (tableCommit.isPresent()) {
            if (isDropCommit(tableCommit.get())) {
                parentDroppedCatalogList.add(map);
            }
        }

        return parentDroppedCatalogList;
    }

    private List<Map<DatabaseIdent, String>> getValidParentDatabaseList(TransactionContext ctx,
        TableIdent subBranchTableIdent, List<Map<DatabaseIdent, String>> parentDatabaseList) {
        List<Map<DatabaseIdent, String>> parentValidDatabaseList = new ArrayList<>();
        for (Map<DatabaseIdent, String> map : parentDatabaseList) {
            Iterator<Map.Entry<DatabaseIdent, String>> iterator = map.entrySet().iterator();
            Map.Entry<DatabaseIdent, String> entry = iterator.next();
            DatabaseIdent parentDatabaseIdent = entry.getKey();
            String subBranchVersion = entry.getValue();
            TableIdent parentTableIdent = StoreConvertor.tableIdent(parentDatabaseIdent,
                subBranchTableIdent.getTableId());
            Optional<TableCommitObject> tableCommit = tableMetaStore.getLatestTableCommit(ctx, parentTableIdent,
                subBranchVersion);
            if (tableCommit.isPresent()) {
                if (isDropCommit(tableCommit.get())) {
                    break;
                }
                parentValidDatabaseList.add(map);
            }
        }

        return parentValidDatabaseList;
    }

    private List<Map<DatabaseIdent, String>> getParentDatabaseList(TransactionContext ctx,
        TableIdent subBranchTableIdent, Boolean dropped) {
        DatabaseIdent subBranchDatabaseIdent = StoreConvertor.databaseIdent(subBranchTableIdent.getProjectId(),
            subBranchTableIdent.getCatalogId(), subBranchTableIdent.getDatabaseId(), subBranchTableIdent.getRootCatalogId());
        List<Map<DatabaseIdent, String>> parentDatabaseList = getParentDatabaseList(ctx,
            subBranchDatabaseIdent);

        //check most recent parent branch is dropped
        if (dropped) {
            return getDroppedParentDatabaseList(ctx, subBranchTableIdent, parentDatabaseList);
        }

        return getValidParentDatabaseList(ctx, subBranchTableIdent, parentDatabaseList);
    }

    private List<Map<DatabaseIdent, String>> getParentDatabaseList(TransactionContext ctx,
        TableIdent subBranchTableIdent) {
        DatabaseIdent subBranchDatabaseIdent = StoreConvertor.databaseIdent(subBranchTableIdent.getProjectId(),
            subBranchTableIdent.getCatalogId(), subBranchTableIdent.getDatabaseId(), subBranchTableIdent.getRootCatalogId());
        List<Map<DatabaseIdent, String>> parentDatabaseList = getParentDatabaseList(ctx,
            subBranchDatabaseIdent);

        List<Map<DatabaseIdent, String>> parentValidDatabaseList = new ArrayList<>();
        for (Map<DatabaseIdent, String> map : parentDatabaseList) {
            Iterator<Map.Entry<DatabaseIdent, String>> iterator = map.entrySet().iterator();
            Map.Entry<DatabaseIdent, String> entry = iterator.next();
            DatabaseIdent parentDatabaseIdent = entry.getKey();
            String subBranchVersion = entry.getValue();

            TableIdent parentTableIdent = StoreConvertor.tableIdent(parentDatabaseIdent, subBranchTableIdent.getTableId());
            Optional<TableCommitObject> tableCommit = tableMetaStore.getLatestTableCommit(ctx, parentTableIdent,
                subBranchVersion);
            if (tableCommit.isPresent()) {
                parentValidDatabaseList.add(map);
            }
        }

        return parentValidDatabaseList;
    }

    private List<Map<DatabaseIdent, String>> getParentDatabaseList(TransactionContext ctx,
        DatabaseIdent subBranchDatabaseIdent) {
        CatalogIdent subBranchCatalogIdent = StoreConvertor.catalogIdent(subBranchDatabaseIdent.getProjectId(),
            subBranchDatabaseIdent.getCatalogId(), subBranchDatabaseIdent.getRootCatalogId());
        List<CatalogObject> parentCatalogList = catalogStore.getParentBranchCatalog(ctx, subBranchCatalogIdent);
        Iterator<CatalogObject> parentCatalogIterator = parentCatalogList.listIterator();

        List<Map<DatabaseIdent, String>> mapList = new ArrayList<>();
        CatalogIdent subCatalogIdent = subBranchCatalogIdent;
        while (parentCatalogIterator.hasNext()) {
            CatalogObject parentCatalog = parentCatalogIterator.next();

            //get parent branch version
            CatalogObject subBranchCatalog = catalogStore.getCatalogById(ctx, subCatalogIdent);
            if (subBranchCatalog == null) {
                throw new CatalogServerException(ErrorCode.CATALOG_ID_NOT_FOUND, subBranchCatalog.getCatalogId());
            }

            String subBranchVersion = subBranchCatalog.getParentVersion();

            DatabaseIdent parentDatabaseIdent = StoreConvertor.databaseIdent(parentCatalog.getProjectId(),
                parentCatalog.getCatalogId(), subBranchDatabaseIdent.getDatabaseId(),
                parentCatalog.getRootCatalogId());

            Map<DatabaseIdent, String> map = new HashMap<>();
            map.put(parentDatabaseIdent, subBranchVersion);
            mapList.add(map);
            subCatalogIdent = StoreConvertor.catalogIdent(subBranchCatalog.getProjectId(),
                subBranchCatalog.getCatalogId(), subBranchCatalogIdent.getRootCatalogId());
        }

        return mapList;
    }

    private List<Map<DatabaseIdent, String>> getParentDatabaseList(TransactionContext context,
        TableIdent subBranchTableIdent, String version) {
        CatalogIdent subBranchCatalogIdent = StoreConvertor.catalogIdent(subBranchTableIdent.getProjectId(),
            subBranchTableIdent.getCatalogId(), subBranchTableIdent.getRootCatalogId());
        List<CatalogObject> parentCatalogList = catalogStore.getParentBranchCatalog(context, subBranchCatalogIdent);
        if (parentCatalogList.isEmpty()) {
            return null;
        }

        List<Map<DatabaseIdent, String>> mapList = new ArrayList<>();
        TableIdent tableIdent = new TableIdent(subBranchTableIdent);
        DatabaseIdent validDatabaseIdent = null;
        String validSubBranchVersion = null;
        for (CatalogObject parentCatalog : parentCatalogList) {
            CatalogObject subBranchCatalog = catalogStore.getCatalogById(context, subBranchCatalogIdent);
            if (subBranchCatalog == null) {
                throw new CatalogServerException(ErrorCode.CATALOG_ID_NOT_FOUND, subBranchCatalog.getCatalogId());
            }
            String subBranchVersion = subBranchCatalog.getParentVersion();

            DatabaseIdent parentDatabaseIdent = StoreConvertor.databaseIdent(parentCatalog.getProjectId(),
                parentCatalog.getCatalogId(), tableIdent.getDatabaseId(), parentCatalog.getRootCatalogId());

            //version <= subBranchVersion
            if ((version.compareTo(subBranchVersion) < 0) || (version.compareTo(subBranchVersion) == 0)) {
                validDatabaseIdent = parentDatabaseIdent;
                validSubBranchVersion = subBranchVersion;
            } else {

                break;
            }

            subBranchCatalogIdent = StoreConvertor
                .catalogIdent(parentDatabaseIdent.getProjectId(), parentDatabaseIdent.getCatalogId(),
                    parentDatabaseIdent.getRootCatalogId());
            tableIdent = StoreConvertor
                .tableIdent(parentDatabaseIdent, tableIdent.getTableId());

        }

        Map<DatabaseIdent, String> map = new HashMap<>();
        map.put(validDatabaseIdent, validSubBranchVersion);
        mapList.add(map);

        return mapList;
    }

    private Boolean isDropCommit(TableCommitObject tableCommitObject) {
        return tableCommitObject.getDroppedTime() != 0;
    }


}
