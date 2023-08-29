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
import io.polycat.catalog.common.model.DatabaseHistoryObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.CatalogStore;
import io.polycat.catalog.store.api.DatabaseStore;
import io.polycat.catalog.store.common.StoreConvertor;


public class ParentBranchCatalogIterator implements Iterator<Map<CatalogIdent, String>> {
    private final static CatalogStore catalogStore = ParentBranchCatalogIteratorHelper.getCatalogStore();
    private final static DatabaseStore databaseStore = ParentBranchCatalogIteratorHelper.getDatabaseStore();


    final Iterator<Map<CatalogIdent, String>> parentCatalogIterator;
    Map<CatalogIdent, String> nextMap = Collections.emptyMap();

    public ParentBranchCatalogIterator(TransactionContext ctx, DatabaseIdent subBranchDatabaseIdent, Boolean dropped) {
        parentCatalogIterator = getParentCatalogList(ctx, subBranchDatabaseIdent, dropped).listIterator();
    }

    public ParentBranchCatalogIterator(TransactionContext ctx, DatabaseIdent subBranchDatabaseIdent) {
        parentCatalogIterator = getParentCatalogList(ctx, subBranchDatabaseIdent).listIterator();
    }

    public ParentBranchCatalogIterator(TransactionContext ctx, CatalogIdent subBranchCatalogIdent) {
        parentCatalogIterator = getParentCatalogList(ctx, subBranchCatalogIdent).listIterator();
    }

    public ParentBranchCatalogIterator(TransactionContext ctx, DatabaseIdent subBranchDatabaseIdent, String version) {
        parentCatalogIterator = getParentCatalogList(ctx, subBranchDatabaseIdent, version).listIterator();
    }

    private List<Map<CatalogIdent, String>> getDroppedParentCatalogList(TransactionContext ctx,
        DatabaseIdent subBranchDatabaseIdent, List<Map<CatalogIdent, String>> parentCatalogList) {
        if (parentCatalogList.isEmpty()) {
            return Collections.emptyList();
        }

        List<Map<CatalogIdent, String>> parentDroppedCatalogList = new ArrayList<>();
        Map<CatalogIdent, String> map = parentCatalogList.get(0);
        Iterator<Map.Entry<CatalogIdent, String>> iterator = map.entrySet().iterator();
        Map.Entry<CatalogIdent, String> entry = iterator.next();
        CatalogIdent parentCatalogIdent = entry.getKey();
        String subBranchVersion = entry.getValue();
        DatabaseIdent parentDatabaseIdent = StoreConvertor.databaseIdent(parentCatalogIdent,
            subBranchDatabaseIdent.getDatabaseId());
        Optional<DatabaseHistoryObject> optional = databaseStore.getLatestDatabaseHistory(ctx, parentDatabaseIdent,
            subBranchVersion);
        if (optional.isPresent()) {
            if (DatabaseHistoryHelper.isDropped(optional.get())) {
                parentDroppedCatalogList.add(map);
            }
        }

        return parentDroppedCatalogList;
    }

    private List<Map<CatalogIdent, String>> getValidParentCatalogList(TransactionContext ctx,
        DatabaseIdent subBranchDatabaseIdent, List<Map<CatalogIdent, String>> parentCatalogList) {
        List<Map<CatalogIdent, String>> parentValidCatalogList = new ArrayList<>();
        for (Map<CatalogIdent, String> map : parentCatalogList) {
            Iterator<Map.Entry<CatalogIdent, String>> iterator = map.entrySet().iterator();
            Map.Entry<CatalogIdent, String> entry = iterator.next();
            CatalogIdent parentCatalogIdent = entry.getKey();
            String subBranchVersion = entry.getValue();
            DatabaseIdent parentDatabaseIdent = StoreConvertor.databaseIdent(parentCatalogIdent,
                subBranchDatabaseIdent.getDatabaseId());
            Optional<DatabaseHistoryObject> optional = databaseStore.getLatestDatabaseHistory(ctx, parentDatabaseIdent,
                subBranchVersion);
            if (optional.isPresent()) {
                if (DatabaseHistoryHelper.isDropped(optional.get())) {
                    break;
                }
                parentValidCatalogList.add(map);
            }
        }

        return parentValidCatalogList;
    }

    private List<Map<CatalogIdent, String>> getParentCatalogList(TransactionContext ctx,
        DatabaseIdent subBranchDatabaseIdent, Boolean dropped) {
        CatalogIdent subBranchCatalogIdent = StoreConvertor.catalogIdent(subBranchDatabaseIdent.getProjectId(),
            subBranchDatabaseIdent.getCatalogId(), subBranchDatabaseIdent.getRootCatalogId());
        List<Map<CatalogIdent, String>> parentCatalogList = getParentCatalogList(ctx, subBranchCatalogIdent);

        //check most recent parent branch is dropped
        if (dropped) {
            return getDroppedParentCatalogList(ctx, subBranchDatabaseIdent, parentCatalogList);
        }

        return getValidParentCatalogList(ctx, subBranchDatabaseIdent, parentCatalogList);
    }

    private List<Map<CatalogIdent, String>> getParentCatalogList(TransactionContext ctx,
        DatabaseIdent subBranchDatabaseIdent) {
        CatalogIdent subBranchCatalogIdent = StoreConvertor.catalogIdent(subBranchDatabaseIdent.getProjectId(),
            subBranchDatabaseIdent.getCatalogId(), subBranchDatabaseIdent.getRootCatalogId());
        List<Map<CatalogIdent, String>> parentCatalogList = getParentCatalogList(ctx, subBranchCatalogIdent);

        List<Map<CatalogIdent, String>> parentValidCatalogList = new ArrayList<>();
        for (Map<CatalogIdent, String> map : parentCatalogList) {
            Iterator<Map.Entry<CatalogIdent, String>> iterator = map.entrySet().iterator();
            Map.Entry<CatalogIdent, String> entry = iterator.next();
            CatalogIdent parentCatalogIdent = entry.getKey();
            String subBranchVersion = entry.getValue();

            DatabaseIdent parentDatabaseIdent = StoreConvertor.databaseIdent(parentCatalogIdent,
                subBranchDatabaseIdent.getDatabaseId());
            Optional<DatabaseHistoryObject> optional = databaseStore.getLatestDatabaseHistory(ctx, parentDatabaseIdent,
                subBranchVersion);
            if (optional.isPresent()) {
                parentValidCatalogList.add(map);
            }
        }

        return parentValidCatalogList;
    }

    private List<Map<CatalogIdent, String>> getParentCatalogList(TransactionContext ctx,
        CatalogIdent subBranchCatalogIdent) {
        List<CatalogObject> parentCatalogList = catalogStore.getParentBranchCatalog(ctx, subBranchCatalogIdent);
        Iterator<CatalogObject> parentCatalogIterator = parentCatalogList.listIterator();

        List<Map<CatalogIdent, String>> mapList = new ArrayList<>();
        CatalogIdent subCatalogIdent = subBranchCatalogIdent;
        while (parentCatalogIterator.hasNext()) {
            CatalogObject parentCatalog = parentCatalogIterator.next();

            //get parent branch version
            CatalogObject subBranchCatalog = CatalogObjectHelper.getCatalogObject(subCatalogIdent);
            String subBranchVersion = subBranchCatalog.getParentVersion();

            CatalogIdent parentCatalogIdent = StoreConvertor.catalogIdent(parentCatalog.getProjectId(),
                parentCatalog.getCatalogId(), parentCatalog.getRootCatalogId());

            Map<CatalogIdent, String> map = new HashMap<>();
            map.put(parentCatalogIdent, subBranchVersion);
            mapList.add(map);
            subCatalogIdent = parentCatalogIdent;
        }

        return mapList;
    }

    private List<Map<CatalogIdent, String>> getParentCatalogList(TransactionContext context,
        DatabaseIdent subBranchDatabaseIdent, String baseVersion) {
        CatalogIdent subBranchCatalogIdent = StoreConvertor.catalogIdent(subBranchDatabaseIdent.getProjectId(),
            subBranchDatabaseIdent.getCatalogId(), subBranchDatabaseIdent.getRootCatalogId());
        List<CatalogObject> parentCatalogList = catalogStore.getParentBranchCatalog(context, subBranchCatalogIdent);
        if (parentCatalogList.isEmpty()) {
            return null;
        }

        String version = baseVersion;
        List<Map<CatalogIdent, String>> mapList = new ArrayList<>();
        CatalogIdent validCatalogIdent = null;
        String validSubBranchVersion = null;
        for (CatalogObject parentCatalog : parentCatalogList) {
            CatalogObject subBranchCatalog = catalogStore.getCatalogById(context, subBranchCatalogIdent);
            if (subBranchCatalog == null) {
                throw new CatalogServerException(ErrorCode.CATALOG_ID_NOT_FOUND, subBranchCatalog.getCatalogId());
            }
            String subBranchVersion = subBranchCatalog.getParentVersion();

            //version <= subBranchVersion
            if ((version.compareTo(subBranchVersion) < 0) || (version.compareTo(subBranchVersion) == 0)) {
                validCatalogIdent = StoreConvertor.catalogIdent(parentCatalog.getProjectId(),
                    parentCatalog.getCatalogId(), parentCatalog.getRootCatalogId());
                validSubBranchVersion = subBranchVersion;
            } else {
                break;
            }

            subBranchCatalogIdent = StoreConvertor.catalogIdent(parentCatalog.getProjectId(),
                parentCatalog.getCatalogId(), parentCatalog.getRootCatalogId());
        }


        Map<CatalogIdent, String> map = new HashMap<>();
        map.put(validCatalogIdent, validSubBranchVersion);
        mapList.add(map);

        return mapList;
    }


    public boolean hasNext() {
        return parentCatalogIterator.hasNext();
    }

    public Map<CatalogIdent, String> next() {
        return parentCatalogIterator.next();
    }

    public CatalogIdent nextCatalogIdent() {
        nextMap = parentCatalogIterator.next();
        Iterator<CatalogIdent> iterator = nextMap.keySet().iterator();
        return iterator.next();
    }

    public String nextBranchVersion(CatalogIdent catalogIdent) {
        return nextMap.get(catalogIdent);
    }

    public void remove() {
        throw new UnsupportedOperationException("This Iterator does not implement the remove method");
    }


}
