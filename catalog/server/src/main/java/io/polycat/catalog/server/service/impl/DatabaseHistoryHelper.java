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

import java.util.Optional;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.DatabaseHistoryObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.DatabaseStore;
import io.polycat.catalog.store.common.StoreConvertor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class DatabaseHistoryHelper {
    private static DatabaseStore databaseStore;

    @Autowired
    public void setDatabaseStore(DatabaseStore databaseStore) {
        this.databaseStore = databaseStore;
    }

    public static boolean checkBranchDatabaseIdValid(TransactionContext context, DatabaseIdent databaseIdent,
        String subBranchVersion) throws CatalogServerException {
        Optional<DatabaseHistoryObject> optional = databaseStore.getLatestDatabaseHistory(context,
            databaseIdent, subBranchVersion);
        return optional.isPresent() && (!DatabaseHistoryHelper.isDropped(optional.get()));
    }

    public static boolean isDropped(DatabaseHistoryObject databaseHistoryObject) {
        return databaseHistoryObject.getDroppedTime() > 0;
    }

    public static Optional<DatabaseHistoryObject> getDatabaseHistory(TransactionContext ctx, DatabaseIdent databaseIdent,
        String version) {
        Optional<DatabaseHistoryObject> optional = databaseStore.getDatabaseHistory(ctx, databaseIdent, version);
        if (!optional.isPresent()) {
            optional = getSubBranchFakeDatabaseHistory(ctx, databaseIdent, version);
        }

        return optional;
    }

    public static Optional<DatabaseHistoryObject> getLatestDatabaseHistory(TransactionContext ctx,
        DatabaseIdent databaseIdent,
        String latestVersion) {
        Optional<DatabaseHistoryObject> databaseHistory;
        databaseHistory = databaseStore.getLatestDatabaseHistory(ctx, databaseIdent, latestVersion);
        if (!databaseHistory.isPresent()) {
            databaseHistory = getLatestSubBranchFakeDatabaseHistory(ctx, databaseIdent, latestVersion,
                Optional.empty());
            if (!databaseHistory.isPresent()) {
                return Optional.empty();
            }
        }

        return databaseHistory;
    }

    private static Optional<DatabaseHistoryObject> getSubBranchFakeDatabaseHistory(TransactionContext context,
        DatabaseIdent subBranchDatabaseIdent, String version) {
        CatalogIdent parentCatalogIdent;

        ParentBranchCatalogIterator parentBranchDatabaseIterator = new ParentBranchCatalogIterator(context, subBranchDatabaseIdent, version);
        if (parentBranchDatabaseIterator.hasNext()) {
            parentCatalogIdent = parentBranchDatabaseIterator.nextCatalogIdent();
        } else {
            return Optional.empty();
        }

        DatabaseIdent parentDatabaseIdent = StoreConvertor.databaseIdent(parentCatalogIdent,
            subBranchDatabaseIdent.getDatabaseId());
        DatabaseHistoryObject databaseHistory = databaseStore.getDatabaseHistory(context, parentDatabaseIdent, version).get();

        return Optional.of(databaseHistory);
    }

    private static Optional<DatabaseHistoryObject> getLatestSubBranchFakeDatabaseHistory(TransactionContext context,
        DatabaseIdent subBranchDatabaseIdent, String version, Optional<Boolean> dropped) {
        ParentBranchCatalogIterator parentCatalogIdentIterator;
        if (dropped.isPresent()) {
            parentCatalogIdentIterator = new ParentBranchCatalogIterator(context, subBranchDatabaseIdent, dropped.get());
        } else {
            parentCatalogIdentIterator = new ParentBranchCatalogIterator(context, subBranchDatabaseIdent);
        }

        while (parentCatalogIdentIterator.hasNext()) {
            CatalogIdent parentCatalogIdent = parentCatalogIdentIterator.nextCatalogIdent();
            String subBranchVersion = parentCatalogIdentIterator.nextBranchVersion(parentCatalogIdent);

            DatabaseIdent parentDatabaseIdent = StoreConvertor.databaseIdent(parentCatalogIdent,
                subBranchDatabaseIdent.getDatabaseId());

            String baseVersionStamp = version.compareTo(subBranchVersion) < 0 ? version : subBranchVersion;
            Optional<DatabaseHistoryObject> optional = databaseStore.getLatestDatabaseHistory(context, parentDatabaseIdent,
                baseVersionStamp);
            if (optional.isPresent()) {
                return optional;
            }
        }

        return Optional.empty();
    }
}
