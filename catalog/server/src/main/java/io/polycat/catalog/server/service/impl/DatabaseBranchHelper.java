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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.common.model.DatabaseHistoryObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.DatabaseObject;
import io.polycat.catalog.common.model.TableObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.DatabaseStore;
import io.polycat.catalog.store.api.UserPrivilegeStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class DatabaseBranchHelper {
    private static DatabaseStore databaseStore;
    private static UserPrivilegeStore userPrivilegeStore;


    @Autowired
    public void setDatabaseStore(DatabaseStore databaseStore) {
        this.databaseStore = databaseStore;
    }

    @Autowired
    public void setUserPrivilegeStore(UserPrivilegeStore userPrivilegeStore) {
        this.userPrivilegeStore = userPrivilegeStore;
    }

    public static void mergeNewDatabase(TransactionContext context, String userId, DatabaseIdent srcDatabaseIdent,
        CatalogObject destCatalogRecord) throws CatalogServerException {
        List<TableObject> tableRecordList = TableObjectHelper.listTables(context, srcDatabaseIdent, false);

        // merge cur database obj
        DatabaseObject srcDatabaseRecord = DatabaseObjectHelper.getDatabaseObject(context, srcDatabaseIdent);
        DatabaseObject destDatabaseRecord = new DatabaseObject(srcDatabaseRecord);
        destDatabaseRecord.setUserId(userId);

        DatabaseIdent destDatabaseIdent = new DatabaseIdent(srcDatabaseIdent.getProjectId(),
            destCatalogRecord.getCatalogId(), srcDatabaseIdent.getDatabaseId(), srcDatabaseIdent.getRootCatalogId());

        DatabaseName dstDatabaseName = new DatabaseName(destDatabaseIdent.getProjectId(), destCatalogRecord.getName(),
            destDatabaseRecord.getName());

        addDatabaseObjectRecord(context, destDatabaseIdent, dstDatabaseName, destDatabaseRecord);

        // merge table obj
        for (TableObject tableRecord : tableRecordList) {
            TableBranchHelper.mergeNewTable(context, userId, tableRecord, destDatabaseIdent);
        }
    }

    public static void mergeDatabase(TransactionContext context, String userId, DatabaseIdent srcDatabaseIdent,
        DatabaseIdent destDatabaseIdent, String sameMaxVersion) throws CatalogServerException {
        List<TableObject> srcTableRecordList = TableObjectHelper.listTables(context, srcDatabaseIdent, false);
        List<TableObject> destTableRecordList = TableObjectHelper.listTables(context, destDatabaseIdent, false);
        Map<String, TableObject> destTableRecordMap = destTableRecordList.stream().collect(
            Collectors.toMap(TableObject::getTableId, Function.identity()));

        for (TableObject tableRecord : srcTableRecordList) {
            TableObject destTableRecord = destTableRecordMap.get(tableRecord.getTableId());
            if (destTableRecord != null) {
                TableBranchHelper.mergeTable(context, tableRecord, destTableRecord, sameMaxVersion);
            } else {
                TableBranchHelper.mergeNewTable(context, userId, tableRecord, destDatabaseIdent);
            }
        }
    }

    private static void addDatabaseObjectRecord(TransactionContext ctx, DatabaseIdent databaseIdent,
        DatabaseName databaseName, DatabaseObject databaseObject)
        throws CatalogServerException {
        // check if database name already exists in ObjectName Store subspace
        DatabaseObjectHelper.throwIfDatabaseNameExist(ctx, databaseName, databaseIdent);

        // 1. insert a database name Record into ObjectName Store
        DatabaseObject newDatabaseObject = new DatabaseObject(databaseObject);
        newDatabaseObject.setName(databaseObject.getName());
        databaseStore.upsertDatabase(ctx, databaseIdent, newDatabaseObject);

        String version = VersionManagerHelper.getNextVersion(ctx, databaseIdent.getProjectId(), databaseIdent.getRootCatalogId());

        // 3. insert into catalog history Store
        databaseStore
            .insertDatabaseHistory(ctx, new DatabaseHistoryObject(databaseObject), databaseIdent, false, version);

        // 4. insert user privilege table
        userPrivilegeStore.insertUserPrivilege(ctx, databaseIdent.getProjectId(), databaseObject.getUserId(),
            ObjectType.DATABASE.name(), databaseIdent.getDatabaseId(), true, 0);

        return;
    }

}
