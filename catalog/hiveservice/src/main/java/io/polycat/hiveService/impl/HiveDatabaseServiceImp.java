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
package io.polycat.hiveService.impl;

import java.util.List;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseHistory;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.TableBrief;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.service.api.DatabaseService;
import io.polycat.hiveService.common.HiveServiceHelper;
import io.polycat.hiveService.data.accesser.HiveDataAccessor;
import io.polycat.hiveService.data.accesser.PolyCatDataAccessor;
import io.polycat.hiveService.hive.HiveMetaStoreClientUtil;

import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HiveDatabaseServiceImp implements DatabaseService {

    @Override
    public Database createDatabase(CatalogName catalogName, DatabaseInput dataBaseInput) {
        org.apache.hadoop.hive.metastore.api.Database database;
        database = HiveDataAccessor.toDatabase(dataBaseInput);
        HiveServiceHelper.HiveExceptionHandler(() -> HiveMetaStoreClientUtil.getHMSClient().createDatabase(database));

        return PolyCatDataAccessor.toDatabase(database);
    }

    @Override
    public String dropDatabase(DatabaseName databaseName, Boolean ignoreUnknownDatabase, Boolean deleteData,
        String cascade) {
        return HiveServiceHelper.HiveExceptionHandler(() -> {
            HiveMetaStoreClientUtil.getHMSClient()
                .dropDatabase(databaseName.getCatalogName(), databaseName.getDatabaseName(), deleteData,
                    ignoreUnknownDatabase, Boolean.parseBoolean(cascade));
            return databaseName.getDatabaseName();
        });
    }

    @Override
    public void undropDatabase(DatabaseName databaseName, String databaseId, String rename) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "undropDatabase");
    }

    @Override
    public void alterDatabase(DatabaseName currentDBFullName, DatabaseInput dataBaseInput) {
        org.apache.hadoop.hive.metastore.api.Database hiveDatabase = HiveDataAccessor.toDatabase(dataBaseInput);
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient()
                .alterDatabase(currentDBFullName.getCatalogName(), currentDBFullName.getDatabaseName(),
                    hiveDatabase));
    }

    @Override
    public void renameDatabase(DatabaseName currentDBFullName, String newName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "renameDatabase");
    }

    @Override
    public TraverseCursorResult<List<Database>> listDatabases(CatalogName catalogFullName, boolean includeDrop,
        Integer maximumToScan, String pageToken, String filter) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "listDatabases");
    }

    @Override
    public TraverseCursorResult<List<String>> getDatabaseNames(CatalogName catalogFullName, boolean includeDrop,
        Integer maximumToScan, String pageToken, String filter) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> new TraverseCursorResult<>(
                HiveMetaStoreClientUtil.getHMSClient().getDatabases(catalogFullName.getCatalogName(), filter), null));
    }

    @Override
    public Database getDatabaseByName(DatabaseName databaseName) {
        org.apache.hadoop.hive.metastore.api.Database hiveDatabase = HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient()
                .getDatabase(databaseName.getCatalogName(), databaseName.getDatabaseName()));
        return PolyCatDataAccessor.toDatabase(hiveDatabase);
    }

    @Override
    public TraverseCursorResult<TableBrief[]> getTableFuzzy(CatalogName catalogName, String databasePattern,
        String tablePattern, List<String> tableTypes) {
        List<TableMeta> tblMetas = HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient()
                .getTableMeta(catalogName.getCatalogName(), databasePattern, tablePattern, tableTypes));
        return new TraverseCursorResult(
            tblMetas.stream().map(x -> PolyCatDataAccessor.toTableBrief(catalogName.getProjectId(), x))
                .toArray(TableBrief[]::new), null);
    }

    @Override
    public List<DatabaseHistory> getDatabaseHistory(DatabaseName databaseName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getDatabaseHistory");
    }

    @Override
    public DatabaseHistory getDatabaseByVersion(DatabaseName databaseName, String version) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getDatabaseByVersion");
    }

    @Override
    public TraverseCursorResult<List<DatabaseHistory>> listDatabaseHistory(DatabaseName databaseName, int maxResults,
        String pageToken) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "listDatabaseHistory");
    }
}
