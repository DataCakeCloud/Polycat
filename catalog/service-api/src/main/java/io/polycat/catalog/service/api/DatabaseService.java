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
package io.polycat.catalog.service.api;

import java.util.List;

import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseHistory;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.TableBrief;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;



/**
 * interface: DatabaseService
 */
public interface DatabaseService {
    /* Interfaces for connecting to the Controller layer */

    Database createDatabase(CatalogName catalogName, DatabaseInput dataBaseInput);

    default String dropDatabase(DatabaseName databaseName, String cascade) {
        return dropDatabase(databaseName, false, false, cascade);
    }

    String dropDatabase(DatabaseName databaseName, Boolean ignoreUnknownDatabase, Boolean deleteData ,String cascade);

    /**
     * undrop DB
     *
     * @param databaseName CatalogName
     * @param databaseId database id
     * @param rename Database New Name
     */
    void undropDatabase(DatabaseName databaseName, String databaseId, String rename);

    /**
     * alter DB
     *
     * @param currentDBFullName    Database full name
     * @param dataBaseInput dataBaseDTO
     * @return new database record
     */
    void alterDatabase(DatabaseName currentDBFullName, DatabaseInput dataBaseInput);

    /**
     * rename DB
     *
     * @param currentDBFullName    Database full name
     * @param newName new database name
     * @return new database record
     */
    void renameDatabase(DatabaseName currentDBFullName, String newName);

    /**
     * list DBs
     *
     * @param catalogFullName  CatalogIdentifier
     * @param includeDrop           "true" to see DBs including the deleted ones
     * @param maximumToScan number of DBs limits in one response
     * @param pageToken     pageToken is the next consecutive key to begin with in the LIST-Request
     * @param filter        filter expression
     * @return List of Database
     */
    TraverseCursorResult<List<Database>> listDatabases(CatalogName catalogFullName, boolean includeDrop,
            Integer maximumToScan, String pageToken, String filter);



    /**
     * get DBs Names
     *
     * @param catalogFullName  CatalogIdentifier
     * @param includeDrop   "true" to see DBs including the deleted ones
     * @param maximumToScan number of DBs limits in one response
     * @param pageToken     pageToken is the next consecutive key to begin with in the LIST-Request
     * @param filter        filter expression
     * @return List of DatabaseName Strings
     */
    TraverseCursorResult<List<String>> getDatabaseNames(CatalogName catalogFullName, boolean includeDrop,
        Integer maximumToScan, String pageToken, String filter);



    /**
     * get the detail info of a DB corresponding to the database Name
     *
     * @param databaseName DatabaseName
     * @return Database Database
     */
    Database getDatabaseByName(DatabaseName databaseName);


    /* Interfaces of the internal Service layer */



    /*DatabaseHistoryRecord getDatabaseByVersion(DatabaseIdent databaseIdent, byte[] version);

    List<DatabaseHistoryRecord> getDatabaseHistory(DatabaseIdent databaseIdent);

    List<DatabaseInfo> listDatabases(FDBRecordContext context, CatalogIdent catalogIdent,
        CatalogName catalogName, Tuple pattern);

    DatabaseIdent getDatabaseIdent(FDBRecordContext context, DatabaseName databaseName);

    DatabaseRecord getDatabaseById(FDBRecordContext context, DatabaseIdent databaseIdent);

    String getDatabaseNameById(FDBRecordContext context, DatabaseIdent databaseIdent, Boolean includeDropped);

    DatabaseIdent getDroppedDatabaseIdent(DatabaseName databaseName);*/

     TraverseCursorResult<TableBrief[]> getTableFuzzy(CatalogName catalogName, String databasePattern, String tablePattern,
        List<String> tableTypes);

    List<DatabaseHistory> getDatabaseHistory(DatabaseName databaseName);

    DatabaseHistory getDatabaseByVersion(DatabaseName databaseName, String version);

    TraverseCursorResult<List<DatabaseHistory>> listDatabaseHistory(DatabaseName databaseName,
        int maxResults, String pageToken);
}
