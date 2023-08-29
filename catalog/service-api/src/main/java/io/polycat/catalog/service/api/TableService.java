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

import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.model.stats.ColumnStatistics;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import io.polycat.catalog.common.plugin.request.input.ColumnChangeInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;

import java.util.List;
import java.util.Map;

public interface TableService {

    /* Interfaces for connecting to the Controller layer  */

    void createTable(DatabaseName databaseName, TableInput tableInput);

    Table getTableByName(TableName tableName);

    TraverseCursorResult<List<String>> getTableNames(DatabaseName databaseName, String tableType,
        Integer maxResults, String pageToken, String filter);

    List<Table> getTableObjectsByName(DatabaseName databaseName, List<String> tableNames);

    TraverseCursorResult<List<String>> listTableNamesByFilter(DatabaseName databaseName, String filter, Integer maxNum, String pageToken);

    void dropTable(TableName tableName, boolean deleteData, boolean ignoreUnknownTable, boolean ifPurge);

    void undropTable(TableName tableName, String tableId, String newName);

    void purgeTable(TableName tableName, String tableId);

    void restoreTable(TableName tableName, String version);

    void alterTable(TableName tableName, TableInput tableInput, Map<String,String> alterTableParams);

    void truncateTable(TableName tableName);

    void alterColumn(TableName tableNameParam, ColumnChangeInput columnChangeInput);

    void setProperties(TableName tableName, Map<String, String> properties);

    void unsetProperties(TableName tableName, List<String> propertyKeys);

    TableCommit getLatestTableCommit(TableName tableName);

    TraverseCursorResult<List<TableCommit>> listTableCommits(TableName tableName, int maxResults, String pageToken);

    TraverseCursorResult<List<Table>> listTable(DatabaseName databaseName, Boolean includeDeleted, Integer maxResults,
        String pageToken, String filter);

    PagedList<String> listTableNames(DatabaseName databaseFullName, Boolean includeDrop, Integer maxResults, String pageToken, String filter);

    ColumnStatisticsObj[] getTableColumnStatistics(TableName tableName, List<String> colNames);

    void updateTableColumnStatistics(String projectId, ColumnStatistics stats);

    boolean deleteTableColumnStatistics(TableName tableName, String colName);

    // table constraints
    void addConstraint(String projectId, List<Constraint> constraints);

    void addForeignKey(String projectId, List<ForeignKey> foreignKeys);

    void addPrimaryKey(String projectId, List<PrimaryKey> primaryKeys);

    void dropConstraint(TableName tableName, String cstr_name);

    List<Constraint> getConstraints(TableName tableName, ConstraintType type);

    List<ForeignKey> getForeignKeys(TableName parentTableName, TableName foreignTableName);

    List<PrimaryKey> getPrimaryKeys(TableName tableName);
    // table constraints end
}
