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
package io.polycat.catalog.common.utils;

import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.stats.ColumnStatisticsDesc;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.model.DatabaseName;

import java.util.Locale;

public class CatalogStringUtils {

    public static String normalizeIdentifier(String identifier) {
        if (identifier == null) {
            return identifier;
        }
        return identifier.trim().toLowerCase(Locale.ROOT);
    }

    public static void catalogInnerNormalize(CatalogInnerObject catalogInnerObject) {
        if (catalogInnerObject == null) {
            return;
        }
        catalogInnerObject.setProjectId(normalizeIdentifier(catalogInnerObject.getProjectId()));
        catalogInnerObject.setObjectName(normalizeIdentifier(catalogInnerObject.getObjectName()));
        catalogInnerObject.setCatalogName(normalizeIdentifier(catalogInnerObject.getCatalogName()));
        catalogInnerObject.setDatabaseName(normalizeIdentifier(catalogInnerObject.getDatabaseName()));
    }

    public static void tableNameNormalize(TableName tableName) {
        if (tableName == null) {
            return;
        }
        tableName.setProjectId(normalizeIdentifier(tableName.getProjectId()));
        tableName.setCatalogName(normalizeIdentifier(tableName.getCatalogName()));
        tableName.setDatabaseName(normalizeIdentifier(tableName.getDatabaseName()));
        tableName.setTableName(normalizeIdentifier(tableName.getTableName()));
    }

    public static void databaseNameNormalize(DatabaseName databaseName) {
        if (databaseName == null) {
            return;
        }
        databaseName.setProjectId(normalizeIdentifier(databaseName.getProjectId()));
        databaseName.setCatalogName(normalizeIdentifier(databaseName.getCatalogName()));
        databaseName.setDatabaseName(normalizeIdentifier(databaseName.getDatabaseName()));
    }

    public static void tableInputNormalize(TableInput tableInput) {
        if (tableInput == null) {
            return;
        }
        tableInput.setCatalogName(normalizeIdentifier(tableInput.getCatalogName()));
        tableInput.setDatabaseName(normalizeIdentifier(tableInput.getDatabaseName()));
        tableInput.setTableName(normalizeIdentifier(tableInput.getTableName()));
    }

    public static void columnStatisticsDescNormalize(ColumnStatisticsDesc statisticsDesc) {
        if (statisticsDesc == null) {
            return;
        }
        statisticsDesc.setCatName(normalizeIdentifier(statisticsDesc.getCatName()));
        statisticsDesc.setDbName(normalizeIdentifier(statisticsDesc.getDbName()));
        statisticsDesc.setTableName(normalizeIdentifier(statisticsDesc.getTableName()));
    }
}
