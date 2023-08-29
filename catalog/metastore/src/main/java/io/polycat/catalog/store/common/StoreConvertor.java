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
package io.polycat.catalog.store.common;

import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.IndexIdent;
import io.polycat.catalog.common.model.IndexName;

import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableName;

import io.polycat.catalog.common.model.ViewIdent;
import io.polycat.catalog.common.model.ViewName;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.utils.CatalogStringUtils;

public class StoreConvertor {

    public static CatalogIdent catalogIdent(CatalogName catalogName, String catalogId, String rootCatalogId) {
        return new CatalogIdent(catalogName.getProjectId(), catalogId, rootCatalogId);
    }

    public static CatalogIdent catalogIdent(CatalogObject catalog) {
        return new CatalogIdent(catalog.getProjectId(), catalog.getCatalogId(), catalog.getRootCatalogId());
    }

    public static CatalogIdent catalogIdent(String projectId, String catalogId, String rootCatalogId) {
        return new CatalogIdent(projectId, catalogId, rootCatalogId);
    }

    public static DatabaseIdent databaseIdent(CatalogIdent catalogIdent, String databaseId) {
        return new DatabaseIdent(catalogIdent.getProjectId(), catalogIdent.getCatalogId(), databaseId,
            catalogIdent.getRootCatalogId());
    }

    public static DatabaseIdent databaseIdent(String projectId, String catalogId, String databaseId, String rootCatalogId) {
        return new DatabaseIdent(projectId, catalogId, databaseId, rootCatalogId);
    }

    public static DatabaseIdent databaseIdent(TableIdent table) {
        return new DatabaseIdent(table.getProjectId(), table.getCatalogId(), table.getDatabaseId(), table.getRootCatalogId());
    }

    public static DatabaseIdent databaseIdent(IndexIdent indexIdent) {
        return new DatabaseIdent(indexIdent.getProjectId(), indexIdent.getCatalogId(),
            indexIdent.getDatabaseId(), indexIdent.getRootCatalogId());
    }

    public static TableIdent tableIdent(String projectId, String catalogId,
            String databaseId, String tableId, String rootCatalogId) {
        return new TableIdent(projectId, catalogId, databaseId, tableId, rootCatalogId);
    }

    public static TableIdent tableIdent(CatalogIdent catalogIdent, String databaseId, String tableId) {
        return new TableIdent(catalogIdent.getProjectId(), catalogIdent.getCatalogId(), databaseId, tableId,
            catalogIdent.getRootCatalogId());
    }

    public static TableIdent tableIdent(DatabaseIdent databaseIdent, String tableId) {
        return new TableIdent(databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getDatabaseId(),
            tableId, databaseIdent.getRootCatalogId());
    }

    public static ViewIdent viewIdent(String projectId, String catalogId, String databaseId, String viewId) {
        return new ViewIdent(projectId, catalogId, databaseId, viewId);
    }

    public static CatalogName catalogName(String projectId, String catalogName) {
        return new CatalogName(normalizeIdentifier(projectId), normalizeIdentifier(catalogName));
    }

    public static CatalogName catalogName(DatabaseName databaseName) {
        return new CatalogName(normalizeIdentifier(databaseName.getProjectId()), normalizeIdentifier(databaseName.getCatalogName()));
    }

    public static DatabaseName databaseName(String projectId, String catalogName, String databaseName) {
        return new DatabaseName(normalizeIdentifier(projectId), normalizeIdentifier(catalogName), normalizeIdentifier(databaseName));
    }

    public static DatabaseName databaseName(CatalogName catalogName, String databaseName) {
        return new DatabaseName(normalizeIdentifier(catalogName.getProjectId()), normalizeIdentifier(catalogName.getCatalogName()), normalizeIdentifier(databaseName));
    }

    public static DatabaseName databaseName(TableName tableName) {
        return new DatabaseName(tableName.getProjectId(), tableName.getCatalogName(), tableName.getDatabaseName());
    }

    public static TableName tableName(String projectId, String catalogName, String databaseName, String tableName) {
        return new TableName(normalizeIdentifier(projectId), normalizeIdentifier(catalogName), normalizeIdentifier(databaseName), normalizeIdentifier(tableName));
    }

    public static TableName tableName(DatabaseName dbName, String tblName) {
        return new TableName(normalizeIdentifier(dbName.getProjectId()), normalizeIdentifier(dbName.getCatalogName()), normalizeIdentifier(dbName.getDatabaseName()), normalizeIdentifier(tblName));
    }

    private static String normalizeIdentifier(String identifier) {
        return CatalogStringUtils.normalizeIdentifier(identifier);
    }

    public static ViewName viewName(String projectId, String catalogName, String databaseName, String viewName) {
        return new ViewName(normalizeIdentifier(projectId), normalizeIdentifier(catalogName), normalizeIdentifier(databaseName), normalizeIdentifier(viewName));
    }

    public static IndexName indexName(String projectId, String catalogName, String databaseName,
                                      String indexName) {
        return new IndexName(normalizeIdentifier(projectId), normalizeIdentifier(catalogName), normalizeIdentifier(databaseName), normalizeIdentifier(indexName));
    }

    public static IndexIdent indexIdent(String projectId, String catalogId,
        String databaseId, String indexId) {
        return new IndexIdent(normalizeIdentifier(projectId), catalogId, databaseId, indexId);
    }



}
