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

import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogHistoryObject;
import io.polycat.catalog.common.model.Table;

public class ModelConvertor {
    public static final long DEFAULT_CREATE_TIME = 0L;
    public static final String DEFAULT_OWNER = "";
    public static final String DEFAULT_OWNER_TYPE = "";
    public static final String DEFAULT_AUTH_SOURCE_TYPE = "";
    public static final String DEFAULT_ACCOUNT_ID = "";
    public static final long DEFAULT_DROPPED_TIME = 0L;
    public static final String DEFAULT_DESCRIPTION = "";
    public static final String DEFAULT_LOCATION = "";

    public static Table toTableModel(String catalogName, String databaseName, String tableName) {
        Table model = new Table();
        model.setCatalogName(catalogName);
        model.setDatabaseName(databaseName);
        model.setTableName(tableName);
        return model;
    }

    public static Catalog toCatalog(CatalogHistoryObject catalogByVersion) {
        return new Catalog(catalogByVersion.getVersion(),
            catalogByVersion.getName()
            , DEFAULT_CREATE_TIME
            , catalogByVersion.getParentId()
            , catalogByVersion.getParentVersion()
            , DEFAULT_OWNER
            , DEFAULT_OWNER_TYPE
            , DEFAULT_AUTH_SOURCE_TYPE
            , DEFAULT_ACCOUNT_ID
            , DEFAULT_DROPPED_TIME
            , DEFAULT_DESCRIPTION
            , DEFAULT_LOCATION);
    }
}
