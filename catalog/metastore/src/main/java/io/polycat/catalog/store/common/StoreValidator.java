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

import java.util.Objects;


import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseName;


import org.apache.commons.lang3.StringUtils;

public class StoreValidator {

    public static void validateInput(String input, String inputName) {
        if (StringUtils.isBlank(input)) {
            throw new IllegalArgumentException("invalid " + inputName);
        }
    }

    public static void validate(String tableName) {
        Objects.requireNonNull(tableName);
        validateInput(tableName, "table name");
    }

    public static void validate(CatalogObject catalog) {
        Objects.requireNonNull(catalog);
        validateInput(catalog.getName(), "catalog name");
    }



    public static void requireDBNameNotNull(DatabaseName databaseName) {
        Objects.requireNonNull(databaseName);
        validateInput(databaseName.getProjectId(), "project id");
        validateInput(databaseName.getCatalogName(), "catalog name");
        validateInput(databaseName.getDatabaseName(), "database name");
    }

    public static void requireDBIdentNotNull(DatabaseIdent databaseIdent) {
        Objects.requireNonNull(databaseIdent);
        validateInput(databaseIdent.getProjectId(), "project id");
        validateInput(databaseIdent.getCatalogId(), "catalog id");
        validateInput(databaseIdent.getDatabaseId(), "database id");
    }

    public static void requireCatalogIdentNotNull(CatalogIdent catalogIdent) {
        Objects.requireNonNull(catalogIdent);
        validateInput(catalogIdent.getProjectId(), "project id");
        validateInput(catalogIdent.getCatalogId(), "catalog id");
    }

    public static void requireCatalogNameNotNull(CatalogName catalogName) {
        Objects.requireNonNull(catalogName);
        validateInput(catalogName.getProjectId(), "project id");
        validateInput(catalogName.getCatalogName(), "catalog name");
    }

}
