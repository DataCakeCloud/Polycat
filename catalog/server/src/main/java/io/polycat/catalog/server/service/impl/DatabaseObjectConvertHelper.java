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

import java.text.SimpleDateFormat;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseObject;

public class DatabaseObjectConvertHelper {
    private static final String dateFormat = "yyyy-MM-dd HH:mm:ss";
    private static final SimpleDateFormat SDF = new SimpleDateFormat(dateFormat);

    public static Database toDatabase(String catalogName, DatabaseObject databaseObject) {
        Database database = new Database();
        database.setCatalogName(catalogName);
        database.setDatabaseId(databaseObject.getDatabaseId());
        database.setDatabaseName(databaseObject.getName());
        database.setLocationUri(databaseObject.getLocation());
        database.setDescription(databaseObject.getDescription());
        database.setOwner(databaseObject.getUserId());
        database.setOwnerType(databaseObject.getOwnerType());
        database.setParameters(databaseObject.getProperties());
        database.setCreateTime(databaseObject.getCreateTime());
        database.setDroppedTime(databaseObject.getDroppedTime());
        // version
        // authSourceType
        // accountId
        // ownerType
        return database;
    }

}
