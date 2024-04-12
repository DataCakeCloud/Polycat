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
package io.polycat.catalog.common.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import io.polycat.catalog.store.protos.common.DatabaseInfo;
import lombok.Data;

@Data
public class DatabaseHistoryObject {
    private String eventId = "";
    private String name = "";
    private long  createTime;
    private Map<String, String> properties = Collections.emptyMap();
    private String location = "";
    private String description = "";
    private String userId = "";
    private String version = "";
    private long droppedTime;

    public DatabaseHistoryObject() {
        this.properties = new LinkedHashMap<>();
    }

    public DatabaseHistoryObject(DatabaseHistoryObject src) {
        this.eventId = src.getEventId();
        this.name = src.getName();
        this.createTime = src.getCreateTime();
        this.properties = new LinkedHashMap<>();
        this.properties.putAll(src.getProperties());
        this.location = src.getLocation();
        this.description = src.getDescription();
        this.userId = src.getUserId();
        this.version = src.getVersion();
        this.droppedTime = src.getDroppedTime();
    }

    public DatabaseHistoryObject(DatabaseObject databaseObject) {
        this.name = databaseObject.getName();
        this.properties = databaseObject.getProperties();
        this.location = databaseObject.getLocation();
        this.createTime = databaseObject.getCreateTime();
        this.droppedTime = databaseObject.getDroppedTime();
        this.description = databaseObject.getDescription();
        this.userId = databaseObject.getUserId();
    }

    public DatabaseHistoryObject(String dbhId, String databaseName, String version, DatabaseInfo databaseInfo) {
        this.eventId = dbhId;
        this.name = databaseName;
        this.createTime = databaseInfo.getCreateTime();
        this.properties = new LinkedHashMap<>();
        this.properties.putAll(databaseInfo.getPropertiesMap());
        this.location = databaseInfo.getLocation();
        this.description = databaseInfo.getDescription();
        this.userId = databaseInfo.getUserId();
        this.version = version;
        this.droppedTime = databaseInfo.getDroppedTime();
    }



    public void clearDroppedTime() {
        this.droppedTime = 0;
    }
}
