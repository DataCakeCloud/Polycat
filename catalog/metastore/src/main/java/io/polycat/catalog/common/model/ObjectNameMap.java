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
import io.polycat.catalog.common.ObjectType;
import lombok.Data;

@Data
public class ObjectNameMap {
    private String projectId;
    private ObjectType objectType;
    private String upperObjectName;
    private String objectName;
    private String topObjectId;
    private String upperObjectId;
    private String objectId;

    public ObjectNameMap(String projectId, ObjectType objectType, String upperObjectName, String objectName,
        String topObjectId, String upperObjectId, String objectId) {
        this.projectId = projectId;
        this.objectType = objectType;
        this.upperObjectName = upperObjectName;
        this.objectName = objectName;
        this.topObjectId = topObjectId;
        this.upperObjectId = upperObjectId;
        this.objectId = objectId;
    }

    public ObjectNameMap(DatabaseName databaseName, DatabaseIdent databaseIdent) {
        this.projectId = databaseIdent.getProjectId();
        this.objectType = ObjectType.DATABASE;
        this.upperObjectName = "null";
        this.objectName = databaseName.getDatabaseName();
        this.topObjectId = "null";
        this.upperObjectId = databaseIdent.getCatalogId();
        this.objectId = databaseIdent.getDatabaseId();
    }

    public ObjectNameMap(TableName tableName, TableIdent tableIdent) {
        this.projectId = tableIdent.getProjectId();
        this.objectType = ObjectType.TABLE;
        this.upperObjectName = tableName.getDatabaseName();
        this.objectName = tableName.getTableName();
        this.topObjectId = tableIdent.getCatalogId();
        this.upperObjectId = tableIdent.getDatabaseId();
        this.objectId = tableIdent.getTableId();
    }

}
