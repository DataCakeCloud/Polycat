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

import lombok.Data;
import lombok.experimental.FieldNameConstants;

@Data
@FieldNameConstants
public class RolePrivilegeObject {
    private String roleId;
    private String objectType;
    // if object type is catalog, objectId is catalogId
    // if object type is database, objectId is catalogId.databaseId
    // if object type is table, objectId is catalogId.databaseId.tableId
    private String objectId;
    private long privilege;
    private String catalogId;
    private String databaseId;

    public RolePrivilegeObject(String roleId,  String objectType, String objectId, long privilege,
        String catalogId, String databaseId) {
        this.roleId = roleId;
        this.objectType = objectType;
        this.objectId = objectId;
        this.privilege = privilege;
        this.catalogId = catalogId;
        this.databaseId = databaseId;
    }

    public RolePrivilegeObject(String roleId, String objectType, String objectId, long privilege) {
        this.roleId = roleId;
        this.objectType = objectType;
        this.objectId = objectId;
        this.privilege = privilege;
    }
}

