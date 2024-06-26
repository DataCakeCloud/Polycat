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

import java.util.List;

import lombok.Data;
import lombok.experimental.FieldNameConstants;

@Data
@FieldNameConstants
public class RoleObject {
    private String projectId;
    private String roleName;
    private String roleId;
    private String ownerId;
    private long createTime;
    private String comment;
    private List<String> toUsers;
    //private List<ObjectPrivilege> objectPrivileges;

    public RoleObject(String projectId, String roleName, String roleId, String ownerId, long createTime,
        String comment) {
        this.projectId = projectId;
        this.roleName = roleName;
        this.roleId = roleId;
        this.ownerId = ownerId;
        this.createTime = createTime;
        this.comment = comment;
    }

    public RoleObject(String projectId, String roleName, String roleId) {
        this.projectId = projectId;
        this.roleName = roleName;
        this.roleId = roleId;
    }
}
