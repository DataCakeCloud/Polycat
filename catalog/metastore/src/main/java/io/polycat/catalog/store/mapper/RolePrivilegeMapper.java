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
package io.polycat.catalog.store.mapper;

import io.polycat.catalog.store.gaussdb.pojo.DatabaseHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.DatabaseRecord;
import io.polycat.catalog.store.gaussdb.pojo.DroppedDatabaseNameRecord;
import io.polycat.catalog.store.gaussdb.pojo.RolePrivilegeRecord;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface RolePrivilegeMapper {

    void createRolePrivilegeSubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.role_privilege")
    void dropRolePrivilegeSubspace(@Param("projectId") String projectId);

    void insertRolePrivilege(@Param("projectId") String projectId, @Param("data") RolePrivilegeRecord rolePrivilegeRecord);

    @Delete("DELETE FROM schema_${projectId}.role_privilege WHERE role_id = #{roleId} AND object_type = #{objectType} AND object_id = #{objectId}")
    void deleteRolePrivilege(@Param("projectId") String projectId, @Param("roleId") String roleId,
                             @Param("objectType") String objectType, @Param("objectId") String objectId);

    @Delete("DELETE FROM schema_${projectId}.role_privilege WHERE role_id = #{roleId}")
    void delAllRolePrivilege(@Param("projectId") String projectId, @Param("roleId") String roleId);

    @Delete("DELETE FROM schema_${projectId}.role_privilege WHERE object_type = #{objectType} AND object_id = #{objectId}")
    void removeAllPrivilegeOnObject(@Param("projectId") String projectId, @Param("objectType") String objectType, @Param("objectId") String roleObjectId);

    @Update("UPDATE schema_${projectId}.role_privilege SET privilege = #{newPrivilege} "
        + "WHERE role_id = #{roleId} AND object_type = #{objectType} AND object_id = #{objectId}")
    Integer updateRolePrivilege(@Param("projectId") String projectId, @Param("roleId") String roleId, @Param("objectId") String objectId, @Param("objectType") String objectType, @Param("newPrivilege") long newPrivilege);

    RolePrivilegeRecord getRolePrivilege(@Param("projectId") String projectId, @Param("roleId") String roleId,
                            @Param("objectId") String roleObjectId, @Param("objectType") String objectType);

    List<RolePrivilegeRecord> getRolePrivileges(@Param("projectId") String projectId, @Param("roleId") String roleId);

    List<RolePrivilegeRecord> getRolePrivilegesByFilter(@Param("projectId") String projectId, @Param("filter") String filter);
}
