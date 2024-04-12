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

import io.polycat.catalog.store.gaussdb.pojo.*;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface RolePropertiesMapper {

    void createRolePropertiesSubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.role_properties")
    void dropRolePropertiesSubspace(@Param("projectId") String projectId);

    @Insert("INSERT INTO schema_${projectId}.role_properties (role_id, name, create_time, owner_id, comment)" +
            "        VALUES (#{record.roleId}, #{record.name}, #{record.createTime}, #{record.ownerId}, #{record.comment})")
    void insertRoleProperties(@Param("projectId") String projectId, @Param("record") RolePropertiesRecord record);

    @Select("SELECT * FROM schema_${projectId}.role_properties WHERE role_id=#{roleId}")
    RolePropertiesRecord getRoleProperties(@Param("projectId") String projectId, @Param("roleId") String roleId);

    @Update("UPDATE schema_${projectId}.role_properties SET name = #{data.name}, owner_id = #{data.ownerId}, comment = #{data.comment} "
            + "WHERE role_id = #{data.roleId}")
    void updateRoleProperties(@Param("projectId") String projectId, @Param("data") RolePropertiesRecord record);

    @Delete("DELETE FROM schema_${projectId}.role_properties WHERE role_id = #{roleId}")
    void deleteRoleProperties(@Param("projectId") String projectId, @Param("roleId") String roleId);

    /**
     * when userId contains owner is not userId:  Results may not be comprehensive
     * @param projectId
     * @param filter
     * @return
     */
    List<RoleObjectRecord> showRoleObjectsByFilter(@Param("projectId") String projectId, @Param("filter") String filter);

    List<RolePropertiesRecord> getRolePropertiesList(@Param("projectId") String projectId, @Param("filter") String filter);
}
