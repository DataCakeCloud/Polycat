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

import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.gaussdb.pojo.DatabaseHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.DatabaseRecord;
import io.polycat.catalog.store.gaussdb.pojo.DroppedDatabaseNameRecord;
import io.polycat.catalog.store.gaussdb.pojo.RoleObjectNameRecord;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface RoleObjectNameMapper {

    void createRoleObjectNameSubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.role_object_name")
    void dropRoleObjectNameSubspace(@Param("projectId") String projectId);

    @Insert("INSERT INTO schema_${projectId}.role_object_name (name, object_id)\n" +
            "        VALUES (#{roleName}, #{roleId})")
    void insertRoleObjectName(@Param("projectId") String projectId, @Param("roleName") String roleName, @Param("roleId") String roleId);

    @Delete("DELETE FROM schema_${projectId}.role_object_name WHERE name = #{roleName}")
    void deleteRoleObjectName(@Param("projectId") String projectId, @Param("roleName") String roleName);

    @Select("SELECT * FROM schema_${projectId}.role_object_name WHERE object_id = #{roleId}")
    RoleObjectNameRecord getObjectNameById(@Param("projectId") String projectId, @Param("roleId") String objectId);

    @Select("SELECT * FROM schema_${projectId}.role_object_name WHERE name = #{roleName} LIMIT 1")
    RoleObjectNameRecord getObjectNameByName(@Param("projectId") String projectId, @Param("roleName") String roleName);

    List<RoleObjectNameRecord> searchRoleNames(@Param("projectId") String projectId, @Param("keyword") String keyword);
}
