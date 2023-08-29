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

import io.polycat.catalog.common.model.UserPrivilege;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface UserPrivilegeMapper {

    void createUserPrivilegeSubspace(@Param("projectId") String projectId);

    @Delete("DROP TABLE IF EXISTS schema_${projectId}.user_privilege")
    void dropUserPrivilegeSubspace(@Param("projectId") String projectId);

    void insertUserPrivilege(@Param("projectId") String projectId, @Param("userId") String userId,
                             @Param("objectType") String objectType, @Param("objectId") String objectId,
                             @Param("isOwner") Boolean isOwner, @Param("privilege") long privilege);

    UserPrivilege getUserPrivilege(@Param("projectId") String projectId, @Param("userId") String userId,
                                   @Param("objectType") String objectType, @Param("objectId") String objectId);

    List<UserPrivilege> getUserPrivilegesByFilter(@Param("projectId") String projectId, @Param("filter") String filter);

    void deleteUserPrivilege(@Param("projectId") String projectId, @Param("userId") String userId,
                             @Param("objectType") String objectType, @Param("objectId") String objectId);
}
