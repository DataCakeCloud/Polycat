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

import io.polycat.catalog.common.model.UsageProfileAccessStatObject;
import io.polycat.catalog.common.model.UsageProfileObject;
import io.polycat.catalog.common.model.UsageProfilePrePreStatObject;
import io.polycat.catalog.store.gaussdb.pojo.RoleUserRecord;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface UsageProfileMapper {

    void createUsageProfileSubspace(@Param("projectId") String projectId);

    void createUsageProfilePreStatSubspace(String projectId);

    void createUsageProfileAccessStatSubspace(String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.table_usage_profile_detail")
    void dropUsageProfileSubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.table_usage_profile_prestat")
    void dropUsageProfilePreStatSubspace(String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.table_usage_profile_access_stat")
    void dropUsageProfileAccessStatSubspace(String projectId);

    @Delete("DELETE FROM schema_${projectId}.role_user WHERE role_id = #{roleId} AND user_id = #{userId}")
    int deleteRoleUser(@Param("projectId") String projectId, @Param("roleId") String roleId,
                       @Param("userId") String userId);


    @Delete("DELETE FROM schema_${projectId}.role_user WHERE role_id = #{roleId}")
    void delAllRoleUser(@Param("projectId") String projectId, @Param("roleId") String roleId);

    void insertUsageProfileDetail(@Param("projectId") String projectId, @Param("data") UsageProfileObject usageProfileObject);

    UsageProfilePrePreStatObject getUsageProfilePreStat(@Param("projectId") String projectId, @Param("data") UsageProfilePrePreStatObject preStatObject);

    void insertUsageProfilePreStat(@Param("projectId") String projectId, @Param("data") UsageProfilePrePreStatObject buildPreStatObject);

    void updateUsageProfilePreStat(@Param("projectId") String projectId, @Param("data") UsageProfilePrePreStatObject existPreStat);

    UsageProfileAccessStatObject getUsageProfileAccessStat(@Param("projectId") String projectId, @Param("data") UsageProfileAccessStatObject accessStatObject);

    void insertUsageProfileAccessStat(@Param("projectId") String projectId, @Param("data") UsageProfileAccessStatObject accessStatObject);

    void updateUsageProfileAccessStat(@Param("projectId") String projectId, @Param("data") UsageProfileAccessStatObject existAccessStatObject);

    List<UsageProfilePrePreStatObject> listUsageProfilePreStatByFilter(@Param("projectId") String projectId, @Param("filter") String filter, @Param("limit") int maxBatchRowNum, @Param("offset") long offset);

    List<UsageProfileAccessStatObject> getUsageProfileAccessStatListByFilter(@Param("projectId") String projectId, @Param("filter") String filter, @Param("sortOrder") String sortOrder);

    List<String> getTableAccessUsers(@Param("projectId") String projectId, @Param("catalogName") String catalogName, @Param("databaseName") String databaseName, @Param("tableName") String tableName);

    List<UsageProfileObject> getUsageProfileDetailsByCondition(@Param("projectId") String projectId, @Param("filter") String filter, @Param("rowCount") int rowCount);
}
