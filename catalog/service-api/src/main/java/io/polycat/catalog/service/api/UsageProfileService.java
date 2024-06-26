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
package io.polycat.catalog.service.api;

import java.util.List;

import io.polycat.catalog.common.model.TableAccessUsers;
import io.polycat.catalog.common.model.TableSource;
import io.polycat.catalog.common.model.TableUsageProfile;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.TableUsageProfileInput;
import io.polycat.catalog.common.plugin.request.input.TopTableUsageProfileInput;


public interface UsageProfileService {

    /**
     * record UsageProfile
     *
     * @param tableUsageProfileInput tableUsageProfileInputs
     */
    void recordTableUsageProfile(TableUsageProfileInput tableUsageProfileInput);

    /**
     * get  topTabeles by count
     *
     * @param projectId                 projectId
     * @param catalogName               catalogName
     * @param topTableUsageProfileInput topTableUsageProfileInput
     * @return List<TableUsageProfile>
     */
    List<TableUsageProfile> getTopTablesByCount(String projectId, String catalogName,
        TopTableUsageProfileInput topTableUsageProfileInput);


    /**
     * get SourceTables by table
     *
     * @param table     table
     * @param startTime startTime
     * @param endTime   endTime
     * @param userId
     * @param taskId
     * @param tag
     * @return List<TableUsageProfile>
     */
    List<TableUsageProfile> getUsageProfileByTable(TableName table, long startTime, long endTime, String userId, String taskId, String tag);

    void deleteUsageProfilesByTime(String projectId, long startTime, long endTime);

    List<TableUsageProfile> getUsageProfileGroupByUser(TableName tableName, long startTime, long endTime, String opTypes, boolean sortAsc);

    List<TableAccessUsers> getTableAccessUsers(String projectId, List<TableSource> tableSources);

    TraverseCursorResult<List<TableUsageProfile>> getUsageProfileDetailsByCondition(TableSource tableSource, long startTime, long endTime, List<String> operations,
                                                                                           String userId, String taskId, String tag, int rowCount, String pageToken);
}
