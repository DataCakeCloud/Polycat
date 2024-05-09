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
package io.polycat.hiveService.impl;

import java.util.List;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.TableAccessUsers;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TableSource;
import io.polycat.catalog.common.model.TableUsageProfile;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.TableUsageProfileInput;
import io.polycat.catalog.common.plugin.request.input.TopTableUsageProfileInput;
import io.polycat.catalog.service.api.UsageProfileService;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HiveUsageProfileServiceImpl implements UsageProfileService {

    @Override
    public void recordTableUsageProfile(TableUsageProfileInput tableUsageProfileInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "recordTableUsageProfile");
    }

    @Override
    public List<TableUsageProfile> getTopTablesByCount(String projectId, String catalogName,
        TopTableUsageProfileInput topTableUsageProfileInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getTopTablesByCount");
    }

    @Override
    public List<TableUsageProfile> getUsageProfileByTable(TableName table, long startTime, long endTime, String userId, String taskId, String tag) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getUsageProfileByTable");
    }

    @Override
    public void deleteUsageProfilesByTime(String projectId, long startTime, long endTime) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "deleteUsageProfilesByTime");
    }

    @Override
    public List<TableUsageProfile> getUsageProfileGroupByUser(TableName tableName, long startTime, long endTime, String opTypes, boolean sortAsc) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getUsageProfileGroupByUser");
    }

    @Override
    public List<TableAccessUsers> getTableAccessUsers(String projectId, List<TableSource> tableSources) {
        return null;
    }

    @Override
    public TraverseCursorResult<List<TableUsageProfile>> getUsageProfileDetailsByCondition(TableSource tableSource, long startTime, long endTime, List<String> operations,
                                                                                           String userId, String taskId, String tag, int rowCount, String pageToken) {
        return null;
    }
}
