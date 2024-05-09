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

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.User;
import io.polycat.catalog.common.plugin.request.input.UserGroupInput;
import io.polycat.catalog.service.api.UserGroupService;

/**
 * @Author: d00428635
 * @Create: 2022-05-11
 **/
public class HiveUserGroupServiceImpl  implements UserGroupService {

    @Override
    public void syncUser(String projectId, String domainId, UserGroupInput userInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "syncUser");
    }

    @Override
    public void syncGroup(String projectId, String domainId, UserGroupInput groupInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "syncGroup");
    }

    @Override
    public void syncGroupUserIndex(String projectId, String domainId, Map<String, Object> groupUserIndex) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "syncGroupUserIndex");
    }

    @Override
    public List<User> getUserListByDomain(String projectId, String domainId) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getUserListByDomain");
    }

    @Override
    public List<User> getGroupListByDomain(String projectId, String domainId) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getGroupListByDomain");
    }

    @Override
    public HashSet<String> getUserSetByGroupId(String projectId, String groupId) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getUserListByGroupId");
    }

    @Override
    public HashSet<String> getGroupSetByUserId(String projectId, String userId) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getGroupListByUserId");
    }
}
