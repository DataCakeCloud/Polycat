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
import io.polycat.catalog.common.model.Share;
import io.polycat.catalog.common.plugin.request.input.ShareInput;
import io.polycat.catalog.service.api.GlobalShareService;

public class HiveGlobalShareServiceImpl implements GlobalShareService {

    @Override
    public void createShare(String projectId, ShareInput shareInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "createShare");
    }

    @Override
    public void dropShareById(String projectId, String shareId) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "dropShareById");
    }

    @Override
    public void dropShareByName(String projectId, String shareName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "dropShareByName");
    }

    @Override
    public Share getShareByName(String projectId, String shareName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getShareByName");
    }

    @Override
    public Share getShareById(String projectId, String shareId) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getShareById");
    }

    @Override
    public void alterShare(String projectId, String shareName, ShareInput shareInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "alterShare");
    }

    @Override
    public void addConsumersToShare(String projectId, String shareName, ShareInput shareInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "addConsumersToShare");
    }

    @Override
    public void removeConsumersFromShare(String projectId, String shareName, ShareInput shareInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "removeConsumersFromShare");
    }

    @Override
    public void addUsersToShareConsumer(String projectId, String shareName, ShareInput shareInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "addUsersToShareConsumer");
    }

    @Override
    public void removeUsersFromShareConsumer(String projectId, String shareName, ShareInput shareInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "removeUsersFromShareConsumer");
    }

    @Override
    public List<Share> getShareModels(String projectId, String accountId, String user, String namePattern) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getShareModels");
    }
}
