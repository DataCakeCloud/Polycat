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

import io.polycat.catalog.common.model.Share;
import io.polycat.catalog.common.plugin.request.input.ShareInput;

public interface GlobalShareService {

    /**
     * create share
     *
     * @param projectId
     * @param shareInput
     */
    void createShare(String projectId, ShareInput shareInput);

    void dropShareById(String projectId, String shareId);

    void dropShareByName(String projectId, String shareName);

    Share getShareByName(String projectId, String shareName);

    Share getShareById(String projectId, String shareId);

    void alterShare(String projectId, String shareName, ShareInput shareInput);

    void addConsumersToShare(String projectId, String shareName, ShareInput shareInput);

    void removeConsumersFromShare(String projectId, String shareName, ShareInput shareInput);

    void addUsersToShareConsumer(String projectId, String shareName, ShareInput shareInput);

    void removeUsersFromShareConsumer(String projectId, String shareName, ShareInput shareInput);

    List<Share> getShareModels(String projectId, String accountId, String user, String namePattern);
}