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

public interface ShareService {

    /**
     * create share
     *
     * @param projectId
     * @param shareInput
     */
    void createShare(String projectId, ShareInput shareInput);

    /**
     *
     * @param projectId
     * @param shareId
     */
    void dropShareById(String projectId, String shareId);

    /**
     * drop share by name
     *
     * @param shareName
     */
    void dropShareByName(String projectId, String shareName);

    /**
     * get share by share name
     *
     * @param shareName
     * @return share
     */
    Share getShareByName(String projectId, String shareName);

    /**
     *
     * @param projectId
     * @param shareId
     * @return
     */
    Share getShareById(String projectId, String shareId);

    /**
     * alter share
     *
     * @param shareName
     * @param shareInput
     */
    void alterShare(String projectId, String shareName, ShareInput shareInput);

    /**
     * add accounts to share
     *
     * @param shareName
     * @param shareInput
     */
    void addConsumersToShare(String projectId, String shareName, ShareInput shareInput);

    /**
     * remove accounts from share
     *
     * @param shareName
     * @param shareInput
     */
    void removeConsumersFromShare(String projectId, String shareName, ShareInput shareInput);

    /**
     * add users to share
     *
     * @param shareName
     * @param shareInput
     */
    void addUsersToShareConsumer(String projectId, String shareName, ShareInput shareInput);

    /**
     * remove users from share
     *
     * @param shareName
     * @param shareInput
     */
    void removeUsersFromShareConsumer(String projectId, String shareName, ShareInput shareInput);

    /**
     * add privilege to share
     *
     * @param shareName
     * @param shareInput
     */
    void addPrivilegeToShare(String projectId, String shareName, ShareInput shareInput);

    /**
     * remove privilege from share
     *
     * @param shareName
     * @param shareInput
     */
    void removePrivilegeFromShare(String projectId, String shareName, ShareInput shareInput);

    /**
     * get share models in project
     *
     * @param projectId
     * @param namePattern
     */
    List<Share> getShareModels(String projectId, String accountId, String user, String namePattern);
}