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

import java.util.HashSet;
import java.util.List;
import java.util.Map;


import io.polycat.catalog.common.model.User;
import io.polycat.catalog.common.plugin.request.input.UserGroupInput;

public interface UserGroupService {


    void syncUser(String projectId, String domainId, UserGroupInput userInput);

    void syncGroup(String projectId, String domainId, UserGroupInput groupInput);

    void syncGroupUserIndex(String projectId, String domainId, Map<String, Object> groupUserIndex);

    List<User> getUserListByDomain(String projectId, String domainId);

    List<User> getGroupListByDomain(String projectId, String domainId);

    HashSet<String> getUserSetByGroupId(String projectId, String groupId);

    HashSet<String> getGroupSetByUserId(String projectId, String userId);
}
