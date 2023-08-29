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

import io.polycat.catalog.authorization.policyDecisionPoint.AuthRequest;
import io.polycat.catalog.authorization.policyDecisionPoint.AuthResult;


public interface AuthorizationService {

    /**
     * checkPrivilege
     *
     * @param authRequest authRequest
     */
    AuthResult checkPrivilege(AuthRequest authRequest);

    /**
     * getUserGroupByUser
     *
     * @param userId userId
     * @Return userGroups userGroups
     */
    List<String> getUserGroupsByUser(String projectId, String principalSource, String userId);

    /**
     * getUserRolesByUser
     * @param projectId projectId
     * @param principalSource principalSource
     * @param userId userId
     * @Return userRoles userRoles
     */
    List<String> getUserRolesByUser(String projectId, String principalSource,String userId);
}
