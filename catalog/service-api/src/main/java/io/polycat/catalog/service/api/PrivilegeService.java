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

import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.AuthTableObjParam;
import io.polycat.catalog.common.model.AuthorizationResponse;
import io.polycat.catalog.common.model.AuthorizationType;
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;
import io.polycat.catalog.common.plugin.request.input.ShareInput;

public interface PrivilegeService {

    default AuthorizationResponse sqlAuthentication(String projectId, List<AuthorizationInput> authInputList) {
        return sqlAuthentication(projectId, authInputList, false);
    }

    AuthorizationResponse sqlAuthentication(String projectId, List<AuthorizationInput> authorizationInputList,
        Boolean ignoreUnknownObj);

    boolean authenticationForGrant(String projectId, String objectName, String grantName, String token,
        Operation operation, ObjectType objectType);

    default boolean authenticationForNormal(String projectId, String objectName, String token, Operation operation) {
        return authenticationForNormal(projectId, objectName, token, operation, "");
    }

    boolean authenticationForNormal(String projectId, String objectName, String token, Operation operation,
        String apiMethod);

    default boolean authenticationForDatabase(String projectId, String catalogName, String databaseName,
        String token, Operation operation) {
        return authenticationForDatabase(projectId, catalogName, databaseName, token, operation, "");
    }

    boolean authenticationForDatabase(String projectId, String catalogName, String databaseName,
        String token, Operation operation, String apiMethod);

    boolean authenticationForTable(String projectId, String catalogName, String databaseName, String tableName,
        String token, Operation operation);

    boolean authenticationForTable(String projectId, String catalogName, String databaseName, String tableName,
        String token, Operation operation, String apiMethod);

    default boolean authenticationForTable(AuthTableObjParam authTableObjParam, String token, Operation operation) {
        return authenticationForTable(authTableObjParam, token, operation, "");
    }

    boolean authenticationForTable(AuthTableObjParam authTableObjParam, String token, Operation operation,
        String apiMethod);

    boolean authenticationForAccelerator(String projectId, String catalogName, String databaseName,
        String acceleratorName, String token, Operation operation);

    boolean authenticationToken(String token);

    boolean authenticationForShare(String projectId, String shareName, String token, AuthorizationType authType,
        Operation operation, ShareInput shareInput);

    boolean authenticationForView(String token);

    boolean authenticationMockFalse(String token);
}