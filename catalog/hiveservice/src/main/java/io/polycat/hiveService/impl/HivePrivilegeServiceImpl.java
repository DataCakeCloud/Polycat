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
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.AuthTableObjParam;
import io.polycat.catalog.common.model.AuthorizationResponse;
import io.polycat.catalog.common.model.AuthorizationType;
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;
import io.polycat.catalog.common.plugin.request.input.ShareInput;
import io.polycat.catalog.service.api.PrivilegeService;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HivePrivilegeServiceImpl implements PrivilegeService {

    @Override
    public AuthorizationResponse sqlAuthentication(String projectId, List<AuthorizationInput> authorizationInputList,
        Boolean ignoreUnknownObj) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "sqlAuthentication");
    }

    @Override
    public boolean authenticationForGrant(String projectId, String objectName, String grantName, String token,
        Operation operation, ObjectType objectType) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "authenticationForGrant");
    }

    @Override
    public boolean authenticationForNormal(String projectId, String objectName, String token, Operation operation,
        String apiMethod) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "authenticationForNormal");
    }

    @Override
    public boolean authenticationForDatabase(String projectId, String catalogName, String databaseName, String token,
        Operation operation, String apiMethod) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "authenticationForDatabase");
    }

    @Override
    public boolean authenticationForTable(String projectId, String catalogName, String databaseName, String tableName,
        String token, Operation operation) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "authenticationForTable");
    }

    @Override
    public boolean authenticationForTable(String projectId, String catalogName, String databaseName, String tableName,
        String token, Operation operation, String apiMethod) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "authenticationForTable");
    }

    @Override
    public boolean authenticationForTable(AuthTableObjParam authTableObjParam, String token, Operation operation,
        String apiMethod) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "authenticationForTable");
    }

    @Override
    public boolean authenticationForAccelerator(String projectId, String catalogName, String databaseName,
        String acceleratorName, String token, Operation operation) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "authenticationForAccelerator");
    }

    @Override
    public boolean authenticationToken(String token) {
        return true;
    }

    @Override
    public boolean authenticationForShare(String projectId, String shareName, String token, AuthorizationType authType,
        Operation operation, ShareInput shareInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "authenticationForShare");
    }

    @Override
    public boolean authenticationForView(String token) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "authenticationForView");
    }

    @Override
    public boolean authenticationMockFalse(String token) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "authenticationMockFalse");
    }
}
