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
package io.polycat.catalog.server.service.impl;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.KerberosToken;
import io.polycat.catalog.common.plugin.request.input.TokenInput;
import io.polycat.catalog.service.api.TokenService;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class TokenServiceImpl implements TokenService {

    @Override
    public KerberosToken setToken(String projectId, TokenInput tokenBody) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "setToken");
    }

    @Override
    public void deleteToken(String projectId, String tokenId) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "deleteToken");
    }

    @Override
    public KerberosToken getToken(String projectId, String tokenId) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getToken");
    }

    @Override
    public KerberosToken[] listToken(String projectId, String pattern) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "listToken");
    }

    @Override
    public KerberosToken renewToken(String projectId, String tokenStrForm) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "renewToken");
    }

    @Override
    public KerberosToken addMasterKey(String projectId, TokenInput tokenBody) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "addMasterKey");
    }

    @Override
    public void removeMasterKey(String projectId, String seqNo) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "removeMasterKey");
    }

    @Override
    public KerberosToken[] getMasterKeys(String projectId, String pattern) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getMasterKeys");
    }

    @Override
    public KerberosToken cancelDelegationToken(String projectId, String tokenStrForm) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "cancelDelegationToken");
    }

    @Override
    public KerberosToken getDelegationToken(String projectId, String owner, String renewerKerberosPrincipalName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getDelegationToken");
    }

}
