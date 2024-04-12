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
package io.polycat.catalog.authentication.impl;

import java.io.IOException;

import io.polycat.catalog.authentication.api.Authenticator;
import io.polycat.catalog.authentication.model.Identity;
import io.polycat.catalog.authentication.model.ImcIdentity;
import io.polycat.catalog.authentication.model.TokenParseResult;
import io.polycat.catalog.authentication.oneAccess.ImcClient;
import io.polycat.catalog.authentication.oneAccess.OneAccessClient;
import io.polycat.catalog.authentication.oneAccess.TenantInfo;
import io.polycat.catalog.authentication.model.AuthenticationResult;

import lombok.SneakyThrows;

public class IMCAuthenticator implements Authenticator {
    private ImcClient imcClient;
    private OneAccessClient oneAccessClient;

    public IMCAuthenticator() throws IOException {
        imcClient = new ImcClient();
        oneAccessClient = new OneAccessClient();
    }

    private static ImcIdentity converToImcIdentity(Identity identity){
        ImcIdentity imcIdentity = new ImcIdentity();
        imcIdentity.setUserId(identity.getUserId());
        imcIdentity.setPasswd(identity.getPasswd());
        imcIdentity.setIdentityOwner(identity.getIdentityOwner());
        return imcIdentity;
    }

    @SneakyThrows
    @Override
    public AuthenticationResult authAndCreateToken(Identity identity) throws RuntimeException{
        ImcIdentity imcIdentity =  converToImcIdentity(identity);;
        String userName = identity.getUserId();
        String[] userInfos = userName.split("@");
        String tenant;
        String user;
        if (userInfos.length == 2) {
            user = userInfos[0];
            tenant = userInfos[1];
        } else {
            user = userName;
            tenant = userName;
        }
        try {
            TenantInfo tenantInfo = imcClient.getTenantInfo("enterprise", tenant);
            String token = oneAccessClient.Authenticate(tenantInfo, user, imcIdentity.getPasswd());
            AuthenticationResult result = new AuthenticationResult();
            result.setAllowed(true);
            result.setAccountId(tenant);
            result.setProjectId(tenant);
            result.setToken(token);
            return result;
        } catch (Exception e) {
            throw new RuntimeException("auth fail");
        }
    }

    @Override
    public String getShortName() {
        return "IMCAuthenticator";
    }

    @Override
    public TokenParseResult CheckAndParseToken(String token) throws IOException, ClassNotFoundException{
        return null;
    }

}