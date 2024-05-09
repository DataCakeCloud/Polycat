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

import java.util.Arrays;

import io.polycat.catalog.common.model.KerberosToken;
import io.polycat.catalog.common.plugin.request.input.TokenInput;
import io.polycat.catalog.service.api.TokenService;
import io.polycat.hiveService.common.HiveServiceHelper;
import io.polycat.hiveService.data.accesser.PolyCatDataAccessor;
import io.polycat.hiveService.hive.HiveMetaStoreClientUtil;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HiveTokenServiceImp implements TokenService {

    @Override
    public KerberosToken setToken(String projectId, TokenInput tokenBody) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                HiveMetaStoreClientUtil.getHMSClient().addToken(tokenBody.getTokenId(), tokenBody.getToken());
                KerberosToken token = new KerberosToken();
                token.setMRSToken(tokenBody.getTokenId(), tokenBody.getToken());
                return token;
            });
    }

    @Override
    public void deleteToken(String projectId, String tokenId) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> {
                HiveMetaStoreClientUtil.getHMSClient().removeToken(tokenId);
                return null;
            });
    }

    @Override
    public KerberosToken getToken(String projectId, String tokenId) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> PolyCatDataAccessor.toMRSToken(tokenId, HiveMetaStoreClientUtil.getHMSClient().getToken(tokenId)));
    }

    @Override
    public KerberosToken[] listToken(String projectId, String pattern) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().getAllTokenIdentifiers().stream().map(PolyCatDataAccessor::toMRSToken)
                .toArray(KerberosToken[]::new));
    }

    @Override
    public KerberosToken renewToken(String projectId, String tokenStrForm) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> PolyCatDataAccessor.toMRSToken(HiveMetaStoreClientUtil.getHMSClient().renewDelegationToken(tokenStrForm)));
    }

    @Override
    public KerberosToken addMasterKey(String projectId, TokenInput tokenBody) {
        return HiveServiceHelper.HiveExceptionHandler(

            () -> {
                if (tokenBody.getTokenId() == null) {
                    return PolyCatDataAccessor.toMRSToken(String.valueOf(HiveMetaStoreClientUtil.getHMSClient().addMasterKey(tokenBody.getToken())),
                        tokenBody.getToken());
                } else {
                    HiveMetaStoreClientUtil.getHMSClient().updateMasterKey(Integer.valueOf(tokenBody.getTokenId()), tokenBody.getToken());
                    return PolyCatDataAccessor.toMRSToken(tokenBody.getTokenId(), tokenBody.getToken());
                }
            });
    }

    @Override
    public void removeMasterKey(String projectId, String seqNo) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().removeMasterKey(Integer.valueOf(seqNo)));
    }

    @Override
    public KerberosToken[] getMasterKeys(String projectId, String pattern) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> Arrays.stream(HiveMetaStoreClientUtil.getHMSClient().getMasterKeys()).map(PolyCatDataAccessor::toMRSToken)
                .toArray(KerberosToken[]::new));
    }

    @Override
    public KerberosToken cancelDelegationToken(String projectId, String tokenStrForm) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> {
                HiveMetaStoreClientUtil.getHMSClient().cancelDelegationToken(tokenStrForm);
                return new KerberosToken();
            });
    }

    @Override
    public KerberosToken getDelegationToken(String projectId, String owner, String renewerKerberosPrincipalName) {
        return HiveServiceHelper.HiveExceptionHandler(
            () -> PolyCatDataAccessor.toMRSToken(HiveMetaStoreClientUtil.getHMSClient().getDelegationToken(owner, renewerKerberosPrincipalName)));
    }
}
