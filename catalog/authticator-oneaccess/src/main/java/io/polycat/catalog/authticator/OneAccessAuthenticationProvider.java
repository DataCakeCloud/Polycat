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
package io.polycat.catalog.authticator;

import lombok.SneakyThrows;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import javax.security.sasl.AuthenticationException;

import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.common.plugin.CatalogContext;

public class OneAccessAuthenticationProvider implements PasswdAuthenticationProvider {
    ImcClient imcClient = new ImcClient();
    OneAccessClient oneAccessClient = new OneAccessClient();

    @SneakyThrows
    @Override
    public void Authenticate(String userName, String password) {
       if (userName == null || password == null) {
           throw new AuthenticationException("Authticator fail, userName or password is null");
       }

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
           String token = oneAccessClient.Authenticate(tenantInfo, user, password);
           CatalogUserInformation.logoutCurrentUser();
           CatalogContext context = new CatalogContext(tenant, user, tenant, token);
           CatalogUserInformation.setCurrentUser(context);
       } catch (Exception e) {
           throw new AuthenticationException("Authticator fail");
       }
    }
}
