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

import lombok.Getter;

public class TenantInfo {

    final private static String OAuthURI = "/api/v1/oauth2/token?grant_type=authorization_code&code=";
    final private static String UserInfo = "/api/v1/oauth2/userinfo";

    @Getter
    private String tenant;
    @Getter
    private String domainName;
    @Getter
    private String clientId;
    @Getter
    private String clientSecret;

    public TenantInfo(String tenant, String domainName, String clientId, String clientSecret) {
        this.tenant = tenant;
        this.domainName = domainName;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    public String urlAccessToken(String code) {
        return domainName + OAuthURI + code;
    }

    public String urlUserInfo() {
        return domainName + UserInfo;
    }
}
