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

import java.io.IOException;

import io.polycat.catalog.common.Logger;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.PostMethod;

import com.alibaba.fastjson.JSONObject;

public class OneAccessClient {
    final private static String uriUserName = "/api/v2/tenant/users/user-by-username";
    final private static String OAuthTokenURI = "/api/v1/oauth2/token?";
    private static final Logger logger = Logger.getLogger(OneAccessClient.class.getName());

    public String Authenticate(TenantInfo tenantInfo, String userName, String password) throws IOException {
        String params = String.format("client_id=%s&client_secret=%s&grant_type=%s&username=%s&password=%s",
            tenantInfo.getClientId(), tenantInfo.getClientSecret(), "password", userName, password);

        String urlWithParam = tenantInfo.getDomainName() + OAuthTokenURI + params;
        PostMethod postMethod = new PostMethod(urlWithParam);
        postMethod.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
        logger.info("OneAccessUrl:" + urlWithParam);
        HttpClient httpClient = new HttpClient();
        if (httpClient.executeMethod(postMethod) != HttpStatus.SC_OK) {
            throw new IOException();
        }
        String result = postMethod.getResponseBodyAsString();
        logger.info("result:" + result);
        postMethod.releaseConnection();
        return JSONObject.parseObject(result).getString("access_token");
    }

    public String getUserIdByUserName(String domainName, String userName, String accessToken) throws IOException {
        String url = domainName + uriUserName;
        String headerAuth = "Bearer " + accessToken;

        NameValuePair[] nameValuePairs = {new NameValuePair("user_name", userName)};

        String result = HttpUtil.httpPost(url, null, headerAuth, nameValuePairs);
        JSONObject jsonResult = JSONObject.parseObject(result);
        return jsonResult.getString("user_id");
    }
}
