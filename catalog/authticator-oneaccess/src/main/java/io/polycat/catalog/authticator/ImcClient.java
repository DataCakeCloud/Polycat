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

import javax.security.sasl.AuthenticationException;

import io.polycat.catalog.common.PolyCatConf;
import io.polycat.catalog.common.Logger;

import com.alibaba.fastjson.JSONObject;
import com.cloud.apigateway.sdk.utils.Client;
import com.cloud.apigateway.sdk.utils.Request;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


public class ImcClient {
    private static String urlTenantInfo;
    private static String imcKey;
    private static String imcSecret;
    private static final Logger logger = Logger.getLogger(ImcClient.class.getName());

    public ImcClient() {
        String confPath = PolyCatConf.getConfPath(null);
        if (confPath != null) {
            PolyCatConf polyCatConf = new PolyCatConf(confPath);
            urlTenantInfo = polyCatConf.getCatalogImcUrl();
            imcKey = polyCatConf.getCatalogImcKey();
            imcSecret = polyCatConf.getCatalogImcSecret();
        }
    }

    public static TenantInfo getTenantInfo(String paramType, String tenantOrEnterpriseId)
        throws AuthenticationException {
        Request request = new Request();
        try {
            request.setKey(imcKey);
            request.setSecret(imcSecret);
            request.setMethod("POST");
            request.setUrl(urlTenantInfo);
            request.addHeader("Content-Type", "application/json");
            String body = String.format("{\"id\":\"%s\", \"idType\":\"%s\"}", tenantOrEnterpriseId, paramType);
            request.setBody(body);
            logger.info("body : " + body);
        } catch (Exception e) {
            e.printStackTrace();
            logger.warn("set imc request fail.");
            throw new AuthenticationException("Authticator fail");
        }

        CloseableHttpClient client = null;
        TenantInfo tenantInfo = null;
        try {
            HttpRequestBase signedRequest = Client.sign(request);
            client = HttpClients.custom().build();
            HttpResponse response = client.execute(signedRequest);
            logger.info(response.getStatusLine().toString());
            Header[] resHeaders = response.getAllHeaders();
            for (Header h : resHeaders) {
                logger.info(h.getName() + ":" + h.getValue());
            }
            HttpEntity resEntity = response.getEntity();
            String result;
            if (resEntity != null) {
                result = EntityUtils.toString(resEntity, "UTF-8");
                logger.info(System.getProperty("line.separator") + result);
            } else {
                throw new AuthenticationException("Authticator fail");
            }
            JSONObject jsonResult = JSONObject.parseObject(result);
            JSONObject tenantResult = JSONObject.parseObject(jsonResult.getString("result"));
            tenantInfo = new TenantInfo(tenantResult.getString("tenantId"),
                tenantResult.getString("domainName"),
                tenantResult.getString("clientId"),
                tenantResult.getString("clientSecret"));
            logger.info(String.format("tennat : %s, domainName : %s, clientId : %s, clientSecret : %s.", tenantInfo.getTenant(),
                tenantInfo.getDomainName(), tenantInfo.getClientId(), tenantInfo.getClientSecret()));

        } catch (Exception e) {
            throw new AuthenticationException("Authticator fail");
        } finally {
            try {
                if (client != null) {
                    client.close();
                }
            } catch (IOException e) {
                throw new AuthenticationException("Authticator fail");
            }
        }
        return tenantInfo;
    }
}
