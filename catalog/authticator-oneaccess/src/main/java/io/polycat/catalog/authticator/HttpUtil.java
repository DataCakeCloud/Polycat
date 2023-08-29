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

import jodd.util.StringUtil;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;

public class HttpUtil {
    public static String httpPost(String url, String contentType, String headerAuth, NameValuePair[] bodyParams)
        throws IOException {
        PostMethod postMethod = new PostMethod(url);
        postMethod.addRequestHeader("Content-Type", contentType);
        postMethod.addRequestHeader("Authorization", headerAuth);

        if (bodyParams != null && bodyParams.length > 0) {
            postMethod.setRequestBody(bodyParams);
        }

        HttpClient httpClient = new HttpClient();
        httpClient.executeMethod(postMethod);
        String result = postMethod.getResponseBodyAsString();
        postMethod.releaseConnection();
        return result;
    }

    public static String httpGet(String url, String urlParam, String headerAuth) throws IOException {
        String urlWithParam = url;
        if (!StringUtil.isEmpty(urlParam)) {
            urlWithParam = url + "?" + urlParam;
        }
        GetMethod getMethod = new GetMethod(urlWithParam);
        getMethod.addRequestHeader("Accept", "application/json");
        getMethod.addRequestHeader("Authorization", headerAuth);

        HttpClient httpClient = new HttpClient();
        httpClient.executeMethod(getMethod);
        String result = getMethod.getResponseBodyAsString();
        getMethod.releaseConnection();
        return result;
    }
}
