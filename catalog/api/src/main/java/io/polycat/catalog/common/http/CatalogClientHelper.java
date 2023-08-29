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
package io.polycat.catalog.common.http;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.plugin.response.CatalogWebServiceResult;
import io.polycat.catalog.common.utils.GsonUtil;

import com.google.gson.JsonElement;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import static io.polycat.catalog.common.Constants.KEY_ERRORS;
import static io.polycat.catalog.common.Constants.KEY_ERROR_CODE;

public class CatalogClientHelper {

    private static final Logger LOGGER = Logger.getLogger(CatalogClientHelper.class);
    public static final String NO_ERROR_CODE = "";
    public static final String HTTP_ERROR_CODE = "http.00000";

    public static CatalogWebServiceResult put(String requestUrl) throws IOException {
        return access(requestUrl, new HashMap<>(), null, HttpMethodName.PUT);
    }

    public static CatalogWebServiceResult delete(String requestUrl) throws IOException {
        return access(requestUrl, new HashMap<>(), null, HttpMethodName.DELETE);
    }

    public static CatalogWebServiceResult get(String requestUrl) throws IOException {
        return access(requestUrl, new HashMap<>(), null, HttpMethodName.GET);
    }

    public static CatalogWebServiceResult post(String requestUrl, Object entity) throws IOException {
        return access(requestUrl, new HashMap<>(), entity, HttpMethodName.POST);
    }

    public static CatalogWebServiceResult patch(String requestUrl, Object entity) throws IOException {
        return access(requestUrl, new HashMap<>(), entity, HttpMethodName.PATCH);
    }

    public static CatalogWebServiceResult putWithHeader(String requestUrl, Map<String, String> header) throws IOException {
        return access(requestUrl, header, null, HttpMethodName.PUT);
    }

    public static CatalogWebServiceResult putWithHeader(String requestUrl, Object entity, Map<String, String> header) throws IOException {
        return access(requestUrl, header, entity, HttpMethodName.PUT);
    }

    public static CatalogWebServiceResult deleteWithHeader(String requestUrl, Map<String, String> header) throws IOException {
        return access(requestUrl, header, null, HttpMethodName.DELETE);
    }

    public static CatalogWebServiceResult getWithHeader(String requestUrl, Map<String, String> header) throws IOException {
        return access(requestUrl, header, null, HttpMethodName.GET);
    }
    
    public static CatalogWebServiceResult postWithHeader(String requestUrl, Object entity, Map<String, String> header) throws IOException {
        return access(requestUrl, header, entity, HttpMethodName.POST);
    }

    public static CatalogWebServiceResult patchWithHeader(String requestUrl, Object entity, Map<String, String> header) throws IOException {
        return access(requestUrl, header, entity, HttpMethodName.PATCH);
    }

    public static CatalogWebServiceResult access(String requestUrl, Map<String, String> header, Object entity,
                                              HttpMethodName httpMethod) throws IOException {
        URL url = new URL(requestUrl);
        if (entity == null) {
            return access1(url, header, null, 0L, httpMethod);
        } else {
            String body;
            if (!(entity instanceof String)) {
                body = GsonUtil.toJson(entity);
            } else {
                body = entity.toString();
            }

            InputStream content = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
            return access1(url, header, content, (long) body.getBytes("UTF-8").length, httpMethod);
        }
    }


    private static CatalogWebServiceResult access1(URL url, Map<String, String> headers, InputStream content, Long contentLength,
                                                HttpMethodName httpMethod) throws IOException {
        CloseableHttpResponse httpResponse = null;
        HttpRequestBase httpRequestBase= null;
        CatalogWebServiceResult response = null;
        headers.put("Content-Type", "application/json; charset=utf-8");
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            httpRequestBase = getHttpRequest(url, headers, content, contentLength, httpMethod);
            if (null != client) {
                httpResponse = client.execute(httpRequestBase);
                response = getResponse(httpResponse);
            }
        } finally {
            if (null != httpResponse) {
                try {
                    EntityUtils.consume(httpResponse.getEntity());
                    httpResponse.close();
                } catch (IOException e) {
                    LOGGER.error("Failed to release connection");
                }
            }
            if (null != httpRequestBase) {
                httpRequestBase.releaseConnection();
            }
        }
        return response;
    }

    private static CatalogWebServiceResult getResponse(HttpResponse httpResponse) throws IOException {
        CatalogWebServiceResult result = new CatalogWebServiceResult();
        result.setStatusCode(httpResponse.getStatusLine().getStatusCode());
        result.setReasonPhrase(httpResponse.getStatusLine().getReasonPhrase());
        result.setEntity(EntityUtils.toString(httpResponse.getEntity(), Consts.UTF_8));
        if (!result.is2xxOk() && result.getReasonPhrase().isEmpty()) {
            result.setReasonPhrase(getMessage(result.getEntity(), result.getReasonPhrase()));
        }
        return result;
    }

    private static String getMessage(String entity, String defaultValue) {
        try {
            JsonElement jsonElement = GsonUtil.fromJson(entity, JsonElement.class);
            return jsonElement.getAsJsonObject()
                    .get(Constants.KEY_ERRORS).getAsJsonArray()
                    .get(0).getAsJsonObject()
                    .get(Constants.KEY_MESSAGE)
                    .getAsJsonPrimitive()
                    .getAsString();
        } catch (Exception ex) {
            return defaultValue;
        }
    }


    private static HttpRequestBase getHttpRequest(URL url, Map<String, String> headers, InputStream content,
                                                  Long contentLength, HttpMethodName httpMethod) {

        String urlString = url.toString();
        String parameters = null;
        if (null == urlString || urlString.trim().isEmpty()) {
            return null;
        }
        Map<String, String> parametersMap = new HashMap();
        urlString = Normalizer.normalize(urlString, Normalizer.Form.NFKC);
        if (urlString.contains("?")) {
            parameters = urlString.substring(urlString.indexOf("?") + 1);
            if (null != parameters && !"".equals(parameters)) {
                String[] parameterArray = parameters.split("&");
                String[] pArrTemp = parameterArray;
                int parameterLength = parameterArray.length;

                for (int i = 0; i < parameterLength; i++) {
                    String p = pArrTemp[i];
                    String[] strs = p.split("=");
                    if (strs.length >= 2) {
                        String key = strs[0];
                        String value = strs[1];
                        parametersMap.put(key, value);
                    }
                }
            }
        }

        HttpRequestBase httpRequestBase = createRequest(url, (Header) null, content, contentLength, parametersMap,
                httpMethod);

        if (headers != null) {
            for (Entry<String, String> entry : headers.entrySet()) {
                httpRequestBase.addHeader(entry.getKey(), entry.getValue());
            }
        }

        return httpRequestBase;
    }

    private static HttpRequestBase createRequest(URL url, Header header, InputStream content, Long contentLength,
                                                 Map<String, String> parameters, HttpMethodName httpMethod) {
        Object httpRequest;
        InputStreamEntity entity;
        if (httpMethod == HttpMethodName.POST) {
            HttpPost postMethod = new HttpPost(url.toString());
            if (content != null) {
                entity = new InputStreamEntity(content, contentLength);
                postMethod.setEntity(entity);
            }

            httpRequest = postMethod;
        } else if (httpMethod == HttpMethodName.PUT) {
            HttpPut putMethod = new HttpPut(url.toString());
            httpRequest = putMethod;
            if (content != null) {
                entity = new InputStreamEntity(content, contentLength);
                putMethod.setEntity(entity);
            }
        } else if (httpMethod == HttpMethodName.PATCH) {
            HttpPatch patchMethod = new HttpPatch(url.toString());
            httpRequest = patchMethod;
            if (content != null) {
                entity = new InputStreamEntity(content, contentLength);
                patchMethod.setEntity(entity);
            }
        } else if (httpMethod == HttpMethodName.GET) {
            httpRequest = new HttpGet(url.toString());
        } else {
            httpRequest = new HttpDelete(url.toString());
        }

        ((HttpRequestBase) httpRequest).addHeader(header);
        return (HttpRequestBase) httpRequest;
    }

    public static <M> M makeResultWithModel(CatalogWebServiceResult response, Class<M> entityModelClass) {
        Type type = ParameterizedTypeImpl.make(CatalogResponse.class, new Type[]{entityModelClass}, null);
        CatalogResponse<M> catalogResponse = GsonUtil.fromJson(response.getEntity(), type);
        if (catalogResponse.getCode() < 200 || catalogResponse.getCode() >= 300) {
            throw new HttpResponseException(catalogResponse.getCode(), getErrorCodeFromResponse(catalogResponse),
                catalogResponse.getMessage());
        }
        return catalogResponse.getData();
    }

    public static <M> PagedList<M> makeResultWithModelList(CatalogWebServiceResult response, Class<M> entityModelClass) {
        Type pageListType = ParameterizedTypeImpl.make(PagedList.class, new Type[]{entityModelClass}, null);
        Type type = ParameterizedTypeImpl.make(CatalogResponse.class, new Type[]{pageListType}, null);
        CatalogResponse<PagedList<M>> catalogResponse = GsonUtil.fromJson(response.getEntity(), type);
        if (catalogResponse.getCode() < 200 || catalogResponse.getCode() >= 300) {
            throw new HttpResponseException(catalogResponse.getCode(), getErrorCodeFromResponse(catalogResponse),
                catalogResponse.getMessage());
        }
        return catalogResponse.getData();
    }

    /**
     * Call a REST request, get the REST response and convert the response into a result object. Throw CatalogException
     * if any error indicated by status code in the response
     *
     * @param callable    REST call to run
     * @param resultClass result object class
     * @param <T>         type of the result object
     * @return result object
     * @throws CatalogException of any network error or any error in the REST response
     */
    public static  <T> T withCatalogException(Callable<CatalogWebServiceResult> callable, Class<T> resultClass)
            throws CatalogException {
        try {
            CatalogWebServiceResult result = callable.call();
            throwCatalogExceptionIfHttpError(result);
            return makeResultWithModel(result, resultClass);
        } catch (HttpResponseException e) {
            if (e.getMessage() == null) {
                LOGGER.error("Error from service: " + e.getReasonPhrase());
            } else {
                LOGGER.error("Error from service: " + e.getMessage());
            }

            throw new CatalogException(e.getReasonPhrase(), e, e.getStatusCode());
        } catch (Exception e) {
            LOGGER.error("Error from service: " + e.getMessage());
            throw new CatalogException(e.getMessage(), e);
        }
    }

    public static void makeResultWithModel(CatalogWebServiceResult response) {
        BaseResponse baseResponse = GsonUtil.fromJson(response.getEntity(), BaseResponse.class);
        if (baseResponse.getCode() < 200 || baseResponse.getCode() >= 300) {
            throw new HttpResponseException(baseResponse.getCode(), getErrorCodeFromResponse(baseResponse),
                baseResponse.getMessage());
        }
    }

    /**
     * Call a REST request, get the REST response and convert the response into a result object. Throw CatalogException
     * if any error indicated by status code in the response
     *
     * @param callable REST call to run
     * @throws CatalogException of any network error or any error in the REST response
     */
    public static void withCatalogException(Callable<CatalogWebServiceResult> callable) throws CatalogException {
        try {
            CatalogWebServiceResult result = callable.call();
            throwCatalogExceptionIfHttpError(result);
            makeResultWithModel(result);
        } catch (HttpResponseException e) {
            String message;
            if (e.getReasonPhrase().isEmpty()) {
                message = "code=" + e.getStatusCode();
            } else {
                message = e.getReasonPhrase();
            }
            LOGGER.error("Error from service: " + e.getMessage());
            throw new CatalogException(message, e);
        } catch (Exception e) {
            LOGGER.error("Error from service: " + e.getMessage());
            throw new CatalogException(e);
        }
    }

    /**
     * Same as {@link #withCatalogException(Callable, Class)}
     */
    public static <T> PagedList<T> listWithCatalogException(Callable<CatalogWebServiceResult> callable,
            Class<T> itemClass) throws CatalogException {
        try {
            CatalogWebServiceResult result = callable.call();
            throwCatalogExceptionIfHttpError(result);
            return makeResultWithModelList(result, itemClass);
        } catch (HttpResponseException e) {
            LOGGER.error("Error from service: " + e.getMessage());
            throw new CatalogException(e.getReasonPhrase(), e);
        } catch (Exception e) {
            LOGGER.error("Error from service: " + e.getMessage());
            throw new CatalogException(e);
        }
    }

    public static void throwCatalogExceptionIfHttpError(CatalogWebServiceResult result) throws HttpResponseException {
        if (!result.is2xxOk()) {
            throw new HttpResponseException(result.getStatusCode(), HTTP_ERROR_CODE, result.getReasonPhrase());
        }
    }

    private static String getErrorCodeFromResponse(BaseResponse result) {
        return result.getErrorCode();
    }
}
