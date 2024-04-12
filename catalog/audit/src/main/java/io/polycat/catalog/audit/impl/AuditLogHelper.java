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
package io.polycat.catalog.audit.impl;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import io.polycat.catalog.common.CatalogConstants;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.AuditLog;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.utils.UuidUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerMapping;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

@Component
public class AuditLogHelper {

    private static final Logger log = Logger.getLogger(AuditLogHelper.class);

    public static final String DEFAULT_USER_KEY = "userId";
    public static final String DEFAULT_USER = "Unknown";

    private static final String NAME_LEGAL_CHARACTERS = "[^0-9][\\w]*";
    private static Pattern uriCatalogRegex;
    private static Pattern uriDatabaseRegex;
    private static Pattern uriTableRegex;
    private static Pattern uriFunctionRegex;
    private static Pattern uriPartitionRegex;
    private static Pattern uriRoleRegex;
    private static Pattern uriShareRegex;
    private static Pattern uriUsageProfileRegex;
    private static Pattern uriAuthenticationRegex;

    private static ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
    }

    /**
     * Need to initialize Uri path first
     *
     * @param baseVersion
     */
    public static void initUriPath(String baseVersion) {
        uriCatalogRegex = Pattern.compile(String.format("/%s/%s/catalogs.*", baseVersion, NAME_LEGAL_CHARACTERS));
        uriDatabaseRegex = Pattern.compile(String.format("/%s/%s/catalogs/%s/databases.*", baseVersion, NAME_LEGAL_CHARACTERS, NAME_LEGAL_CHARACTERS));
        uriTableRegex = Pattern.compile(String.format("/%s/%s/catalogs/%s/databases/%s/tables.*", baseVersion, NAME_LEGAL_CHARACTERS, NAME_LEGAL_CHARACTERS, NAME_LEGAL_CHARACTERS));
        // /v1/xxxxxxxx/catalogs/xxxxxxxx_sg2/databases/default/functions/list
        uriFunctionRegex = Pattern.compile(String.format("/%s/%s/catalogs/%s/databases/%s/functions.*", baseVersion, NAME_LEGAL_CHARACTERS, NAME_LEGAL_CHARACTERS, NAME_LEGAL_CHARACTERS));
        uriPartitionRegex = Pattern.compile(String.format("/%s/%s/catalogs/%s/databases/%s/tables/%s/partitions.*", baseVersion, NAME_LEGAL_CHARACTERS, NAME_LEGAL_CHARACTERS, NAME_LEGAL_CHARACTERS, NAME_LEGAL_CHARACTERS));
        uriRoleRegex = Pattern.compile(String.format("/%s/%s/roles.*", baseVersion, NAME_LEGAL_CHARACTERS));
        uriShareRegex = Pattern.compile(String.format("/%s/%s/shares.*", baseVersion, NAME_LEGAL_CHARACTERS));
        uriUsageProfileRegex = Pattern.compile(String.format("/%s/%s/usageprofiles.*", baseVersion, NAME_LEGAL_CHARACTERS));
        uriAuthenticationRegex = Pattern.compile(String.format("/%s/%s/authentication.*", baseVersion, NAME_LEGAL_CHARACTERS));
    }

    public static AuditLog getAuditLog(HttpServletRequest request) {
        ObjectType objectType = getObjectTypeByUri(request.getRequestURI());
        Map<String, Object> requestParam = getRequestParam(request, null);
        String projectId = (String) requestParam.get(CatalogConstants.CATALOG_REQUEST_PATH_VAR_PROJECT);
        requestParam.remove(CatalogConstants.CATALOG_REQUEST_PATH_VAR_PROJECT);
        //Construct an audit log
        return AuditLog.builder()
                .id(UuidUtil.generateUUID32())
                .timestamp(System.currentTimeMillis())
                .pathUri(request.getRequestURI())
                .method(request.getMethod())
                .objectType(objectType)
                .sourceIp(request.getRemoteAddr())
                .remotePort(request.getRemotePort())
                .userId(getUser(request))
                .projectId(projectId)
                .requestParams(requestParam)
                .build();
    }

    public static AuditLog getAuditLog(HttpServletRequest request, byte[] requestBufferBytes) {
        ObjectType objectType = getObjectTypeByUri(request.getRequestURI());
        Map<String, Object> requestParam = getRequestParam(request, requestBufferBytes);
        String projectId = (String) requestParam.get(CatalogConstants.CATALOG_REQUEST_PATH_VAR_PROJECT);
        requestParam.remove(CatalogConstants.CATALOG_REQUEST_PATH_VAR_PROJECT);
        //Construct an audit log
        return AuditLog.builder()
                .id(UuidUtil.generateUUID32())
                .timestamp(System.currentTimeMillis())
                .pathUri(request.getRequestURI())
                .method(request.getMethod())
                .objectType(objectType)
                .sourceIp(request.getRemoteAddr())
                .remotePort(request.getRemotePort())
                .userId(getUser(request))
                .projectId(projectId)
                .requestParams(requestParam)
                .build();
    }

    public static Map<String, Object> getRequestParam(HttpServletRequest request, byte[] requestBufferBytes) {
        Map<String, String[]> paramVariables = (Map) request.getParameterMap();
        HashMap<String, Object> map = new HashMap<>();
        map.putAll(paramVariables);
        Map<String, Object> pathVariables = getAttributeVariables(request);
        map.putAll(pathVariables);
        if (requestBufferBytes != null) {
            String bodyData = AuditLogHelper.getString(requestBufferBytes);
            map.put("requestBodyData", bodyData);
        }
        return map;
    }

    public static String getRequestParamString(HttpServletRequest request) {
        Map<String, String[]> paramVariables = (Map) request.getParameterMap();
        StringBuilder requestParams = new StringBuilder();
        for (String key : paramVariables.keySet()) {
            List<String> values = Arrays.asList(paramVariables.get(key));
            requestParams.append(key).append(":").append(values).append(";");
        }
        return requestParams.toString();
    }

    public static ObjectType getObjectTypeByUri(String uriPath) {
        if (uriAuthenticationRegex.matcher(uriPath).matches()) {
            return ObjectType.ROLE;
        } else if (uriRoleRegex.matcher(uriPath).matches()) {
            return ObjectType.ROLE;
        } else if (uriTableRegex.matcher(uriPath).matches()) {
            return ObjectType.TABLE;
        } else if (uriFunctionRegex.matcher(uriPath).matches()) {
            return ObjectType.FUNCTION;
        } else if (uriDatabaseRegex.matcher(uriPath).matches()) {
            return ObjectType.DATABASE;
        } else if (uriCatalogRegex.matcher(uriPath).matches()) {
            return ObjectType.CATALOG;
        } else if (uriShareRegex.matcher(uriPath).matches()) {
            return ObjectType.SHARE;
        }
        return ObjectType.OTHERS;
    }

    public static boolean skipRegexUriPath(String uriPath) {
        if (uriUsageProfileRegex.matcher(uriPath).matches()) {
            return true;
        }
        return false;
    }

    public static boolean matchIgnoreMethod(String[] ignoreMethods, HttpServletRequest request) {
        if (!ArrayUtils.isEmpty(ignoreMethods)) {
            for (String method : ignoreMethods) {
                if (Objects.equals(method, request.getMethod())) {
                    return true;
                }
            }
        }
        return false;
    }


    public static String getUserFromContent(byte[] bytes) {
        String user = DEFAULT_USER;
        String reqBody;
        reqBody = getString(bytes);
        if (StringUtils.isBlank(reqBody)) {
            return user;
        }

        try {
            // search not performed
            JsonNode paramValue = objectMapper.readValue(reqBody, JsonNode.class);
            if (null != paramValue) {
                return String.valueOf(paramValue.get(DEFAULT_USER_KEY));
            }
        } catch (JsonParseException e) {
            log.error(e.getMessage());
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return user;
    }

    public static String getString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static String getUser(HttpServletRequest request) {
        /*
        //TODO Waiting for the authentication logic to be completed
        TokenParseResult tokenParseResult = null;
        String localToken = request.getHeader("authorization");
        if (!StringUtils.isEmpty(localToken)) {
            try {
                tokenParseResult = Authentication.CheckAndParseToken("LocalAuthenticator", localToken);
            } catch (Exception e) {
                log.debug("check local token error " + e);
            }
        }
        if (null != tokenParseResult && null != tokenParseResult.getUserId() && null != tokenParseResult.getAccountId()) {
            user = tokenParseResult.getAccountId() + "@" + tokenParseResult.getUserId();
        }*/
        return request.getRemoteUser() == null ? DEFAULT_USER : request.getRemoteUser();
    }

    public static String getRequestProjectId(HttpServletRequest request) {
        Map<String, Object> pathVariables = getAttributeVariables(request);
        return (String) pathVariables.get(CatalogConstants.CATALOG_REQUEST_PATH_VAR_PROJECT);
    }

    private static Map<String, Object> getAttributeVariables(HttpServletRequest request) {
        Object attribute = request.getAttribute(HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE);
        if (attribute == null) {
            return new LinkedHashMap<>();
        }
        return (Map<String, Object>) attribute;
    }

    public static void setAuditLogResponseAttr(HttpServletResponse response, AuditLog auditLog, String data) {
        HttpStatus httpStatus = HttpStatus.resolve(response.getStatus());
        if (httpStatus != null) {
            if (httpStatus.isError()) {
                auditLog.setDetail(httpStatus.getReasonPhrase());
            }
            auditLog.setResponse(getResponseData(data));
            auditLog.setState(httpStatus.series().toString());
        } else {
            auditLog.setState(HttpStatus.OK.series().toString());
        }
    }

    public static BaseResponse getResponseData(String data) {
        return new Gson().fromJson(data, BaseResponse.class);
    }
}
