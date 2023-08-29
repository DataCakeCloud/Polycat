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
package io.polycat.catalog.client.util;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.polycat.catalog.client.Exception.CatalogClientException;
import io.polycat.catalog.client.Exception.ClientErrorCode;
import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.plugin.request.base.CatalogRequestBase;
import io.polycat.catalog.common.plugin.request.base.DatabaseRequestBase;
import io.polycat.catalog.common.plugin.request.base.ProjectRequestBase;
import io.polycat.catalog.common.plugin.request.base.TableRequestBase;


public class RequestChecker {

    static final Pattern RESOURCE_PATTERN = Pattern.compile("[a-z0-9_]*", Pattern.CASE_INSENSITIVE);
    static final Pattern PARAMETER_PATTERN = Pattern.compile("[a-z0-9-_.~!*'();:@&=+$,/?#\\]\\[]*", Pattern.CASE_INSENSITIVE);
    public static void check(ProjectRequestBase request) {
        checkResource(request);
    }

    public static void check(ProjectRequestBase request, Map<String, String> params) {
        checkResource(request);
        checkParameters(params);
    }

    private static void checkResource(ProjectRequestBase request) {
        if (request instanceof TableRequestBase) {
            TableRequestBase req = (TableRequestBase) request;
            if (!checkResourceFormat(req.getTableName())) {
                throw new CatalogClientException(ClientErrorCode.RESOURCE_FORMAT_ERROR, "table");
            }

            if (!checkResourceLength(req.getTableName())) {
                throw new CatalogClientException(ClientErrorCode.RESOURCE_LENGTH_ERROR, "table");
            }
        }
        if (request instanceof DatabaseRequestBase) {
            DatabaseRequestBase req = (DatabaseRequestBase) request;
            if (!checkResourceFormat(req.getDatabaseName())) {
                throw new CatalogClientException(ClientErrorCode.RESOURCE_FORMAT_ERROR, "database");
            }

            if (!checkResourceLength(req.getDatabaseName())) {
                throw new CatalogClientException(ClientErrorCode.RESOURCE_LENGTH_ERROR, "database");
            }
        }
        if (request instanceof CatalogRequestBase) {
            CatalogRequestBase req = (CatalogRequestBase) request;
            if (!checkResourceFormat(req.getCatalogName())) {
                throw new CatalogClientException(ClientErrorCode.RESOURCE_FORMAT_ERROR, "catalog");
            }

            if (!checkResourceLength(req.getCatalogName())) {
                throw new CatalogClientException(ClientErrorCode.RESOURCE_LENGTH_ERROR, "catalog");
            }
        }

        if (!checkResourceFormat(request.getProjectId())) {
            throw new CatalogClientException(ClientErrorCode.RESOURCE_FORMAT_ERROR, "catalog");
        }

        if (!checkResourceLength(request.getProjectId())) {
            throw new CatalogClientException(ClientErrorCode.RESOURCE_LENGTH_ERROR, "catalog");
        }
    }

    private static void checkParameters(Map<String, String> params) {
        params.forEach((key, value)->{
            if (!checkParameterFormat(value)) {
                throw new CatalogClientException(ClientErrorCode.PARAMETER_FORMAT_ERROR, key);
            }

            if (!checkParameterLength(value)) {
                throw new CatalogClientException(ClientErrorCode.PARAMETER_LENGTH_ERROR, key);
            }
        });
    }

    private static boolean checkResourceFormat(String name) {
        Matcher matcher = RESOURCE_PATTERN.matcher(name);
        return matcher.matches();
    }

    private static boolean checkParameterFormat(String name) {
        Matcher matcher = PARAMETER_PATTERN.matcher(name);
        return matcher.matches();
    }

    private static boolean checkResourceLength(String name) {
        return name.length() <= Constants.RESOURCE_MAX_LENGTH;
    }

    private static boolean checkParameterLength(String name) {
        return name.length() <= Constants.RESOURCE_MAX_LENGTH;
    }
}
