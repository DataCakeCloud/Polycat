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
package io.polycat.catalog.common;

public interface CatalogConstants {

    String CATALOG_REQUEST_PATH_VAR_PROJECT = "project-id";

    /**
     * HTTP
     */
    String HTTP_HEADER_CONTENT_TYPE_KEY = "Content-Type";
    String HTTP_HEADER_CONTENT_TYPE_VALUE = "application/json;charset=UTF-8";
    /**
     * http default version
     */
    String DEFAULT_VERSION = "v1";
    String CONF_AUDIT_BASE_URL_VERSION = "audit.base.url.version";
    String CONF_AUDIT_ENABLE = "audit.enable";

    /**
     * audit skip method
     */
    String[] AUDIT_IGNORE_METHOD = {"HEAD", "OPTIONS", "TRACE", "CONNECT"};
}
