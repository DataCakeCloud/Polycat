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
package io.polycat.catalog.client.endpoint;

import org.apache.commons.lang3.StringUtils;

/**
 * @author liangyouze
 * @date 2024/2/28
 */
public class DefaultEndpointProvider implements EndpointProvider{
    @Override
    public String getEndpoint() {
        String catalogHost = System.getenv("CATALOG_HOST");
        if (StringUtils.isEmpty(catalogHost)) {
            catalogHost = System.getProperty("CATALOG_HOST");
        }

        if (StringUtils.isEmpty(catalogHost)) {
            catalogHost = "127.0.0.1";
        }
        String catalogPort = System.getenv("CATALOG_PORT");
        if (StringUtils.isEmpty(catalogPort)) {
            catalogPort = System.getProperty("CATALOG_PORT");
        }
        if (StringUtils.isEmpty(catalogPort)) {
            catalogPort = "8082";
        }
        return catalogHost + ":" + catalogPort;
    }
}
