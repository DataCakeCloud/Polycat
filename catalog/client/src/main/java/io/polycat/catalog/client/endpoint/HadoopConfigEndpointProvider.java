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

import io.polycat.catalog.client.Exception.CatalogClientException;
import io.polycat.catalog.client.Exception.ClientErrorCode;
import io.polycat.catalog.client.util.Constants;
import org.apache.hadoop.conf.Configuration;

/**
 * @author liangyouze
 * @date 2024/2/28
 */
public class HadoopConfigEndpointProvider implements EndpointProvider{

    private String catalogHost;
    private String catalogPort;

    public HadoopConfigEndpointProvider(Configuration conf) {
        if (conf != null) {
            this.catalogHost = conf.get(Constants.POLYCAT_CLIENT_HOST, "127.0.0.1");
            this.catalogPort = conf.get(Constants.POLYCAT_CLIENT_PORT, "8082");
        }
        if (catalogHost == null) {
            throw new CatalogClientException(ClientErrorCode.PARAMETER_CANNOT_NULL_ERROR,
                    Constants.POLYCAT_CLIENT_HOST);
        }
    }

    public HadoopConfigEndpointProvider() {
        this(new Configuration());
    }

    @Override
    public String getEndpoint() {
        return catalogHost + ":" + catalogPort;
    }
}
