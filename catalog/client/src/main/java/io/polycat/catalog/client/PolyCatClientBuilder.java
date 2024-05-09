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
package io.polycat.catalog.client;

import io.polycat.catalog.client.authorization.CredentialsProvider;
import io.polycat.catalog.client.authorization.DefaultCredentialsProvider;
import io.polycat.catalog.client.authorization.HadoopConfCredentialsProvider;
import io.polycat.catalog.client.endpoint.DefaultEndpointProvider;
import io.polycat.catalog.client.endpoint.EndpointProvider;
import io.polycat.catalog.client.endpoint.HadoopConfigEndpointProvider;
import org.apache.hadoop.conf.Configuration;

/**
 * @author liangyouze
 * @date 2024/2/28
 */
public class PolyCatClientBuilder {

    private CredentialsProvider credentialsProvider;
    private EndpointProvider endpointProvider;

    private PolyCatClientBuilder() {

    };

    public PolyCatClientBuilder withCredentials(CredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
        return this;
    }

    public PolyCatClientBuilder withEndpointProvider(EndpointProvider endpointProvider) {
        this.endpointProvider = endpointProvider;
        return this;
    }

    public static PolyCatClientBuilder standard() {
        return new PolyCatClientBuilder()
                .withEndpointProvider(new DefaultEndpointProvider())
                .withCredentials(new DefaultCredentialsProvider());
    }

    public Client build() {
        return new PolyCatClientV2(endpointProvider, credentialsProvider);
    }

    public static Client buildWithHadoopConf(Configuration configuration) {
        return new PolyCatClientBuilder()
                .withEndpointProvider(new HadoopConfigEndpointProvider(configuration))
                .withCredentials(new HadoopConfCredentialsProvider(configuration))
                .build();
    }

}
