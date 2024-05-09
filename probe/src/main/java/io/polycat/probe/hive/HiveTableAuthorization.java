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
package io.polycat.probe.hive;

import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.AuthorizationResponse;
import io.polycat.catalog.common.plugin.request.AuthenticationRequest;
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;
import io.polycat.probe.PolyCatClientUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author renjianxu
 * @date 2023/7/11
 */
public class HiveTableAuthorization {
    private static final Logger LOG = LoggerFactory.getLogger(HiveTableAuthorization.class);
    private PolyCatClient polyCatClient;
    private String polyCatCatalog;
    private String defaultDbName;

    public HiveTableAuthorization(Configuration conf, String polyCatCatalog, String defaultDbName) {
        this.polyCatClient = PolyCatClientUtil.buildPolyCatClientFromConfig(conf);
        this.polyCatCatalog = polyCatCatalog;
        this.defaultDbName = defaultDbName;
    }

    public List<AuthorizationInput> checkAuthorization(
            Map<Operation, Set<String>> operationObjects) {
        // build authorizationInput
        List<AuthorizationInput> authorizationInputs =
                PolyCatClientUtil.createAuthorizationInputByOperationObjects(
                        polyCatClient.getProjectId(),
                        polyCatCatalog,
                        polyCatClient.getContext().getUser(),
                        polyCatClient.getContext().getToken(),
                        defaultDbName,
                        operationObjects);

        List<AuthorizationInput> authorizationNotAllowedList = new ArrayList<>();
        for (AuthorizationInput authorization : authorizationInputs) {
            LOG.info(String.format("authorization info:%s\n", authorization));
            try {
                AuthorizationResponse response =
                        polyCatClient.authenticate(
                                new AuthenticationRequest(
                                        polyCatClient.getProjectId(),
                                        Collections.singletonList(authorization)));
                if (!response.getAllowed()) {
                    LOG.info(
                            String.format(
                                    "object [%s] no permission [%s] \n",
                                    authorization.getCatalogInnerObject().getObjectName(),
                                    authorization.getOperation().getPrintName()));
                    authorizationNotAllowedList.add(authorization);
                }
            } catch (CatalogException e) {
                if (e.getStatusCode() == 404) {
                    String msg =
                            String.format(
                                    "object [%s] not found error!",
                                    authorization.getCatalogInnerObject().getObjectName());
                    LOG.error(msg, e);
                    throw new CatalogException(msg, e);
                } else {
                    throw new RuntimeException(e);
                }
            }
        }
        return authorizationNotAllowedList;
    }
}
