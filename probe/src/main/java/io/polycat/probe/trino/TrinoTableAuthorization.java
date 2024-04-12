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
package io.polycat.probe.trino;

import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.AuthorizationResponse;
import io.polycat.catalog.common.plugin.request.AuthenticationRequest;
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;
import io.polycat.probe.PolyCatClientUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrinoTableAuthorization {
    private static final Logger LOG = LoggerFactory.getLogger(TrinoTableAuthorization.class);

    private PolyCatClient polyCatClient;
    private String polyCatCatalog;
    private String defaultDbName;
    private Configuration configuration;

    public TrinoTableAuthorization(
            Configuration conf, String polyCatCatalog, String defaultDbName) {
        this.polyCatClient = PolyCatClientUtil.buildPolyCatClientFromConfig(conf);
        this.polyCatCatalog = polyCatCatalog;
        this.defaultDbName = defaultDbName;
        this.configuration = conf;
    }

    public List<AuthorizationInput> parseSqlAndCheckAuthorization(
            Map<Operation, Set<String>> operationAndTablesName) throws CatalogException {
        // create authorizationInputs
        List<AuthorizationInput> authorizationInputs =
                PolyCatClientUtil.createAuthorizationInputByOperationObjects(
                        polyCatClient.getProjectId(),
                        polyCatCatalog,
                        polyCatClient.getContext().getUser(),
                        polyCatClient.getContext().getToken(),
                        defaultDbName,
                        operationAndTablesName);
        // permission check
        List<AuthorizationInput> authorizationNotAllowedList = new ArrayList<>();
        if (!authorizationInputs.isEmpty()) {
            for (AuthorizationInput authorization : authorizationInputs) {
                LOG.info(String.format("authorization info:%s\n", authorization));
                try {
                    AuthorizationResponse response =
                            polyCatClient.authenticate(
                                    new AuthenticationRequest(
                                            polyCatClient.getProjectId(),
                                            Arrays.asList(authorization)));
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
                                        "table [%s] not found error!",
                                        authorization.getCatalogInnerObject().getObjectName());
                        LOG.error(msg, e);
                        throw new CatalogException(msg, e);
                    } else {
                        throw new CatalogException("table authenticate error!", e);
                    }
                }
            }
        }
        return authorizationNotAllowedList;
    }
}
