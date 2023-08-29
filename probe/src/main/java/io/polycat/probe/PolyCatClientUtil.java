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
package io.polycat.probe;

import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.plugin.CatalogContext;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolyCatClientUtil {
    private static final Logger LOG = LoggerFactory.getLogger(PolyCatClientUtil.class);

    private static final String POLYCAT_CLIENT_HOST = "polycat.client.host";
    private static final String POLYCAT_CLIENT_PORT = "polycat.client.port";
    private static final String POLYCAT_CLIENT_USERNAME = "polycat.client.userName";
    private static final String POLYCAT_CLIENT_PASSWORD = "polycat.client.password";
    private static final String POLYCAT_CLIENT_PROJECT_ID = "polycat.client.projectId";
    private static final String POLYCAT_CLIENT_TOKEN = "polycat.client.token";
    private static final String POLYCAT_CLIENT_TENANT_NAME = "polycat.client.tenantName";

    public static PolyCatClient buildPolyCatClientFromConfig(Configuration conf) {
        PolyCatClient polyCatClient =
                new PolyCatClient(
                        conf.get(POLYCAT_CLIENT_HOST), conf.getInt(POLYCAT_CLIENT_PORT, 80));
        try {
            String projectId = conf.get(POLYCAT_CLIENT_PROJECT_ID);
            String polyCatUserName = conf.get(POLYCAT_CLIENT_USERNAME);
            if (StringUtils.isEmpty(polyCatUserName)) {
                final String ugiUserName = UserGroupInformation.getCurrentUser().getUserName();
                final String[] project2User = ugiUserName.split("#");
                if (project2User.length == 2) {
                    projectId = project2User[0];
                    polyCatUserName = project2User[1];
                } else {
                    polyCatUserName = ugiUserName;
                }
            }

            CatalogContext catalogContext = null;
            String token = conf.get(POLYCAT_CLIENT_TOKEN);
            String password = conf.get(POLYCAT_CLIENT_PASSWORD);

            String tenantName = conf.get(POLYCAT_CLIENT_TENANT_NAME);

            if (StringUtils.isNotEmpty(password)) {
                catalogContext = CatalogUserInformation.doAuth(polyCatUserName, password);
                catalogContext.setProjectId(projectId);
                catalogContext.setTenantName(tenantName);
            } else if (StringUtils.isNotEmpty(token)) {
                catalogContext = new CatalogContext(projectId, polyCatUserName, tenantName, token);
            } else {
                throw new CatalogException(
                        "param [polycat.client.password] or [polycat.client.token] is required!");
            }
            polyCatClient.setContext(catalogContext);
            LOG.info("current polyCat userName:{}", polyCatClient.getUserName());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return polyCatClient;
    }
}
