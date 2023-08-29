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

import io.polycat.catalog.authentication.Authentication;
import io.polycat.catalog.authentication.model.AuthenticationResult;
import io.polycat.catalog.authentication.model.Identity;
import io.polycat.catalog.common.GlobalConfig;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.plugin.CatalogContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class CatalogUserInformation {

    public final static String POLYCAT_USER_NAME = "polycat.user.name";
    public final static String POLYCAT_USER_TENANT = "polycat.user.tenant";
    public final static String POLYCAT_USER_PASSWORD = "polycat.user.password";
    public final static String POLYCAT_USER_PROJECT = "polycat.user.project";
    public final static String POLYCAT_USER_TOKEN = "polycat.user.token";

    private final static ThreadLocal<CatalogContext> catalogContext = new InheritableThreadLocal<>();

    public static CatalogContext getCurrentUser() {
        return catalogContext.get();
    }

    public static void setCurrentUser(CatalogContext context) {
        catalogContext.set(context);
    }

    public static CatalogContext login(String userName, String password) {
        CatalogContext context = doAuth(userName, password);
        setCurrentUser(context);
        return context;
    }

    public static CatalogContext doAuth(String userName, String password) {
        String authImpl = GlobalConfig.getString(GlobalConfig.AUTH_IMPL);
        AuthenticationResult result;
        Identity identity = new Identity();
        identity.setUserId(userName);
        identity.setPasswd(password);
        identity.setIdentityOwner(authImpl);
        result = Authentication.authAndCreateToken(identity);
        if (result.getAllowed()) {
            return new CatalogContext(result.getProjectId(), userName, result.getAccountId(), result.getToken());
        } else {
            throw new CatalogException("user_name or password error");
        }
    }

    public static CatalogContext createContextFromConf(Configuration conf) {
        String userName = null;
        String password = null;
        String token = null;
        String project = null;
        String tenant = null;
        if (conf != null) {
            userName = conf.get(POLYCAT_USER_NAME, conf.get("spark." + POLYCAT_USER_NAME));
            password = conf.get(POLYCAT_USER_PASSWORD, conf.get("spark." + POLYCAT_USER_PASSWORD));
            token = conf.get(POLYCAT_USER_TOKEN, conf.get("spark." + POLYCAT_USER_TOKEN));
            project = conf.get(POLYCAT_USER_PROJECT, conf.get("spark." + POLYCAT_USER_PROJECT));
            tenant = conf.get(POLYCAT_USER_TENANT, conf.get("spark." + POLYCAT_USER_TENANT));
        }

        if (StringUtils.isBlank(userName)) {
            userName = System.getenv("USER_NAME");
            password = System.getenv("PASSWORD");
        }
        if (StringUtils.isBlank(userName)) {
            return null;
        }
        CatalogContext context;
        if (StringUtils.isBlank(password)) {
            context = new CatalogContext(project, userName, tenant, token);
        } else {
            context = doAuth(userName, password);
            if (context.getProjectId() == null) {
                context.setProjectId(project);
            }
            if (context.getTenantName() == null) {
                context.setTenantName(tenant);
            }
        }
        return context;
    }

    public static void logoutCurrentUser() {
        catalogContext.remove();
    }

}
