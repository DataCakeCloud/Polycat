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
package io.polycat.catalog.server.filter;

import java.util.ArrayList;

import io.polycat.catalog.audit.impl.AuditLogHelper;
import io.polycat.catalog.common.CatalogConstants;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.servlet.Filter;
import java.util.HashMap;
import java.util.List;

@Configuration
public class FilterConfigurations {

    @Getter
    private static String baseVersion = CatalogConstants.DEFAULT_VERSION;
    private static String URL_PATTERN_COMMON = "/%s/*";
    private static final String[] URL_PATTERN_COMMON_IGNORE = {"/", "/static/**", "/error/**", "/logout", "/code", "/login",
            "/inf-druid/**", "/druid/**", "/index", "/index.html", "/version", "/favicon.ico",
            "/swagger-resources/**", "/swagger-ui.html", "/doc.html/**", "/webjars/bycdao-ui/ace/ace.min.js",
            "/swagger-ui.html", "/v2/api-docs", "/swagger-resources/**", "/swagger/**", "/**/v2/api-docs", "/**/*.js"
            , "/**/*.css", "/**/*.png", "/**/*.ico", "/webjars/springfox-swagger-ui/**", "/actuator/**", "/druid/**", "/webjars/**"};

    private static final String SENSITIVE_WORDS = "password,pwd,PASSWORD";

    private static String uriDatabaseRegex = "/v1/projects/[A-Za-z0-9_]+/[A-Za-z0-9_]+/databases";
    private static String uriTableRegex = "/v1/projects/[A-Za-z0-9_]+/[A-Za-z0-9_]+/[A-Za-z0-9_]+/tables";

    @Value("${" + CatalogConstants.CONF_AUDIT_BASE_URL_VERSION + ":" + CatalogConstants.DEFAULT_VERSION + "}")
    public void initUriPath(String version) {
        baseVersion = version;
        URL_PATTERN_COMMON = String.format(URL_PATTERN_COMMON, baseVersion);
    }

    @Bean
    public FilterRegistrationBean<Filter> AuditLogFilterRegistration() {
        FilterRegistrationBean<Filter> registrationBean = new FilterRegistrationBean<Filter>();
        registrationBean.setFilter(new AuditLogFilter());
        List<String> urlPatterns = new ArrayList<String>();
        urlPatterns.add(URL_PATTERN_COMMON);
        registrationBean.setUrlPatterns(urlPatterns);
        HashMap<String, String> initParam = new HashMap<>();
        initParam.put("sensitiveWords", SENSITIVE_WORDS);
        initParam.put("notLogUrls", "");
        registrationBean.setInitParameters(initParam);
        registrationBean.setOrder(0);
        return registrationBean;
    }
}
