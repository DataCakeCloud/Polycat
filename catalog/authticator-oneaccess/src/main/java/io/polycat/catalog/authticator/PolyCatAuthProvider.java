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
package io.polycat.catalog.authticator;


import javax.security.sasl.AuthenticationException;

import io.polycat.catalog.client.CatalogUserInformation;
import io.polycat.catalog.common.exception.CatalogException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;

public class PolyCatAuthProvider implements PasswdAuthenticationProvider, Configurable {

    private Configuration conf;

    public PolyCatAuthProvider() {
    }

    @Override
    public void Authenticate(String user, String password) throws AuthenticationException {
        CatalogUserInformation.logoutCurrentUser();
        try {
            CatalogUserInformation.login(user, password);
        } catch (CatalogException catalogException) {
            throw new AuthenticationException(catalogException.getMessage());
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
