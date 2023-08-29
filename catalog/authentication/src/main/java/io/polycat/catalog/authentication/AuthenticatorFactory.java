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
package io.polycat.catalog.authentication;

import io.polycat.catalog.authentication.api.Authenticator;
import io.polycat.catalog.authentication.impl.LocalAuthenticator;
import io.polycat.catalog.common.GlobalConfig;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.exception.CatalogException;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

public class AuthenticatorFactory {
    /**
     * Short name map to full class name of the {@link Authenticator} implementation
     */
    private static final Map<String, Authenticator> cachedAuthenticators = new HashMap<>();

    //default Authenticator
    private static String DEFAULT_AUTHENTICATOR_NAME = "authenticator.shortName";

    private static String defaultAuthShortName = "";

    private static final Logger logger = Logger.getLogger(AuthenticatorFactory.class.getName());

    public synchronized static Authenticator getAuthenticator(String shortName) throws CatalogException {
        Authenticator authenticator = cachedAuthenticators.get(shortName);
        if (authenticator == null) {
            ServiceLoader<Authenticator> loader = ServiceLoader.load(Authenticator.class);
            for (Authenticator auth : loader) {
                if (auth.getShortName().equals(shortName)) {
                    authenticator = auth;
                    cachedAuthenticators.put(authenticator.getShortName(), auth);
                    logger.info("cache authenticator {}", shortName);
                    break;
                }
            }
            if (authenticator == null) {
                throw new CatalogException(String.format("Authenticator '%s' not found", shortName.toString()));
            }
        }
        return authenticator;
    }

    public synchronized static Authenticator getAuthenticator() throws CatalogException {
        String filePath = GlobalConfig.getString(GlobalConfig.CONF_DIR) + "/authentication.conf";
        try {
            InputStream input = FileUtils.openInputStream(FileUtils.getFile(filePath));
            Properties prop = new Properties();
            prop.load(input);
            if(prop.size() != 0) {
                defaultAuthShortName = prop.get(DEFAULT_AUTHENTICATOR_NAME).toString();
            }
            logger.info("loaded properties: " + filePath);
        } catch (IOException ex) {
            logger.error("Failed to load properties file " + filePath, ex);
            throw new CatalogException("Failed to load properties file " + filePath, ex);
        }
        return getAuthenticator(defaultAuthShortName);
    }
}
