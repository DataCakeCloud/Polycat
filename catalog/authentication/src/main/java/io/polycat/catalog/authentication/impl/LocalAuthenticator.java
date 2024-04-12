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
package io.polycat.catalog.authentication.impl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;

import io.polycat.catalog.authentication.api.Authenticator;
import io.polycat.catalog.authentication.model.Identity;
import io.polycat.catalog.authentication.model.LocalIdentity;
import io.polycat.catalog.authentication.model.LocalToken;
import io.polycat.catalog.authentication.model.TokenParseResult;
import io.polycat.catalog.authentication.model.AuthenticationResult;
import io.polycat.catalog.common.GlobalConfig;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.exception.CatalogException;

import org.apache.commons.lang3.StringUtils;

/*
Local authenticator, used only in test scenarios.
 */
public class LocalAuthenticator implements Authenticator {

    private static final Logger logger = Logger.getLogger(LocalAuthenticator.class.getName());

    //local user information
    private final List<LocalIdentity> identities = new LinkedList<>();

    //让构造函数为 private，这样该类就不会被实例化
    public LocalAuthenticator() throws IOException {
        try {
            BufferedReader reader = getIdentityFile();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] strIdentity = line.split(":");
                LocalIdentity identity = new LocalIdentity();
                identity.setAccountId(strIdentity[0]);
                identity.setProjectId(strIdentity[1]);
                identity.setUserId(strIdentity[2]);
                identity.setPasswd(strIdentity[3]);
                identity.setIdentityOwner("LocalAuthenticator");
                identities.add(identity);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw e;
        }

        if (identities.isEmpty()) {
            throw new CatalogException("no identity is configured");
        } else {
            logger.info(identities.size() + " identities added");
        }
    }

    public BufferedReader getIdentityFile() throws FileNotFoundException {
        String filePath = GlobalConfig.getString(GlobalConfig.IDENTITY_FILE_PATH);
        if (StringUtils.isBlank(filePath)) {
            // get from default configuration file named identity.conf
            filePath = getClass().getClassLoader().getResource("identity.conf").getPath();
            logger.info("using identity conf file = " + filePath);
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("identity.conf");
            if (inputStream == null) {
                logger.error("identity.conf file not found in resources");
                throw new FileNotFoundException("identity.conf file not found in resources");
            } else {
                return new BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            }
        } else {
            // get from user setting location
            logger.info("using identity conf file = " + filePath);
            return new BufferedReader(new FileReader(filePath));
        }
    }

    public String CreateUserTokenByPassword(Identity identity) throws RuntimeException {
        LocalIdentity localIdentity = (LocalIdentity) identity;

        LocalToken localToken = new LocalToken();
        localToken.setAccountId(localIdentity.getAccountId());
        localToken.setUserId(localIdentity.getUserId());
        localToken.setPasswd(localIdentity.getPasswd());

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(256);
        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(localToken);
            objectOutputStream.flush();
            objectOutputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String base64encodedString = Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());

        return base64encodedString;
    }

    private static LocalIdentity converToLocalIdentity(Identity identity){
        LocalIdentity localIdentity = new LocalIdentity();
        localIdentity.setUserId(identity.getUserId());
        localIdentity.setPasswd(identity.getPasswd());
        localIdentity.setIdentityOwner(identity.getIdentityOwner());
        return localIdentity;
    }

    @Override
    public TokenParseResult CheckAndParseToken(String token) throws IOException, ClassNotFoundException {
        TokenParseResult result = new TokenParseResult();
        byte[] bytes = Base64.getDecoder().decode(token);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        LocalToken localToken = (LocalToken) objectInputStream.readObject();
        LocalIdentity identity = new LocalIdentity();
        identity.setIdentityOwner("LocalAuthenticator");
        identity.setPasswd(localToken.getPasswd());
        identity.setAccountId(localToken.getAccountId());
        identity.setUserId(localToken.getUserId());

        for (LocalIdentity value : identities) {
            if ((identity.getUserId().equals(value.getUserId()))
                && (identity.getPasswd().equals(value.getPasswd()))) {
                result.setAllowed(true);
                result.setAccountId(identity.getAccountId());
                result.setUserId(identity.getUserId());
                return result;
            }
        }
        result.setAllowed(false);
        return result;
    }

    @Override
    public AuthenticationResult authAndCreateToken(Identity identity) throws RuntimeException {
        LocalIdentity localIdentity = converToLocalIdentity(identity);
        AuthenticationResult result = new AuthenticationResult();
        for (LocalIdentity value : identities) {
            if ((localIdentity.getUserId().equals(value.getUserId()))
                && (localIdentity.getPasswd().equals(value.getPasswd()))) {
                result.setToken(CreateUserTokenByPassword(value));
                result.setAllowed(true);
                result.setAccountId(value.getAccountId());
                result.setProjectId(value.getProjectId());
                return result;
            }
        }

        result.setAllowed(false);
        return result;
    }

    @Override
    public String getShortName() {
        return "LocalAuthenticator";
    }
}
