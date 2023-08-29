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

import io.polycat.catalog.common.GlobalConfig;
import io.polycat.catalog.authentication.api.Authenticator;
import io.polycat.catalog.authentication.model.LocalIdentity;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuthenticatorFactoryTest {

    @Test
    public void getAuthenticatorTest() {
        Authenticator currentAuthenticator = AuthenticatorFactory.getAuthenticator("LocalAuthenticator");
        LocalIdentity[] identities = new LocalIdentity[1];
        identities[0] = new LocalIdentity();
        identities[0].setProjectId("Guangdong_Shenzhen");
        identities[0].setAccountId("ShenZhenDaXue");
        identities[0].setUserId("Zhangli001");
        identities[0].setPasswd("Zhangli001@163.com");
        identities[0].setIdentityOwner("LocalAuthenticator");

        assertTrue(currentAuthenticator.authAndCreateToken(identities[0]).getAllowed());
        GlobalConfig.resetToDefault(GlobalConfig.IDENTITY_FILE_PATH);
    }

    @Test
    public void getAuthenticatorTestByDefault() {
        GlobalConfig.set(GlobalConfig.CONF_DIR, "../../conf");
        Authenticator currentAuthenticator = AuthenticatorFactory.getAuthenticator();
        LocalIdentity[] identities = new LocalIdentity[1];
        identities[0] = new LocalIdentity();
        identities[0].setProjectId("Guangdong_Shenzhen");
        identities[0].setAccountId("ShenZhenDaXue");
        identities[0].setUserId("Zhangli001");
        identities[0].setPasswd("Zhangli001@163.com");
        identities[0].setIdentityOwner("");

        assertTrue(currentAuthenticator.authAndCreateToken(identities[0]).getAllowed());
        GlobalConfig.resetToDefault(GlobalConfig.IDENTITY_FILE_PATH);
    }

}
