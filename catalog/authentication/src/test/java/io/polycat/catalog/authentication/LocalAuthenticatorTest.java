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

import java.io.IOException;
import io.polycat.catalog.authentication.impl.LocalAuthenticator;
import io.polycat.catalog.authentication.model.LocalIdentity;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LocalAuthenticatorTest {
    public static LocalAuthenticator localAuthenticator;

    @BeforeAll
    public static void setup() throws IOException {
        localAuthenticator = new LocalAuthenticator();
    }

    @Test
    public void accessAuthTest() throws IOException {
        LocalIdentity[] identities = new LocalIdentity[3];
        identities[0] = new LocalIdentity();
        identities[0].setProjectId("Guangdong_Shenzhen");
        identities[0].setAccountId("ShenZhenDaXue");
        identities[0].setUserId("Zhangli001");
        identities[0].setPasswd("Zhangli001@163.com");

        identities[1] = new LocalIdentity();
        identities[1].setProjectId("zhengjiang_Hangzhou");
        identities[1].setAccountId("ZheJiangDaXue");
        identities[1].setUserId("Wangwei008");
        identities[1].setPasswd("Wangwei008@165.com");

        identities[2] = new LocalIdentity();
        identities[2].setProjectId("project1");
        identities[2].setAccountId("tenantA");
        identities[2].setUserId("wukong");
        identities[2].setPasswd("pwd");

        assertEquals(localAuthenticator.authAndCreateToken(identities[0]).getAllowed(), true);
        assertEquals(localAuthenticator.authAndCreateToken(identities[1]).getAllowed(), false);

        // print token for testing purpose
        System.out.println(localAuthenticator.authAndCreateToken(identities[2]).getToken());
    }

    @Test
    public void createUserTokenByPasswordTest() throws IOException, ClassNotFoundException {
        LocalIdentity[] identities = new LocalIdentity[2];
        identities[0] = new LocalIdentity();
        identities[0].setProjectId("Guangdong_Shenzhen");
        identities[0].setAccountId("ShenZhenDaXue");
        identities[0].setUserId("Zhangli001");
        identities[0].setPasswd("Zhangli001@163.com");
        identities[0].setIdentityOwner("LocalAuthenticator");

        identities[1] = new LocalIdentity();
        identities[1].setProjectId("zhengjiang_Hangzhou");
        identities[1].setAccountId("ZheJiangDaXue");
        identities[1].setUserId("Wangwei008");
        identities[1].setPasswd("Wangwei008@165.com");
        identities[1].setIdentityOwner("LocalAuthenticator");

        String strToken = localAuthenticator.CreateUserTokenByPassword(identities[0]);
        assertEquals(localAuthenticator.CheckAndParseToken(strToken).getAllowed(), true);

        String strToken1 = localAuthenticator.CreateUserTokenByPassword(identities[1]);
        assertEquals(localAuthenticator.CheckAndParseToken(strToken1).getAllowed(), false);
    }
}
