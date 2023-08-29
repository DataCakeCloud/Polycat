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
import io.polycat.catalog.authentication.model.LocalIdentity;
import io.polycat.catalog.common.GlobalConfig;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AuthenticationTest {
    @Test
    public void authRequestTest() throws IOException {
        GlobalConfig.set(GlobalConfig.CONF_DIR, "../../conf");
        LocalIdentity[] identities = new LocalIdentity[3];
        identities[0] = new LocalIdentity();
        identities[0].setProjectId("shenzhen");
        identities[0].setAccountId("tenantA");
        identities[0].setUserId("amy");
        identities[0].setPasswd("huawei");
        identities[0].setIdentityOwner("LocalAuthenticator");


        identities[1] = new LocalIdentity();
        identities[1].setProjectId("shenzhen");
        identities[1].setAccountId("tenantA");
        identities[1].setUserId("bob");
        identities[1].setPasswd("huawei");
        identities[1].setIdentityOwner("");

        assertEquals(Authentication.authAndCreateToken(identities[0]).getAllowed(), true);
        assertEquals(Authentication.authAndCreateToken(identities[1]).getAllowed(), true);
    }
}
