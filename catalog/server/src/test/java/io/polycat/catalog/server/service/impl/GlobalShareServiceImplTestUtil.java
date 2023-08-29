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
package io.polycat.catalog.server.service.impl;

import io.polycat.catalog.common.model.Share;
import io.polycat.catalog.common.plugin.request.input.ShareInput;
import io.polycat.catalog.service.api.GlobalShareService;
import io.polycat.catalog.store.api.VersionManager;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@Disabled
public class GlobalShareServiceImplTestUtil extends  TestUtil{

    @Autowired
    private GlobalShareService globalShareService;

    @Test
    public void createShareTest() {
        String shareNameString = "testShare1";
        String account = "abc";

        ShareInput shareInput = new ShareInput();
        shareInput.setShareName(shareNameString);
        shareInput.setAccountId(account);
        shareInput.setUserId(userId);

        globalShareService.createShare(projectId, shareInput);

        Share share = globalShareService.getShareByName(projectId, shareNameString);
        assertNotNull(share);
        assertEquals(share.getShareName(), shareNameString);
        assertEquals(share.getOwnerAccount(), account);

        //drop share
        globalShareService.dropShareByName(projectId, shareNameString);
//        Share share1 = shareService.getShareByName(shareName);
//        assertEquals(share1, null);
    }

    @Test
    public void shareAddAccountTest() {
        String shareNameString = "testShare1";
        String account = "abc";

        ShareInput shareInput = new ShareInput();
        shareInput.setShareName(shareNameString);
        shareInput.setAccountId(account);
        shareInput.setUserId(userId);
        globalShareService.createShare(projectId, shareInput);

        String[] accounts = {account};
        shareInput.setAccountIds(accounts);
        shareInput.setUsers(accounts);
        globalShareService.addConsumersToShare(projectId, shareNameString, shareInput);
        String[] users = {"xy12345", "pz12345"};
        shareInput.setUsers(users);
        globalShareService.addUsersToShareConsumer(projectId, shareNameString, shareInput);

        Share share = globalShareService.getShareByName(projectId, shareNameString);
        assertNotNull(share);
        assertEquals(share.getShareName(), shareNameString);
        assertEquals(share.getOwnerAccount(), account);
        globalShareService.removeUsersFromShareConsumer(projectId, shareNameString, shareInput);
        globalShareService.removeConsumersFromShare(projectId, shareNameString, shareInput);
        globalShareService.dropShareByName(projectId, shareNameString);
    }
}
