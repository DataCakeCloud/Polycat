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

import io.polycat.catalog.common.model.lock.LockInfo;
import io.polycat.catalog.common.plugin.request.CreateLockRequest;
import io.polycat.catalog.common.plugin.request.ShowLocksRequest;
import io.polycat.catalog.common.plugin.request.input.LockInput;
import org.junit.Test;

import java.util.List;

/**
 * @author liangyouze
 * @date 2024/2/21
 */
public class SDKLockTest {

    protected static PolyCatClient getClient() {
        PolyCatClient client = new PolyCatClient();
        client.setContext(CatalogUserInformation.doAuth("test", "dash"));
        client.getContext().setTenantName("shenzhen");
        client.getContext().setProjectId("default_project");
        client.getContext().setToken("nonull");
        return client;
    }

    @Test
    public void testLock() {
        final PolyCatClient client = getClient();
        final CreateLockRequest createLockRequest = new CreateLockRequest();
        final LockInput lockInput = new LockInput("test_default_catalog1.default.test1", "TABLE", "user1", "127.0.0.1");
        createLockRequest.setInput(lockInput);
        createLockRequest.setProjectId(client.getProjectId());
        // client.lock(createLockRequest);
        // client.checkLock(new CheckLockRequest(client.getProjectId(), 1L));
        // client.heartbeat(new LockHeartbeatRequest(client.getProjectId(), 1L));
        // client.unlock(new UnlockRequest(client.getProjectId(), 1L));
        final List<LockInfo> lockInfos = client.showLocks(new ShowLocksRequest(client.getProjectId()));
        System.out.println(lockInfos);
    }
}
