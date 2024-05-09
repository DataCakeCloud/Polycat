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

import java.util.Arrays;

import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.PrivilegeRoles;
import io.polycat.catalog.common.model.Role;
import io.polycat.catalog.common.plugin.request.ShowRolePrivilegesRequest;
import io.polycat.catalog.common.plugin.request.input.ShowRolePrivilegesInput;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class SDKRolePrivilegeTest {
    @Test
    public void test_searchTable() {
        final PolyCatClient client = SDKDiscoveryTest.getClient();
        ShowRolePrivilegesInput input = new ShowRolePrivilegesInput();
        input.setExactRoleNames(Arrays.asList("test_role2"));
        input.setExcludeRolePrefix("test_role21");
        input.setObjectType(ObjectType.TABLE.name());
        ShowRolePrivilegesRequest request = new ShowRolePrivilegesRequest(client.getProjectId(), input);
        PagedList<Role> rolePagedList = client.showRolePrivileges(request);
        for (Role role : rolePagedList.getObjects()) {
            log.info("role: {}", role);
        }
        PagedList<PrivilegeRoles> privilegeRolesPagedList = client.showPrivilegeRoles(request);
        for (PrivilegeRoles role : privilegeRolesPagedList.getObjects()) {
            log.info("role: {}", role);
        }
    }
}
