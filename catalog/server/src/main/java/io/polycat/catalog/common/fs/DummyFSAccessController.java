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
package io.polycat.catalog.common.fs;

/**
 * @author liangyouze
 * @date 2023/7/21
 */
public class DummyFSAccessController implements FSAccessController{

    @Override
    public void grantPrivilege(String userName, String userGroupName, FSOperation operation, String location, Boolean recursive) {
        System.out.println(String.format("Succeed grant %s %s privilege to %s:%s", operation, location, userName, userGroupName));
    }

    @Override
    public void revokePrivilege(String userName, String userGroupName, FSOperation operation, String location, Boolean recursive) {
        System.out.println(String.format("Succeed revoke %s %s privilege to %s:%s", operation, location, userName, userGroupName));
    }
}
