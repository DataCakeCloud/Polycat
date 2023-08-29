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
package io.polycat.catalog.authorization.policyDecisionPoint;

import java.util.List;

import lombok.Getter;

@Getter
public class AuthRequest {
    private int accessType;
    private String projectId;
    private String userSource;
    private String accountId;
    private String user;
    private List<String> userGroups;
    private List<String> userRoles;
    private String objectId;
    private String objectName;
    private int objectType;
    private int operation;
    private long privilege;

    protected AuthRequest() {
    }

    public static class Builder {
        private int accessType;
        private String projectId;
        private String userSource;
        private String accountId;
        private String user;
        private List<String> userGroups;
        private List<String> userRoles;
        private String objectId;
        private String objectName;
        private int objectType;
        private int operation;
        private long privilege;

        public AuthRequest.Builder setAccessType(int  accessType) {
            this.accessType = accessType;
            return this;
        }

        public AuthRequest.Builder setProjectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public AuthRequest.Builder setAccountId(String accountId) {
            this.accountId = accountId;
            return this;
        }

        public AuthRequest.Builder setUserSource(String userSource) {
            this.userSource = userSource;
            return this;
        }

        public AuthRequest.Builder setUser(String user) {
            this.user = user;
            return this;
        }

        public AuthRequest.Builder setUserGroup(List<String> userGroups) {
            this.userGroups = userGroups;
            return this;
        }

        public AuthRequest.Builder setUserRoles(List<String> userRoles) {
            this.userRoles = userRoles;
            return this;
        }

        public AuthRequest.Builder setObjectId(String objectId) {
            this.objectId = objectId;
            return this;
        }

        public AuthRequest.Builder setObjectName(String objectName) {
            this.objectName = objectName;
            return this;
        }

        public AuthRequest.Builder setObjectType(int objectType) {
            this.objectType = objectType;
            return this;
        }

        public AuthRequest.Builder setOperation(int operation) {
            this.operation = operation;
            return this;
        }

        public AuthRequest.Builder setPrivilege(int privilege) {
            this.privilege = privilege;
            return this;
        }


        public AuthRequest build() {
            AuthRequest authRequest = new AuthRequest();
            authRequest.accessType = accessType;
            authRequest.projectId = projectId;
            authRequest.accountId = accountId;
            authRequest.userSource = userSource;
            authRequest.user = user;
            authRequest.userGroups = userGroups;
            authRequest.userRoles = userRoles;
            authRequest.objectId = objectId;
            authRequest.objectName = objectName;
            authRequest.objectType = objectType;
            authRequest.operation = operation;
            authRequest.privilege = privilege;
            return authRequest;
        }
    }
}
