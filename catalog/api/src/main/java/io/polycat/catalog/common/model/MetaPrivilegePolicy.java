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
package io.polycat.catalog.common.model;
import lombok.Getter;

@Getter
public class MetaPrivilegePolicy {
    private String policyId;
    private String projectId;
    private int principalType;
    private int principalSource;
    private String principalId;
    private int objectType;
    // if object type is catalog, objectId is catalogId
    // if object type is database, objectId is catalogId.databaseId
    // if object type is table, objectId is catalogId.databaseId.tableId
    private String objectId;
    private String objectName;
    private boolean effect;
    private long privilege;
    private String condition;
    private String obligation;
    private long updateTime;
    private boolean grantAble;
    private MetaPrivilegePolicy() {
    }

    public static Builder  builder() {
        return new Builder();
    }

    public void  setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public static class Builder {
        private String policyId;
        private String projectId;
        private int principalType;
        private int principalSource;
        private String principalId;
        private int objectType;
        // if object type is catalog, objectId is catalogId
        // if object type is database, objectId is catalogId.databaseId
        // if object type is table, objectId is catalogId.databaseId.tableId
        private String objectId;
        private String objectName;
        private boolean effect;
        private long privilege;
        private String condition;
        private String obligation;
        private long updateTime;
        private boolean grantAble;
        public MetaPrivilegePolicy.Builder setPolicyId(String policyId) {
            this.policyId = policyId;
            return this;
        }
        public MetaPrivilegePolicy.Builder setProjectId(String projectId) {
            this.projectId = projectId;
            return this;
        }
        public MetaPrivilegePolicy.Builder setPrincipalType(int principalType) {
            this.principalType = principalType;
            return this;
        }
        public MetaPrivilegePolicy.Builder setPrincipalSource(int principalSource) {
            this.principalSource = principalSource;
            return this;
        }
        public MetaPrivilegePolicy.Builder setPrincipalId(String principalId) {
            this.principalId = principalId;
            return this;
        }
        public MetaPrivilegePolicy.Builder setObjectType(int objectType) {
            this.objectType = objectType;
            return this;
        }
        public MetaPrivilegePolicy.Builder setObjectId(String objectId) {
            this.objectId = objectId;
            return this;
        }
        public MetaPrivilegePolicy.Builder setObjectName(String objectName) {
            this.objectName = objectName;
            return this;
        }
        public MetaPrivilegePolicy.Builder setEffect(boolean effect) {
            this.effect = effect;
            return this;
        }
        public MetaPrivilegePolicy.Builder setPrivilege(long privilege) {
            this.privilege = privilege;
            return this;
        }
        public MetaPrivilegePolicy.Builder setCondition(String condition) {
            this.condition = condition;
            return this;
        }
        public MetaPrivilegePolicy.Builder setObligation(String obligation) {
            this.obligation = obligation;
            return this;
        }
        public MetaPrivilegePolicy.Builder setUpdateTime(long updateTime) {
            this.updateTime = updateTime;
            return this;
        }
        public MetaPrivilegePolicy.Builder setGrantAble(boolean grantAble) {
            this.grantAble = grantAble;
            return this;
        }
        public MetaPrivilegePolicy build() {
            MetaPrivilegePolicy policy = new MetaPrivilegePolicy();
            policy.policyId = policyId;
            policy.projectId = projectId;
            policy.principalType = principalType;
            policy.principalSource = principalSource;
            policy.principalId = principalId;
            policy.objectType = objectType;
            policy.objectId = objectId;
            policy.objectName = objectName;
            policy.effect = effect;
            policy.privilege = privilege;
            policy.condition = condition;
            policy.obligation = obligation;
            policy.updateTime = updateTime;
            policy.grantAble = grantAble;
            return policy;
        }
    }
}








