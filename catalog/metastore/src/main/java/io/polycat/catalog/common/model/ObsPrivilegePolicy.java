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

import lombok.Data;

@Data
public class ObsPrivilegePolicy {
        private String policyId;
        private String projectId;
        private int principalType;
        private int principalSource;
        private String principalId;
        private String obsEndpoint;
        private String obsPath;
        private int permission;
        private long updateTime;


        public ObsPrivilegePolicy(String policyId, String projectId,
                int principalType,int principalSource, String principalId,
                String obsPath, String obsEndpoint, int permission, long updateTime) {
            this.policyId = policyId;
            this.projectId = projectId;
            this.principalType = principalType;
            this.principalSource = principalSource;
            this.principalId = principalId;
            this.obsPath = obsPath;
            this.obsEndpoint = obsEndpoint;
            this.permission = permission;
            this.updateTime = updateTime;
        }
}
