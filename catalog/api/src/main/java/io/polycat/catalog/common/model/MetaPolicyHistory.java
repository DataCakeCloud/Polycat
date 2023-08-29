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
public class MetaPolicyHistory {

    private String policyId;
    private String projectId;
    private PrincipalType principalType;
    private PrincipalSource principalSource;
    private String principalId;
    private PolicyModifyType modifyType;
    private long updateTime;

    public MetaPolicyHistory(String policyId, String projectId,
        int principalType, int principalSource, String principalId,
        long updateTime, int modifyType) {
        this.policyId = policyId;
        this.projectId = projectId;
        this.principalType = PrincipalType.getPrincipalType(principalType);
        this.principalSource = PrincipalSource.getPrincipalSource(principalSource);
        this.principalId = principalId;
        this.modifyType = PolicyModifyType.getModifyType(modifyType);
        this.updateTime = updateTime;
    }
}
