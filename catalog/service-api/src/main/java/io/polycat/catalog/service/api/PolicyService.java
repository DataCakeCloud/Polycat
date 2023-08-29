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
package io.polycat.catalog.service.api;

import java.util.List;

import io.polycat.catalog.common.model.MetaPolicyHistory;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.Policy;
import io.polycat.catalog.common.model.Principal;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.plugin.request.input.PolicyInput;

public interface PolicyService {

    void addAllPrivilegeOnObjectToPrincipal(String projectId,  String principalName, PolicyInput policyInput);

    void addMetaPolicyToPrincipal(String projectId,  String principalName, PolicyInput policyInput);

    void removeAllMetaPrivilegeFromPrincipal(String projectId, String principalName, PolicyInput policyInput);

    void removeAllPrivilegeOnObjectFromPrincipal(String projectId, String principalName,
        PolicyInput policyInput);

    void revokeMetaPolicyFromPrincipal(String projectId,  String principalName, PolicyInput policyInput);

    List<Policy> showMetaPolicyFromPrincipal(String projectId, String principalType,String principalSource,String principalName );

    List<MetaPrivilegePolicy>  listMetaPolicyByPrincipal(String projectId, List<Principal> principalList);

    List<MetaPolicyHistory> getUpdatedMetaPolicyIdsByTime(String projectId, long time);

    List<MetaPrivilegePolicy> getUpdatedMetaPolicyByIdList(String projectId, String principalType,
        List<String> policyIdList);
}
