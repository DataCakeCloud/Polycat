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
package io.polycat.hiveService.impl;

import java.util.List;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.MetaPolicyHistory;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.Policy;
import io.polycat.catalog.common.model.Principal;
import io.polycat.catalog.common.plugin.request.input.PolicyInput;
import io.polycat.catalog.service.api.PolicyService;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HivePolicyServiceImpl implements PolicyService {

    @Override
    public void addAllPrivilegeOnObjectToPrincipal(String projectId, String principalName, PolicyInput policyInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "addAllPrivilegeOnObjectToPrincipal");
    }

    @Override
    public void addMetaPolicyToPrincipal(String projectId, String principalName, PolicyInput policyInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "addMetaPolicyToPrincipal");
    }

    @Override
    public void removeAllMetaPrivilegeFromPrincipal(String projectId, String principalName, PolicyInput policyInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "removeAllMetaPrivilegeFromPrincipal");
    }

    @Override
    public void removeAllPrivilegeOnObjectFromPrincipal(String projectId, String principalName,
        PolicyInput policyInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "removeAllPrivilegeOnObjectFromPrincipal");
    }

    @Override
    public void revokeMetaPolicyFromPrincipal(String projectId, String principalName, PolicyInput policyInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "revokeMetaPolicyFromPrincipal");
    }

    @Override
    public List<Policy> showMetaPolicyFromPrincipal(String projectId, String principalType, String principalSource,
        String principalName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getMetaPolicyFromPrincipal");
    }

    @Override
    public List<MetaPrivilegePolicy> listMetaPolicyByPrincipal(String projectId, List<Principal> principalList) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "listMetaPolicyByPrincipal");
    }

    @Override
    public List<MetaPolicyHistory> getUpdatedMetaPolicyIdsByTime(String projectId, long time) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getUpdatedMetaPolicyIdsByTime");
    }

    @Override
    public List<MetaPrivilegePolicy> getUpdatedMetaPolicyByIdList(String projectId, String principalType,
        List<String> policyIdList){
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getUpdatedMetaPolicyByIdList");
    }
}
