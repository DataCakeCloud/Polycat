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
package io.polycat.catalog.store.gaussdb;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.store.api.PolicyStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.List;


@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class PolicyStoreImpl implements PolicyStore {
    @Override
    public String insertMetaPrivilegePolicy(TransactionContext context, String projectId, PrincipalType principalType, int principalSource, String principalId, int objectType, String objectId, boolean effect, long privilege, boolean isOwner, String condition, String obligation, long updateTime, boolean grantAble) throws MetaStoreException {
        return null;
    }

    @Override
    public String insertDataPrivilegePolicy(TransactionContext context, String projectId, PrincipalType principalType, int principalSource, String principalId, String obsPath, String obsEndpoint, int permission, long updateTime) throws MetaStoreException {
        return null;
    }

    @Override
    public String deleteMetaPrivilegePolicy(TransactionContext context, String projectId, PrincipalType principalType, int principalSource, String principalId, int objectType, String objectId, boolean effect, long privilege, boolean isOwner) throws MetaStoreException {
        return null;
    }

    @Override
    public String deleteDataPrivilegePolicy(TransactionContext context, String projectId, PrincipalType principalType, int principalSource, String principalId, String obsPath, String obsEndpoint) throws MetaStoreException {
        return null;
    }

    @Override
    public String updateMetaPrivilegePolicy(TransactionContext context, String projectId, PrincipalType principalType, MetaPrivilegePolicy metaPrivilegePolicy, String condition, String obligation, long updateTime, boolean grantAble) throws MetaStoreException {
        return null;
    }

    @Override
    public String updateDataPrivilegePolicy(TransactionContext context, String projectId, PrincipalType principalType, ObsPrivilegePolicy dataPrivilegePolicy, int permission, long updateTime) throws MetaStoreException {
        return null;
    }

    @Override
    public MetaPrivilegePolicy getMetaPrivilegePolicy(TransactionContext context, String projectId, PrincipalType principalType, int principalSource, String principalId, int objectType, String objectId, boolean effect, long privilege) throws MetaStoreException {
        return null;
    }

    @Override
    public ObsPrivilegePolicy getDataPrivilegePolicy(TransactionContext context, String projectId, PrincipalType principalType, int principalSource, String principalId, String obsPath, String obsEndpoint) throws MetaStoreException {
        return null;
    }

    @Override
    public List<MetaPrivilegePolicy> getMetaPrivilegesByPrincipal(TransactionContext context, String projectId, PrincipalType principalType, int principalSource, String principalId) throws MetaStoreException {
        return null;
    }

    @Override
    public List<ObsPrivilegePolicy> getDataPrivilegesByPrincipal(TransactionContext context, String projectId, PrincipalType principalType, int principalSource, String principalId) throws MetaStoreException {
        return null;
    }

    @Override
    public List<MetaPrivilegePolicy> getMetaPrivilegesByPrincipalOnObject(TransactionContext context, String projectId, PrincipalType principalType, int principalSource, String principalId, int objectType, String objectId) throws MetaStoreException {
        return null;
    }

    @Override
    public void delAllMetaPrivilegesOfPrincipal(TransactionContext context, String projectId, PrincipalType principalType, int principalSource, String principalId) throws MetaStoreException {

    }

    @Override
    public void delAllMetaPrivilegesOfPrincipalOnObject(TransactionContext context, String projectId, PrincipalType principalType, int principalSource, String principalId, int objectType, String objectId) throws MetaStoreException {

    }

    @Override
    public void delAllMetaPrivilegeOnObject(TransactionContext context, String projectId, int objectType, String objectId, boolean isShare) throws MetaStoreException {

    }

    @Override
    public void insertMetaPrivilegePolicyHistory(TransactionContext context, String projectId, String policyId, PrincipalType principalType, int principalSource, String principalId, int modifyType, long updateTime) throws MetaStoreException {

    }

    @Override
    public void updateMetaPrivilegePolicyHistory(TransactionContext context, String projectId, String policyId, PrincipalType principalType, int principalSource, String principalId, int modifyType, long updateTime) throws MetaStoreException {

    }

    @Override
    public byte[] getShareMetaPolicyHistoryWithToken(TransactionContext context, String projectId, long time, List<MetaPolicyHistory> policyHistoryList, byte[] continuation) {
        return new byte[0];
    }

    @Override
    public byte[] getMetaPolicyHistoryWithToken(TransactionContext context, String projectId, long time, List<MetaPolicyHistory> policyHistoryList, byte[] continuation) {
        return new byte[0];
    }

    @Override
    public byte[] getMetaPrivilegesByPrincipalWithToken(TransactionContext context, String projectId, PrincipalType principalType, int num, String principalId, List<MetaPrivilegePolicy> privilegesList, byte[] continuation) {
        return new byte[0];
    }

    @Override
    public byte[] delAllMetaPrivilegesOfPrincipalOnObjectWithToken(TransactionContext context, String projectId, PrincipalType type, int principalSource, String principalId, int objectType, String objectId, byte[] continuation) {
        return new byte[0];
    }

    @Override
    public byte[] delAllMetaPrivilegesOfPrincipalWithToken(TransactionContext context, String projectId, PrincipalType type, int principalSource, String principalId, byte[] continuation) {
        return new byte[0];
    }

    @Override
    public void getMetaPrivilegeByPolicyId(TransactionContext context, String projectId, PrincipalType principalType, String policyId, List<MetaPrivilegePolicy> policyObjectList) {

    }
}
