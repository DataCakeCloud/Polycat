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
package io.polycat.catalog.server.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import io.polycat.catalog.authorization.policyDecisionPoint.AccessType;
import io.polycat.catalog.authorization.policyDecisionPoint.AuthManager;
import io.polycat.catalog.authorization.policyDecisionPoint.AuthRequest;
import io.polycat.catalog.authorization.policyDecisionPoint.AuthResult;
import io.polycat.catalog.authorization.policyDecisionPoint.AuthShare;
import io.polycat.catalog.authorization.policyDecisionPoint.AuthTable;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.Principal;
import io.polycat.catalog.common.model.PrincipalSource;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.model.Share;
import io.polycat.catalog.common.model.ShareConsumer;
import io.polycat.catalog.service.api.AuthorizationService;
import io.polycat.catalog.service.api.PolicyService;
import io.polycat.catalog.service.api.RoleService;
import io.polycat.catalog.service.api.ShareService;
import io.polycat.catalog.service.api.UserGroupService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AuthorizationServiceImpl implements AuthorizationService {

    private ExecutorService executorService;
    private static final int MAX_POOL_SIZE = 10;
    private static final String AUTH_POOL_NAME = "auth-pool-%d";
    private static final String objectRegex = "\\.";

    @Autowired
    private PolicyService policyService;
    @Autowired
    private RoleService roleService;
    @Autowired
    private UserGroupService userGroupService;
    @Autowired
    private ShareService shareService;

    public AuthorizationServiceImpl() {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(AUTH_POOL_NAME).build();
        executorService = Executors.newFixedThreadPool(MAX_POOL_SIZE, namedThreadFactory);
    }

    /**
     * checkPrivilege
     *
     * @param authRequest authRequest
     */
    @Override
    public AuthResult checkPrivilege(AuthRequest authRequest) {
        List<MetaPrivilegePolicy> privilegePolicies = getPrivilegePolicy(authRequest);
        if(authRequest.getAccessType() == AccessType.TABLE_ACCESS.getNum()) {
            AuthTable authTable = new AuthTable();
            authTable.init(privilegePolicies);
            return authTable.checkPrivilege(authRequest);
        }

        if(authRequest.getAccessType() == AccessType.SHARE_ACCESS.getNum()) {
            AuthShare authShare = new AuthShare();
            authShare.init(privilegePolicies);
            return authShare.checkPrivilege(authRequest);
        }

        TAG: if(authRequest.getAccessType() == AccessType.SHARE_ACCESS_SELECT.getNum()) {
            String shareId =  "";
            String projectId = "";
            if (authRequest.getObjectType() == ObjectType.SHARE.getNum()) {
                String[] objectIds = authRequest.getObjectId().split(objectRegex);
                if(objectIds.length == 4) {
                    projectId = objectIds[0];
                    shareId = objectIds[1];
                }
            }

            if(StringUtils.isEmpty(shareId) || StringUtils.isEmpty(projectId)) {
                break TAG;
            }

            List<MetaPrivilegePolicy> sharePrivilegePolicies = getSharePrivilegePolicy(
                projectId, shareId);
            AuthShare authShare = new AuthShare();
            authShare.init(sharePrivilegePolicies);
            authShare.getShareConsumers(getShareConsumer(projectId, shareId));
            return authShare.checkPrivilege(authRequest);
        }

        if (authRequest.getAccessType() == AccessType.PRIVILEGE_ACCESS.getNum()) {
            AuthManager authManger = new AuthManager();
            authManger.init(privilegePolicies);
            return authManger.checkPrivilege(authRequest);
        }

        AuthResult.Builder result = new AuthResult.Builder();
        result.setIsAllowed(false);
        result.setReason("The access type is not recognized!");
        return result.build();
    }

    private List<MetaPrivilegePolicy> getPrivilegePolicy(AuthRequest authRequest) {
        List<Principal> principals = new ArrayList<>();
        Principal user = new Principal();
        user.setPrincipalId(authRequest.getUser());
        user.setPrincipalType(PrincipalType.USER.toString());
        user.setPrincipalSource(authRequest.getUserSource());
        principals.add(user);

        for (String principal : authRequest.getUserGroups()){
            Principal userGroup = new Principal();
            userGroup.setPrincipalType(PrincipalType.GROUP.toString());
            userGroup.setPrincipalSource(authRequest.getUserSource());
            userGroup.setPrincipalId(principal);
            principals.add(userGroup);
        }

        for (String principal : authRequest.getUserRoles()){
            Principal userRole = new Principal();
            userRole.setPrincipalType(PrincipalType.ROLE.toString());
            userRole.setPrincipalSource(authRequest.getUserSource());
            userRole.setPrincipalId(principal);
            principals.add(userRole);
        }
        return  policyService.listMetaPolicyByPrincipal(authRequest.getProjectId(), principals);
    }

    private List<ShareConsumer> getShareConsumer(String projectId, String shareId) {
        List<ShareConsumer> shareConsumers = new ArrayList<>();
        return shareConsumers;
    }

    private  String getShareCatalog(String projectId, String shareId) {
        Share share = shareService.getShareById(projectId, shareId);
        if(ObjectUtils.isEmpty(share)){
            return null;
        }
        return share.getCatalogName();
    }

    private List<MetaPrivilegePolicy> getSharePrivilegePolicy(String projectId, String shareId) {
        List<MetaPrivilegePolicy> sharePrivilegePolicy =  new ArrayList<>();
        List<Principal> sharePrincipals = new ArrayList<>();
        Principal sharePrincipal = new Principal();
        sharePrincipal.setPrincipalSource(PrincipalSource.LOCAL.toString());
        sharePrincipal.setPrincipalType(PrincipalType.SHARE.toString());
        sharePrincipal.setPrincipalId(shareId);
        sharePrincipals.add(sharePrincipal);

        return  policyService.listMetaPolicyByPrincipal(projectId, sharePrincipals);
    }


    /**
     * getUserGroupByUser
     *
     * @param userId userId
     * @Return userGroups userGroups
     */
    public List<String> getUserGroupsByUser(String projectId, String principalSource, String userId) {
        List<String> userGroups = new ArrayList<>();
        return userGroups;
    }

    /**
     * getUserRolesByUser
     * @param projectId projectId
     * @param principalSource principalSource
     * @param userId userId
     * @Return userRoles userRoles
     */
    public List<String> getUserRolesByUser(String projectId, String principalSource, String userId) {
        List<String> userRoles = new ArrayList<>();
        return userRoles;
    }
}