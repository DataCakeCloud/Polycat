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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.EffectType;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.ObjectPrivilege;
import io.polycat.catalog.common.model.OperationPrivilege;
import io.polycat.catalog.common.model.OperationPrivilegeType;
import io.polycat.catalog.common.model.PolicyModifyType;
import io.polycat.catalog.common.model.PrincipalSource;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.model.Role;
import io.polycat.catalog.common.model.RoleObject;
import io.polycat.catalog.common.model.RolePrincipalObject;
import io.polycat.catalog.common.model.ShareConsumerObject;
import io.polycat.catalog.common.model.ShareObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.ViewIdent;
import io.polycat.catalog.common.model.ViewRecordObject;
import io.polycat.catalog.common.plugin.request.input.RoleInput;
import io.polycat.catalog.common.utils.UuidUtil;
import io.polycat.catalog.service.api.NewRoleService;
import io.polycat.catalog.store.api.NewRoleStore;
import io.polycat.catalog.store.api.PolicyStore;
import io.polycat.catalog.store.api.Transaction;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.impl.CatalogStoreImpl;
import io.polycat.catalog.store.fdb.record.impl.NewRoleStoreImpl;
import io.polycat.catalog.store.fdb.record.impl.PolicyStoreImpl;
import io.polycat.catalog.store.fdb.record.impl.RoleStoreImpl;
import io.polycat.catalog.store.fdb.record.impl.ShareStoreImpl;
import io.polycat.catalog.store.fdb.record.impl.TransactionImpl;
import io.polycat.catalog.store.fdb.record.impl.ViewStoreImpl;
import io.polycat.catalog.util.CheckUtil;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class NewRoleServiceImpl implements NewRoleService {

    private static final Logger log = Logger.getLogger(RoleServiceImpl.class);

    @Autowired
    private Transaction storeTransaction;

    @Autowired(required = false)
    private PolicyStore policyStore;

    private static final NewRoleStoreImpl roleStore = NewRoleStoreImpl.getInstance();

    @Override
    public void createRole(String projectId, RoleInput roleInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            if (roleStore.roleNameExist(context, projectId, roleInput.getRoleName())) {
                throw new MetaStoreException(ErrorCode.ROLE_ALREADY_EXIST, roleInput.getRoleName());
            }
            String roleId = UuidUtil.generateId();
            roleStore.insertRoleProperties(context, projectId, roleId, roleInput.getRoleName(),
                roleInput.getOwnerUser());

            long updateTime = RecordStoreHelper.getCurrentTime();
            String policyId = policyStore.insertMetaPrivilegePolicy(context, projectId,
                PrincipalType.USER, PrincipalSource.IAM.getNum(), roleInput.getOwnerUser(),
                ObjectType.ROLE.getNum(), roleId, true, 0, true,
                null, null, updateTime, true);
            if (policyId == null) {
                throw new CatalogServerException(ErrorCode.POLICY_ID_NOT_FOUND);
            }
            policyStore.insertMetaPrivilegePolicyHistory(context, projectId, policyId,
                PrincipalType.USER, PrincipalSource.IAM.getNum(), roleInput.getOwnerUser(),
                PolicyModifyType.ADD.getNum(), updateTime);

            context.commit();
        }
    }

    private void dropRoleObject(TransactionContext context, String projectId, RoleObject roleObject) {
        String roleId = roleObject.getRoleId();
        roleStore.deleteRoleProperties(context, projectId, roleId);
        roleStore.delAllRolePrincipal(context, projectId, roleId);

        policyStore.delAllMetaPrivilegesOfPrincipal(context,projectId,
            PrincipalType.ROLE,PrincipalSource.IAM.getNum(), roleId);

        policyStore.delAllMetaPrivilegesOfPrincipalOnObject(context, projectId,
            PrincipalType.USER, PrincipalSource.IAM.getNum(), roleObject.getOwnerId(),
            ObjectType.ROLE.getNum(), roleId);

    }


    @Override
    public void dropRoleById(String projectId, String roleId) {
        CheckUtil.checkStringParameter(projectId, roleId);
        try (TransactionContext context = storeTransaction.openTransaction()) {
            RoleObject roleObject = roleStore.getRolePropertiesByRoleId(context, projectId, roleId);
            dropRoleObject(context, projectId, roleObject);
            context.commit();
        }
    }

    @Override
    public void dropRoleByName(String projectId, String roleName) {
        CheckUtil.checkStringParameter(projectId, roleName);
        try (TransactionContext context = storeTransaction.openTransaction()) {
            RoleObject roleObject = roleStore.getRolePropertiesByRoleName(context, projectId, roleName);
            dropRoleObject(context, projectId, roleObject);
            context.commit();
        }
    }

    public Role getRoleById(String projectId, String roleId) {
        CheckUtil.checkStringParameter(projectId, roleId);
        try (TransactionContext context = storeTransaction.openTransaction()) {
            RoleObject roleObject = roleStore.getRolePropertiesByRoleId(context, projectId, roleId);
            Role role = getRoleFromObject(context, projectId, roleObject);
            context.commit();
            return role;
        }
    }

    @Override
    public Role getRoleByName(String projectId, String roleName) {
        CheckUtil.checkStringParameter(projectId, roleName);
        try (TransactionContext context = storeTransaction.openTransaction()) {
            RoleObject roleObject = roleStore.getRolePropertiesByRoleName(context, projectId, roleName);
            Role role = getRoleFromObject(context, projectId, roleObject);
            context.commit();
            return role;
        }
    }


    @Override
    public void alterRoleName(String projectId, String roleName, RoleInput roleInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {

            RoleObject roleObject = roleStore.getRolePropertiesByRoleName(context, projectId, roleName);
            roleObject.setRoleName(roleInput.getRoleName());
            roleStore.updateRoleProperties(context, roleObject);
            context.commit();
        }
    }

    private Role getRoleFromObject(TransactionContext context, String projectId, RoleObject roleObject) {

        String roleId = roleObject.getRoleId();
        List<RolePrincipalObject> rolePrincipals = roleStore.getRolePrincipalsByRoleId(context, projectId, roleId);
        List<String> rolePrincipalInfos = new ArrayList<>();
        for (RolePrincipalObject rolePrincipal : rolePrincipals) {
            StringBuffer sb = new StringBuffer();
            sb.append(rolePrincipal.getPrincipalType()).append(":")
                .append(rolePrincipal.getPrincipalSource()).append(":")
                .append(rolePrincipal.getPrincipalId());
            rolePrincipalInfos.add(sb.toString());
        }

        List<MetaPrivilegePolicy> rolePrivilegesList = policyStore.getMetaPrivilegesByPrincipal(context,projectId,
            PrincipalType.ROLE, PrincipalSource.IAM.getNum(), roleId);

        List<ObjectPrivilege> objectPrivilegeList = PrivilegePolicyHelper.convertObjectPrivilegeList(context, projectId, rolePrivilegesList);

        return toRole(roleObject, rolePrincipalInfos, objectPrivilegeList);
    }

    public static Role toRole(RoleObject roleObject, List<String> roleUserIds,
        List<ObjectPrivilege> objectPrivilegeList) {
        Role role = new Role();

        List<io.polycat.catalog.common.model.RolePrivilege> rolePrivilegeList = new ArrayList<>();
        role.setProjectId(roleObject.getProjectId());
        role.setRoleId(roleObject.getRoleId());
        role.setRoleName(roleObject.getRoleName());
        role.setToUsers(roleUserIds.toArray(new String[0]));
        role.setOwner(roleObject.getOwnerId());
        Date date = new Date(roleObject.getCreateTime());
        String dateFormat = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat SDF = new SimpleDateFormat(dateFormat);
        role.setCreatedTime(SDF.format(date));
        for (int i = 0; i < objectPrivilegeList.size(); i++) {
            ObjectPrivilege objectPrivilege = objectPrivilegeList.get(i);
            io.polycat.catalog.common.model.RolePrivilege rolePrivilege = new io.polycat.catalog.common.model.RolePrivilege();
            rolePrivilege.setName(objectPrivilege.getObjectName());
            rolePrivilege.setPrivilege(objectPrivilege.getPrivilege());
            rolePrivilege.setGrantedOn(objectPrivilege.getObjectType());
            rolePrivilegeList.add(rolePrivilege);

        }
        role.setRolePrivileges(rolePrivilegeList.toArray(new io.polycat.catalog.common.model.RolePrivilege[0]));
        return role;
    }

    /**
     * add user to role
     *
     * @param roleName
     * @param roleInput
     */
    public void addUserToRole(String projectId, String roleName, RoleInput roleInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            RoleObject roleObject = roleStore.getRolePropertiesByRoleName(context, projectId, roleName);
            if (roleObject == null) {
                context.commit();
                return;
            }
            String[] users = roleInput.getUserId();
            for (int i = 0; i < users.length; i++) {
                String[] src = users[i].split(":");
                PrincipalType type = PrincipalType.valueOf(src[0]);
                PrincipalSource source = PrincipalSource.valueOf(src[1]);
                roleStore.insertRolePrincipal(context, projectId, roleObject.getRoleId(),
                    type, source, src[2]);
            }
            context.commit();
        }
    }

    /**
     * remove user from role
     *
     * @param roleName
     * @param roleInput
     */
    public void removeUserFromRole(String projectId, String roleName, RoleInput roleInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            RoleObject roleObject = roleStore.getRolePropertiesByRoleName(context, projectId, roleName);
            if (roleObject == null) {
                context.commit();
                return;
            }

            String[] users = roleInput.getUserId();
            for (int i = 0; i < users.length; i++) {
                String[] src = users[i].split(":");
                PrincipalType type = PrincipalType.valueOf(src[0]);
                PrincipalSource source = PrincipalSource.valueOf(src[1]);
                roleStore.deleteRolePrincipal(context, projectId, roleObject.getRoleId(),
                    type, source, src[2]);
            }
            context.commit();
        }
    }

    private List<Role> convertToRoleModel(List<RoleObject> roleObjects) {
        List<Role> roleModels = new ArrayList<>();
        for (RoleObject roleObject : roleObjects) {
            Role model = new Role();
            Date date = new Date(roleObject.getCreateTime());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            model.setCreatedTime(sdf.format(date));
            model.setRoleName(roleObject.getRoleName());
            model.setOwner(roleObject.getOwnerId());
            List<String> users = new ArrayList<>();
            if (roleObject.getToUsers() != null && !roleObject.getToUsers().isEmpty()) {
                users.addAll(roleObject.getToUsers());
            }
            model.setToUsers(users.toArray(new String[users.size()]));
            model.setProjectId(roleObject.getProjectId());
            model.setRoleId(roleObject.getRoleId());

            roleModels.add(model);
        }
        return roleModels;
    }

    /**
     * get role models in project
     *
     * @param projectId
     * @param namePattern
     */
    @Override
    public List<Role> getRoleModels(String projectId, String userId, String namePattern) {
        List<Role> roleModels;
        //List<RoleRecord> roles = versionStore.getRoles(projectId, userId, namePattern);
        List<RoleObject> roleObjects = new ArrayList<>();
        try (TransactionContext context = storeTransaction.openTransaction()) {
            roleStore.makeOutBoundRoles(context, roleObjects, projectId, userId, namePattern);
            roleStore.makeInBoundRoles(context, roleObjects, userId, projectId, namePattern);
            context.commit();
        }
        roleModels = convertToRoleModel(roleObjects);

        return roleModels;
    }
}
