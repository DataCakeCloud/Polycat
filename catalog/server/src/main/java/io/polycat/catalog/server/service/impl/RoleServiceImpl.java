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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.polycat.catalog.common.*;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.plugin.request.input.RoleInput;
import io.polycat.catalog.common.utils.GsonUtil;
import io.polycat.catalog.server.util.TransactionRunnerUtil;
import io.polycat.catalog.service.api.CatalogService;
import io.polycat.catalog.service.api.RoleService;
import io.polycat.catalog.store.api.*;
import io.polycat.catalog.util.CheckUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class RoleServiceImpl implements RoleService {

    private final static String dateFormat = "yyyy-MM-dd HH:mm:ss";

    @Autowired
    private RoleStore roleStore;
    @Autowired
    private UserPrivilegeStore userPrivilegeStore;

    @Autowired
    private CatalogService catalogService;


    /**
     * create role
     *
     * @param projectId
     * @param roleInput
     */
    @Override
    public void createRole(String projectId, RoleInput roleInput) {
        CheckUtil.checkNameLegality("roleName", roleInput.getRoleName());
        TransactionRunnerUtil.transactionRunThrow(context -> {
            if (roleStore.roleObjectNameExist(context, projectId, roleInput.getRoleName())) {
                throw new MetaStoreException(ErrorCode.ROLE_ALREADY_EXIST, roleInput.getRoleName());
            }
            String roleId = roleStore.generateRoleObjectId(context, projectId);
            roleStore.insertRoleObjectName(context, projectId, roleInput.getRoleName(), roleId);
            roleStore.insertRoleProperties(context, projectId, roleId, roleInput.getRoleName(),
                roleInput.getOwnerUser(), roleInput.getComment());
            userPrivilegeStore.insertUserPrivilege(context, projectId, roleInput.getOwnerUser(),
                ObjectType.ROLE.name(), roleId, true, 0);
            return null;
        });
    }

    private void dropRoleById(TransactionContext context, String projectId, String roleId) {
        RoleObject roleObject = roleStore.getRoleProperties(context, projectId, roleId);
        CheckUtil.assertNotNull(roleObject, ErrorCode.ROLE_ID_NOT_FOUND, roleId);
        roleStore.deleteRoleProperties(context, projectId, roleId);
        roleStore.deleteRoleObjectName(context, projectId, roleObject.getRoleName());
        roleStore.delAllRolePrivilege(context, projectId, roleId);
        roleStore.delAllRoleUser(context, projectId, roleId);

        userPrivilegeStore.deleteUserPrivilege(context, projectId, roleObject.getOwnerId(),
            ObjectType.ROLE.name(), roleId);
    }

    /**
     * drop role by id
     *
     * @param projectId
     * @param roleId
     */
    @Override
    public void dropRoleById(String projectId, String roleId) {
        CheckUtil.checkStringParameter(projectId, roleId);
        TransactionRunnerUtil.transactionRunThrow(context -> {
            dropRoleById(context, projectId, roleId);
            return null;
        });
    }

    /**
     * drop share by name
     *  @param projectId
     * @param roleName
     */
    @Override
    public void dropRoleByName(String projectId, String roleName) {
        CheckUtil.checkStringParameter(projectId, roleName);
        TransactionRunnerUtil.transactionRunThrow(context -> {
            String roleId = roleStore.getRoleId(context, projectId, roleName);
            CheckUtil.assertNotNull(roleId, ErrorCode.ROLE_NOT_FOUND, roleName);
            dropRoleById(context, projectId, roleId);
            return null;
        });
    }

    public static Role toRole(RoleObject roleObject, List<String> roleUserIds, List<ObjectPrivilege> objectPrivilegeList ) {
        Role role = new Role();
        List<io.polycat.catalog.common.model.RolePrivilege> rolePrivilegeList = new ArrayList<>();
        role.setProjectId(roleObject.getProjectId());
        role.setRoleId(roleObject.getRoleId());
        role.setRoleName(roleObject.getRoleName());
        role.setToUsers(roleUserIds.toArray(new String[0]));
        role.setOwner(roleObject.getOwnerId());
        role.setComment(roleObject.getComment());
        if (roleObject.getCreateTime() > 0) {
            Date date = new Date(roleObject.getCreateTime());
            SimpleDateFormat SDF = new SimpleDateFormat(dateFormat);
            role.setCreatedTime(SDF.format(date));
        }
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

    private Role getRoleById(TransactionContext context, String projectId, String roleId) {
        RoleObject roleObject = roleStore.getRoleProperties(context, projectId, roleId);
        CheckUtil.assertNotNull(roleObject, ErrorCode.ROLE_ID_NOT_FOUND, roleId);
        List<RoleUserObject> roleUsers = roleStore.getRoleUsersByRoleId(context, projectId, roleId);
        List<String> roleUserIds = new ArrayList<>();
        for (RoleUserObject roleUser : roleUsers) {
            roleUserIds.add(roleUser.getUserId());
        }

        List<RolePrivilegeObject> rolePrivilegeObjectList = roleStore.getRolePrivilege(context, projectId, roleId);
        List<ObjectPrivilege> objectPrivilegeList = RolePrivilegeHelper.convertRolePrivilege(context, projectId, rolePrivilegeObjectList);
        return toRole(roleObject, roleUserIds, objectPrivilegeList);
    }

    /**
     * get role by roleName
     *
     * @param roleName
     * @return role
     */
    @Override
    public Role getRoleByName(String projectId, String roleName) {
        CheckUtil.checkStringParameter(projectId, roleName);
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            String roleObjectId = roleStore.getRoleId(context, projectId, roleName);
            CheckUtil.assertNotNull(roleObjectId, ErrorCode.ROLE_NOT_FOUND, roleName);
            return getRoleById(context, projectId, roleObjectId);
        }).getResult();
    }

    /**
     *
     * @param projectId
     * @param roleId
     * @return
     */
    @Override
    public Role getRoleById(String projectId, String roleId) {
        CheckUtil.checkStringParameter(projectId, roleId);

        return TransactionRunnerUtil.transactionRunThrow(context -> {
            return getRoleById(context, projectId, roleId);
        }).getResult();
    }

    /**
     * alter role
     *
     * @param roleName
     * @param roleInput
     */
    @Override
    public void alterRole(String projectId, String roleName, RoleInput roleInput) {
        CheckUtil.checkNameLegality("roleName", roleInput.getRoleName());
        TransactionRunnerUtil.transactionRunThrow(context -> {
            String roleObjectId = roleStore.getRoleId(context, projectId, roleName);
            CheckUtil.assertNotNull(roleObjectId, ErrorCode.ROLE_NOT_FOUND, roleName);
            String newRoleName = roleInput.getRoleName();
            String newRoleObjectId = roleStore.getRoleId(context, projectId, newRoleName);
            if (newRoleObjectId != null) {
                throw new MetaStoreException(ErrorCode.ROLE_ALREADY_EXIST, newRoleName);
            }
            RoleObject roleObject = roleStore.getRoleProperties(context, projectId, roleObjectId);

            roleObject.setRoleName(roleInput.getRoleName());
            if (roleInput.getComment() != null) {
                roleObject.setComment(roleInput.getComment());
            }
            if (roleInput.getOwnerUser() != null) {
                roleObject.setOwnerId(roleInput.getOwnerUser());
            }
            roleStore.updateRoleProperties(context, roleObject);
            if (!roleName.equals(roleInput.getRoleName())) {
                roleStore.deleteRoleObjectName(context, projectId, roleName);
                roleStore.insertRoleObjectName(context, roleObject.getProjectId(), roleObject.getRoleName(), roleObject.getRoleId());
            }
            return true;
        });
    }

    private void addPrivilegeToRole(String projectId, String roleName, String objectType, CatalogInnerObject catalogInnerObject,
        long privilege) {
        TransactionRunnerUtil.transactionRunThrow(context -> {
            String roleId = roleStore.getRoleId(context, projectId, roleName);
            CheckUtil.assertNotNull(roleId, ErrorCode.ROLE_NOT_FOUND, roleName);
            String rolePrivilegeObjectId = RolePrivilegeHelper.getRolePrivilegeObjectId(objectType, catalogInnerObject);
            RolePrivilegeObject rolePrivilegeObject = roleStore.getRolePrivilege(context, projectId,
                roleId, objectType, rolePrivilegeObjectId);
            if (rolePrivilegeObject == null) {
                roleStore.insertRolePrivilege(context, projectId, roleId, objectType, rolePrivilegeObjectId, catalogInnerObject, privilege);
            } else {
                long newPrivilege = privilege | rolePrivilegeObject.getPrivilege();
                roleStore.updateRolePrivilege(context, projectId, rolePrivilegeObject, newPrivilege);
            }
            return true;
        });
    }

    private void removePrivilegeFromRole(String projectId, String roleName, String objectType, CatalogInnerObject catalogInnerObject,
        long privilege) {
        TransactionRunnerUtil.transactionRunThrow(context -> {
            String roleId = roleStore.getRoleId(context, projectId, roleName);
            CheckUtil.assertNotNull(roleId, ErrorCode.ROLE_NOT_FOUND, roleName);
            String rolePrivilegeObjectId = RolePrivilegeHelper.getRolePrivilegeObjectId(objectType, catalogInnerObject);
            RolePrivilegeObject rolePrivilegeObject = roleStore.getRolePrivilege(context, projectId,
                roleId, objectType, rolePrivilegeObjectId);
            if (rolePrivilegeObject == null) {
                throw new MetaStoreException(ErrorCode.ROLE_PRIVILEGE_INVALID);
            }

            long newPrivilege = (~privilege) & rolePrivilegeObject.getPrivilege();
            if (newPrivilege == 0) {
                roleStore.deleteRolePrivilege(context, projectId, roleId, objectType, RolePrivilegeHelper.getRolePrivilegeObjectId(objectType, catalogInnerObject));
            } else {
                roleStore.updateRolePrivilege(context, projectId, rolePrivilegeObject, newPrivilege);
            }
            return true;
        });
    }

    private void modifyPrivilege(String projectId, String roleName, RoleInput roleInput, boolean isAdd) {
        OperationPrivilege operationPrivilege = RolePrivilegeHelper.getOperationPrivilege(roleInput.getOperation());
        CheckUtil.assertNotNull(operationPrivilege, ErrorCode.ROLE_PRIVILEGE_INVALID);
        String objectName = roleInput.getObjectName();
        if (operationPrivilege.getObjectType() != ObjectType.valueOf(roleInput.getObjectType())) {
            throw new CatalogServerException(ErrorCode.ROLE_PRIVILEGE_OPERATION_INCONSISTENT, objectName, roleInput.getObjectType(), roleInput.getOperation());
        }
        assertTypeWithNameConsistent(roleInput, roleInput.getObjectType());
        CatalogInnerObject catalogInnerObject = RolePrivilegeHelper.getCatalogObject(projectId,
                operationPrivilege.getObjectType().name(), objectName);

        long privilege = RolePrivilegeHelper.convertOperationPrivilege(operationPrivilege);
        if (privilege == 0) {
            throw new CatalogServerException(ErrorCode.ROLE_PRIVILEGE_INVALID);
        }

        if (isAdd) {
            checkObjectName(objectName);
            addPrivilegeToRole(projectId, roleName, operationPrivilege.getObjectType().name(), catalogInnerObject, privilege);
        } else {
            removePrivilegeFromRole(projectId, roleName, operationPrivilege.getObjectType().name(), catalogInnerObject, privilege);
        }
    }

    private void checkObjectName(String objectName) {
        String[] names = objectName.split(RolePrivilegeHelper.OBJECT_SEPARATE_SYMBOL);
        for (int i = 0; i < names.length; i++) {
            if (!names[i].endsWith(RolePrivilegeHelper.WILDCARD_SYMBOL)) {
                CheckUtil.checkNameLegality("objectName", names[i]);
            }
        }
    }

    /**
     * add privilege to role
     *
     * @param roleName
     * @param roleInput
     */
    @Override
    public void addPrivilegeToRole(String projectId, String roleName, RoleInput roleInput) {
        modifyPrivilege(projectId, roleName, roleInput, true);
    }

    /**
     * add all privilege to role
     *
     * @param roleName
     * @param roleInput
     */
    @Override
    public void addAllPrivilegeOnObjectToRole(String projectId, String roleName, RoleInput roleInput) {
        String objectType = ObjectType.valueOf(roleInput.getObjectType().toUpperCase()).name();
        CatalogInnerObject catalogInnerObject = RolePrivilegeHelper.getCatalogObject(projectId,
            objectType, roleInput.getObjectName());
        assertTypeWithNameConsistent(roleInput, objectType);
        long privilege = RolePrivilegeHelper.getObjectAllPrivilegesByType(objectType);
        if (privilege == 0) {
            throw new CatalogServerException(ErrorCode.ROLE_PRIVILEGE_INVALID);
        }

        TransactionRunnerUtil.transactionRunThrow(context -> {
            String roleId = roleStore.getRoleId(context, projectId, roleName);
            CheckUtil.assertNotNull(roleId, ErrorCode.ROLE_NOT_FOUND, roleName);
            String rolePrivilegeObjectId = RolePrivilegeHelper.getRolePrivilegeObjectId(objectType, catalogInnerObject);
            RolePrivilegeObject rolePrivilegeObject = roleStore.getRolePrivilege(context, projectId,
                roleId, objectType, rolePrivilegeObjectId);
            if (rolePrivilegeObject == null) {
                roleStore.insertRolePrivilege(context, projectId, roleId, objectType , rolePrivilegeObjectId, catalogInnerObject, privilege);
            } else {
                long newPrivilege = privilege | rolePrivilegeObject.getPrivilege();
                roleStore.updateRolePrivilege(context, projectId, rolePrivilegeObject, newPrivilege);
            }
            return true;
        });
    }

    private void assertTypeWithNameConsistent(RoleInput roleInput, String objectType) {
        String objectName = roleInput.getObjectName();
        CheckUtil.checkStringParameter(objectName, "objectName");
        String[] strArray = objectName.split("\\.");
        boolean doesThrow = ObjectType.TABLE.name().equals(objectType) && strArray.length != 3;

        if (ObjectType.DATABASE.name().equals(objectType) && strArray.length != 2) {
            doesThrow = true;
        }
        if (doesThrow) {
            throw new MetaStoreException(ErrorCode.ROLE_PRIVILEGE_OPERATION_INCONSISTENT, objectName, objectType, roleInput.getOperation());
        }
    }

    /**
     * remove privilege from role
     *
     * @param roleName
     * @param roleInput
     */
    @Override
    public void removePrivilegeFromRole(String projectId, String roleName, RoleInput roleInput) {
        if (Operation.REVOKE_ALL_OPERATION_FROM_ROLE == roleInput.getOperation()) {
            TransactionRunnerUtil.transactionRunThrow(context -> {
                String roleId = roleStore.getRoleId(context, projectId, roleName);
                CheckUtil.assertNotNull(roleId, ErrorCode.ROLE_NOT_FOUND, roleName);
                // delete all role privilege record
                roleStore.delAllRolePrivilege(context, projectId, roleId);
                return true;
            });
            return;
        }
        modifyPrivilege(projectId, roleName, roleInput, false);
    }

    /**
     * remove all privilege on object from role
     *
     * @param roleName
     * @param roleInput
     */
    @Override
    public void removeAllPrivilegeOnObjectFromRole(String projectId, String roleName, RoleInput roleInput) {
        String objectType = ObjectType.valueOf(roleInput.getObjectType().toUpperCase()).name();
        CatalogInnerObject catalogInnerObject = RolePrivilegeHelper.getCatalogObject(projectId,
            objectType, roleInput.getObjectName());
        long privilege = 0xffffffff;

        TransactionRunnerUtil.transactionRunThrow(context -> {
            String roleId = roleStore.getRoleId(context, projectId, roleName);
            CheckUtil.assertNotNull(roleId, ErrorCode.ROLE_NOT_FOUND, roleName);
            String rolePrivilegeObjectId = RolePrivilegeHelper.getRolePrivilegeObjectId(objectType, catalogInnerObject);
            RolePrivilegeObject rolePrivilegeObject = roleStore.getRolePrivilege(context, projectId,
                roleId, objectType, rolePrivilegeObjectId);
            if (rolePrivilegeObject == null) {
                throw new MetaStoreException(ErrorCode.ROLE_PRIVILEGE_INVALID);
            }

            long newPrivilege = (~privilege) & rolePrivilegeObject.getPrivilege();
            if (newPrivilege == 0) {
                roleStore.deleteRolePrivilege(context, projectId, roleId, rolePrivilegeObject.getObjectType(),
                    rolePrivilegeObject.getObjectId());
            } else {
                roleStore.updateRolePrivilege(context, projectId, rolePrivilegeObject, newPrivilege);
            }
            return true;
        });
    }

    /**
     * add user to role
     *
     * @param roleName
     * @param roleInput
     */
    @Override
    public void addUserToRole(String projectId, String roleName, RoleInput roleInput) {
        TransactionRunnerUtil.transactionRunThrow(context -> {
            String roleObjectId = roleStore.getRoleId(context, projectId, roleName);
            CheckUtil.assertNotNull(roleObjectId, ErrorCode.ROLE_NOT_FOUND, roleName);

            String[] users = roleInput.getUserId();
            for (int i = 0; i < users.length; i++) {
                roleStore.insertRoleUser(context, projectId, roleObjectId, users[i]);
            }
            return true;
        });
    }

    /**
     * remove user from role
     *
     * @param roleName
     * @param roleInput
     */
    @Override
    public void removeUserFromRole(String projectId, String roleName, RoleInput roleInput) {
        TransactionRunnerUtil.transactionRunThrow(context -> {
            String roleId = roleStore.getRoleId(context, projectId, roleName);
            CheckUtil.assertNotNull(roleId, ErrorCode.ROLE_NOT_FOUND, roleName);
            String[] users = roleInput.getUserId();
            for (int i = 0; i < users.length; i++) {
                if (!roleStore.deleteRoleUser(context, projectId, roleId, users[i])) {
                    throw new MetaStoreException(ErrorCode.ROLE_USER_RELATIONSHIP_NOT_FOUND, roleName, users[i]);
                }
            }
            return true;
        });
    }

    private List<Role> convertToRoleModel(List<RoleObject> roleObjects) {
        List<Role> roleModels = new ArrayList<>();
        Set<String> roleSet = new HashSet<>((int) (roleObjects.size() / 0.75f + 1));
        String roleId = null;
        for (RoleObject roleObject : roleObjects) {
            Role model = new Role();
            roleId = roleObject.getRoleId();
            if (roleSet.contains(roleId)) {
                continue;
            }
            if (roleObject.getCreateTime() > 0) {
                Date date = new Date(roleObject.getCreateTime());
                SimpleDateFormat SDF = new SimpleDateFormat(dateFormat);
                model.setCreatedTime(SDF.format(date));
            }
            model.setRoleName(roleObject.getRoleName());
            model.setOwner(roleObject.getOwnerId());
            model.setComment(roleObject.getComment());
            if (!CollectionUtils.isEmpty(roleObject.getToUsers())) {
                model.setToUsers(roleObject.getToUsers().toArray(new String[0]));
            }
            model.setProjectId(roleObject.getProjectId());
            model.setRoleId(roleObject.getRoleId());

            roleModels.add(model);
            roleSet.add(roleId);
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
        List<RoleObject> roleObjects = TransactionRunnerUtil.transactionRunThrow(context -> roleStore.getAllRoleObjects(context, projectId, userId, namePattern)).getResult();
        return convertToRoleModel(roleObjects);
    }

    @Override
    public List<Role> getRoleNames(String projectId, String keyword) {
        List<RoleObject> roleObjects = TransactionRunnerUtil.transactionRunThrow(context -> roleStore.getAllRoleNames(context, projectId, keyword)).getResult();
        return convertToRoleModel(roleObjects);
    }

    @Override
    public List<String> showPermObjectsByUser(String projectId, String userId, String objectType, String filterJson) {
        CheckUtil.checkStringParameter(objectType, "objectType");
        ObjectType type = ObjectType.valueOf(objectType);
        List<Role> roleList = getRoleModels(projectId, userId, null);
        // get owner is $userId
        Set<String> ownerObjectIds = getOwnerObjectIds(projectId, userId, objectType);
        log.info("roleList: {}", roleList);
        List<String> resList = new ArrayList<>();
        if (!(CollectionUtils.isEmpty(roleList)) || !CollectionUtils.isEmpty(ownerObjectIds)) {
            List<RolePrivilegeObject> privilegeObjectList = roleStore.getRoleByIds(null, projectId, objectType, roleList.stream().map(Role::getRoleId).collect(Collectors.toList()));
            //TODO
            switch (type) {
                case CATALOG:
                    resList = privilegeCatalogType(projectId, userId, privilegeObjectList, filterJson);
                    break;
                case DATABASE:
                    break;
                case TABLE:
                    break;
                default:
                    break;
            }


        }


        return resList;
    }

    private Set<String> getOwnerObjectIds(String projectId, String userId, String objectType) {
        List<UserPrivilege> userPrivileges = userPrivilegeStore.getUserPrivileges(null, projectId, userId, objectType);
        return userPrivileges.stream().map(UserPrivilege::getObjectId).collect(Collectors.toSet());
    }

    private List<String> privilegeCatalogType(String projectId, String userId, List<RolePrivilegeObject> privilegeObjectList, String filterJson) {
        TraverseCursorResult<List<Catalog>> traverseCursorResult = catalogService.getCatalogs(projectId, Integer.MAX_VALUE, "", null);
        List<Catalog> catalogList = traverseCursorResult.getResult();
        Set<String> ownerObjects = catalogList.stream().filter(x -> userId.equals(x.getOwner())).map(Catalog::getCatalogName).collect(Collectors.toSet());
        Set<String> privilegeObjects = privilegeObjectList.stream().map(RolePrivilegeObject::getObjectId).collect(Collectors.toSet());
        List<String> resList = new ArrayList<>();
        if (StringUtils.isNotEmpty(filterJson)) {
            JsonObject filterJsonObject = GsonUtil.create().fromJson(filterJson, JsonObject.class);
            Set<String> filterKeys = filterJsonObject.keySet();
            JsonArray jsonArray = GsonUtil.create().toJsonTree(catalogList).getAsJsonArray();
            Iterator<JsonElement> iterator = jsonArray.iterator();
            JsonObject jo = null;
            Set<String> colKeys = null;
            while (iterator.hasNext()) {
                jo = iterator.next().getAsJsonObject();
                colKeys = jo.keySet();
                if (colKeys.containsAll(filterKeys)) {
                    boolean passFlag = true;
                    for (String k : filterKeys) {
                        if (!filterJsonObject.get(k).equals(jo.get(k))) {
                            passFlag = false;
                            break;
                        }
                    }
                    if (passFlag) {
                        resList.add(jo.get(Catalog.Fields.catalogName).getAsString());
                    }
                }
            }
        } else {
            resList = catalogList.stream().map(Catalog::getCatalogName).collect(Collectors.toList());
        }
        log.info("privilegeObjects: {}", privilegeObjects);
        privilegeObjects.addAll(ownerObjects);
        return (List<String>) org.apache.commons.collections.CollectionUtils.intersection(privilegeObjects, resList);
    }

}
