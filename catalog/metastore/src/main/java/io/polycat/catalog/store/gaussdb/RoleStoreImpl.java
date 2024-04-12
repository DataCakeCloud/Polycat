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

import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.model.RoleObject.Fields;
import io.polycat.catalog.common.plugin.request.input.ShowRolePrivilegesInput;
import io.polycat.catalog.store.api.RoleStore;
import io.polycat.catalog.store.common.StoreSqlConvertor;
import io.polycat.catalog.store.gaussdb.pojo.*;
import io.polycat.catalog.store.mapper.RolePrivilegeMapper;
import io.polycat.catalog.store.mapper.RoleObjectNameMapper;
import io.polycat.catalog.store.mapper.RolePropertiesMapper;
import io.polycat.catalog.store.mapper.RoleUserMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class RoleStoreImpl implements RoleStore {

    private static final Logger logger = Logger.getLogger(RoleStoreImpl.class);

    @Autowired
    RolePrivilegeMapper rolePrivilegeMapper;

    @Autowired
    RolePropertiesMapper rolePropertiesMapper;

    @Autowired
    RoleObjectNameMapper roleObjectNameMapper;

    @Autowired
    RoleUserMapper roleUserMapper;

    private static class RoleStoreImplHandler {
        private static final RoleStoreImpl INSTANCE = new RoleStoreImpl();
    }

    public static RoleStoreImpl getInstance() {
        return RoleStoreImplHandler.INSTANCE;
    }

    @Override
    public void createRoleSubspace(TransactionContext context, String projectId) {
        rolePropertiesMapper.createRolePropertiesSubspace(projectId);
        rolePrivilegeMapper.createRolePrivilegeSubspace(projectId);
        roleObjectNameMapper.createRoleObjectNameSubspace(projectId);
        roleUserMapper.createRoleUserSubspace(projectId);
    }

    @Override
    public void dropRoleSubspace(TransactionContext context, String projectId) {
        rolePropertiesMapper.dropRolePropertiesSubspace(projectId);
        rolePrivilegeMapper.dropRolePrivilegeSubspace(projectId);
        roleObjectNameMapper.dropRoleObjectNameSubspace(projectId);
        roleUserMapper.dropRoleUserSubspace(projectId);
    }

    @Override
    public String generateRoleObjectId(TransactionContext context, String projectId) {
        return SequenceHelper.generateObjectId(context, projectId);
    }


    @Override
    public Boolean roleObjectNameExist(TransactionContext context, String projectId, String roleName) throws MetaStoreException {
        return roleObjectNameMapper.getObjectNameByName(projectId, roleName) != null;
    }

    @Override
    public void insertRoleObjectName(TransactionContext context, String projectId, String roleName, String roleId) throws MetaStoreException {
        roleObjectNameMapper.insertRoleObjectName(projectId, roleName, roleId);
    }

    @Override
    public String getRoleId(TransactionContext context, String projectId, String roleName) throws MetaStoreException {
        RoleObjectNameRecord objectName = roleObjectNameMapper.getObjectNameByName(projectId, roleName);
        if (objectName != null) {
            return objectName.getObjectId();
        }
        return null;
    }

    @Override
    public void deleteRoleObjectName(TransactionContext context, String projectId, String roleName) throws MetaStoreException {
        roleObjectNameMapper.deleteRoleObjectName(projectId, roleName);
    }

    @Override
    public void insertRoleProperties(TransactionContext context, String projectId, String roleId, String roleName, String ownerId, String comment) throws MetaStoreException {
        RolePropertiesRecord record = new RolePropertiesRecord();
        record.setRoleId(roleId);
        record.setName(roleName);
        record.setComment(comment);
        record.setOwnerId(ownerId);
        record.setCreateTime(RecordStoreHelper.getCurrentTime());
        rolePropertiesMapper.insertRoleProperties(projectId, record);
    }

    @Override
    public void updateRoleProperties(TransactionContext context, RoleObject roleObject) throws MetaStoreException {
        RolePropertiesRecord record = new RolePropertiesRecord();
        record.setRoleId(roleObject.getRoleId());
        record.setOwnerId(roleObject.getOwnerId());
        record.setComment(roleObject.getComment());
        record.setName(roleObject.getRoleName());
        record.setCreateTime(roleObject.getCreateTime());
        rolePropertiesMapper.updateRoleProperties(roleObject.getProjectId(), record);
    }

    @Override
    public RoleObject getRoleProperties(TransactionContext context, String projectId, String roleId) throws MetaStoreException {
        RolePropertiesRecord record = rolePropertiesMapper.getRoleProperties(projectId, roleId);
        if (record == null) {
            return null;
        }
        return new RoleObject(projectId, record.getName(), record.getRoleId(), record.getOwnerId(), record.getCreateTime(), record.getComment());
    }

    @Override
    public void deleteRoleProperties(TransactionContext context, String projectId, String roleId) throws MetaStoreException {
        rolePropertiesMapper.deleteRoleProperties(projectId, roleId);
    }

    @Override
    public void insertRoleUser(TransactionContext context, String projectId, String roleId, String userId) throws MetaStoreException {
        roleUserMapper.insertRoleUser(projectId, roleId, userId);
    }

    @Override
    public List<RoleUserObject> getRoleUsersByRoleId(TransactionContext context, String projectId, String roleId) throws MetaStoreException {
        List<RoleUserRecord> roleUserRecords = roleUserMapper.getRoleUsersByRoleId(projectId, roleId);
        return roleUserRecords.stream().map(x -> new RoleUserObject(x.getRoleId(), x.getUserId())).collect(Collectors.toList());
    }

    @Override
    public List<RoleUserObject> getRoleUsersByUserId(TransactionContext context, String projectId, String userId) throws MetaStoreException {
        List<RoleUserRecord> roleUserRecords = roleUserMapper.getRoleUsersByUserId(projectId, userId);
        return roleUserRecords.stream().map(x -> new RoleUserObject(x.getRoleId(), x.getUserId())).collect(Collectors.toList());
    }

    @Override
    public boolean deleteRoleUser(TransactionContext context, String projectId, String roleId, String userId) throws MetaStoreException {
        return roleUserMapper.deleteRoleUser(projectId, roleId, userId) > 0;
    }

    @Override
    public void delAllRoleUser(TransactionContext context, String projectId, String roleId) throws MetaStoreException {
        roleUserMapper.delAllRoleUser(projectId, roleId);
    }

    @Override
    public void insertRolePrivilege(TransactionContext context, String projectId, String roleId, String objectType, String rolePrivilegeObjectId, CatalogInnerObject catalogInnerObject, long privilege) throws MetaStoreException {
        RolePrivilegeRecord rolePrivilegeRecord = new RolePrivilegeRecord(roleId, objectType, rolePrivilegeObjectId, privilege, catalogInnerObject.getCatalogId(), catalogInnerObject.getDatabaseId());
        rolePrivilegeMapper.insertRolePrivilege(projectId, rolePrivilegeRecord);
    }

    @Override
    public void updateRolePrivilege(TransactionContext context, String projectId, RolePrivilegeObject rolePrivilegeObject, long newPrivilege) throws MetaStoreException {
        rolePrivilegeMapper.updateRolePrivilege(projectId, rolePrivilegeObject.getRoleId(), rolePrivilegeObject.getObjectId(), rolePrivilegeObject.getObjectType(), newPrivilege);
    }

    @Override
    public RolePrivilegeObject getRolePrivilege(TransactionContext context, String projectId, String roleId, String objectType, String rolePrivilegeObjectId) throws MetaStoreException {
        RolePrivilegeRecord rolePrivilege = rolePrivilegeMapper.getRolePrivilege(projectId, roleId, rolePrivilegeObjectId, objectType);
        if (rolePrivilege != null) {
            return  new RolePrivilegeObject(rolePrivilege.getRoleId(), rolePrivilege.getObjectType(), rolePrivilege.getObjectId(), rolePrivilege.getPrivilege(), rolePrivilege.getCatalogId(), rolePrivilege.getDatabaseId());
        }
        return null;
    }

    @Override
    public List<RolePrivilegeObject> getRolePrivilege(TransactionContext context, String projectId, String roleId) throws MetaStoreException {
        List<RolePrivilegeRecord> rolePrivileges = rolePrivilegeMapper.getRolePrivileges(projectId, roleId);
        return rolePrivileges.stream().map(x -> new RolePrivilegeObject(x.getRoleId(), x.getObjectType(), x.getObjectId(), x.getPrivilege(), x.getCatalogId(), x.getDatabaseId())).collect(Collectors.toList());
    }

    @Override
    public void deleteRolePrivilege(TransactionContext context, String projectId, String roleId, String objectType, String objectId) throws MetaStoreException {
        rolePrivilegeMapper.deleteRolePrivilege(projectId, roleId, objectType, objectId);
    }

    @Override
    public void delAllRolePrivilege(TransactionContext context, String projectId, String roleId) throws MetaStoreException {
        rolePrivilegeMapper.delAllRolePrivilege(projectId, roleId);
    }

    @Override
    public void removeAllPrivilegeOnObject(TransactionContext context, String projectId, String objectType, String rolePrivilegeObjectId) throws MetaStoreException {
        rolePrivilegeMapper.removeAllPrivilegeOnObject(projectId, objectType, rolePrivilegeObjectId);
    }

    @Override
    public List<RoleObject> getAllRoleObjects(TransactionContext context, String projectId, String userId,
                                              String namePattern, boolean containOwner) {
        String roleUserFilterSql = StoreSqlConvertor.get()
                .equals(RoleUserRecord.Fields.userId, userId).getFilterSql();
        List<String> roleIds = roleUserMapper.getRoleIds(projectId, roleUserFilterSql);
        if (containOwner) {
            String rolePropertiesFilterSql = StoreSqlConvertor.get()
                    .equals(RolePropertiesRecord.Fields.ownerId, userId).AND()
                    .likeRight(RolePropertiesRecord.Fields.name, namePattern).getFilterSql();
            roleIds.addAll(rolePropertiesMapper.getRolePropertiesList(projectId, rolePropertiesFilterSql).stream().map(RolePropertiesRecord::getRoleId).collect(Collectors.toList()));
        }

        List<RoleObject> roleObjects = new ArrayList<>();
        if ((!StringUtils.isEmpty(userId) || !StringUtils.isEmpty(namePattern)) && roleIds.isEmpty()) {
            return roleObjects;
        }
        String filterSql = StoreSqlConvertor.get().in(RolePropertiesRecord.Fields.roleId, new HashSet<>(roleIds)).AND()
                .likeRight(RolePropertiesRecord.Fields.name, namePattern).getFilterSql();
        List<RoleObjectRecord> roleObjectRecords = rolePropertiesMapper.showRoleObjectsByFilter(projectId, filterSql);
        roleObjectRecords.forEach(record -> roleObjects.add(convertRoleObject(projectId, record)));
        return roleObjects;
    }

    private RoleObject convertRoleObject(String projectId, RoleObjectRecord record) {
        RoleObject roleObject = new RoleObject(projectId, record.getRoleName(), record.getRoleId(), record.getOwnerId(),
            record.getCreateTime(), record.getComment());
        if (StringUtils.isNotBlank(record.getUsers())) {
            roleObject.setToUsers(Arrays.asList(record.getUsers().split(",")));
        }
        return roleObject;
    }

    @Override
    public List<RoleObject> getAllRoleNames(TransactionContext context, String projectId, String keyword) {
        List<RoleObjectNameRecord> roleObjectNameRecords = roleObjectNameMapper.searchRoleNames(projectId, keyword);
        return roleObjectNameRecords.stream().map(x -> new RoleObject(projectId, x.getName(), x.getObjectId())).collect(Collectors.toList());
    }

    @Override
    public List<RolePrivilegeObject> getRoleByIds(TransactionContext context, String projectId, String objectType, List<String> roleIds) {
        if (CollectionUtils.isEmpty(roleIds)) {
            return new ArrayList<>();
        }
        StoreSqlConvertor sqlConvertor = StoreSqlConvertor.get().equals(RolePrivilegeObject.Fields.objectType, objectType).AND()
                .in(RolePrivilegeObject.Fields.roleId, roleIds);
        return getRolePrivilegesByFilter(context, projectId, sqlConvertor.getFilterSql());
    }

    @Override
    public List<RolePrivilegeObject> showRolePrivileges(TransactionContext context, String projectId, List<String> roleIds,
        ShowRolePrivilegesInput input, int batchNum, long batchOffset) {
        return getRolePrivilegesByFilter(context, projectId,
            getRoleAndPrivilegeCommonSql(roleIds, input),
            batchNum, batchOffset);
    }

    private String getRoleAndPrivilegeCommonSql(List<String> roleIds, ShowRolePrivilegesInput input) {
        return StoreSqlConvertor.get()
            .in(Fields.roleId, roleIds).AND()
            .equals(RolePrivilegeObject.Fields.objectType, input.getObjectType()).AND()
            .in(RolePrivilegeObject.Fields.objectId, input.getExactObjectNames()).AND()
            .likeRight(RolePrivilegeObject.Fields.objectId, input.getObjectNamePrefix())
            .getFilterSql();
    }

    @Override
    public List<PrivilegeRolesObject> showPrivilegeRoles(TransactionContext context, String projectId,
        List<String> roleIds, ShowRolePrivilegesInput input, int batchNum, long batchOffset) {
        return rolePrivilegeMapper.getPrivilegeRolesByFilter(projectId, getRoleAndPrivilegeCommonSql(roleIds, input), batchNum, batchOffset);
    }

    @Override
    public List<RoleObject> showRoleInfos(TransactionContext context, String projectId, ShowRolePrivilegesInput input) {
        List<RoleObjectRecord> roleInfoByFilter = rolePrivilegeMapper.getRoleInfoByFilter(projectId,
            StoreSqlConvertor.get().in(RolePropertiesRecord.Fields.name, input.getExactRoleNames()).AND()
                .likeRight(RolePropertiesRecord.Fields.name, input.getExcludeRolePrefix(), true).AND()
                .likeRight(RolePropertiesRecord.Fields.name, input.getIncludeRolePrefix()).getFilterSql(),
            StoreSqlConvertor.get().equals(RoleUserRecord.Fields.userId, input.getUserId()).getFilterSql(),
            StoreSqlConvertor.get().equals(RolePrivilegeObject.Fields.objectType, input.getObjectType()).AND()
                .in(RolePrivilegeObject.Fields.objectId, input.getExactObjectNames()).AND()
                .likeRight(RolePrivilegeObject.Fields.objectId, input.getObjectNamePrefix())
                .getFilterSql());
        return roleInfoByFilter.stream().map(record -> convertRoleObject(projectId, record)).collect(Collectors.toList());
    }

    private List<RolePrivilegeObject> getRolePrivilegesByFilter(TransactionContext context, String projectId, String filterSql) {
        return getRolePrivilegesByFilter(context, projectId, filterSql, Integer.MAX_VALUE, 0L);
    }

    private List<RolePrivilegeObject> getRolePrivilegesByFilter(TransactionContext context, String projectId, String filterSql, Integer limit, Long offset) {
        List<RolePrivilegeRecord> privilegeRecords = rolePrivilegeMapper.getRolePrivilegesByFilter(projectId, filterSql, limit, offset);
        return privilegeRecords.stream().map(x -> new RolePrivilegeObject(x.getRoleId(), x.getObjectType(), x.getObjectId(), x.getPrivilege(), x.getCatalogId(), x.getDatabaseId())).collect(Collectors.toList());
    }

}
