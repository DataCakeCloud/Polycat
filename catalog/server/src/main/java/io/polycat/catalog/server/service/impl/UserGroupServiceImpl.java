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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.model.PrincipalSource;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.User;
import io.polycat.catalog.common.plugin.request.input.UserGroupInput;
import io.polycat.catalog.service.api.UserGroupService;
import io.polycat.catalog.store.api.Transaction;
import io.polycat.catalog.store.api.UserGroupStore;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class UserGroupServiceImpl implements UserGroupService {

    private static final Logger log = Logger.getLogger(RoleServiceImpl.class);
    @Autowired
    private Transaction storeTransaction;
    @Autowired
    private UserGroupStore userGroupStore;


    @Override
    public void syncUser(String projectId, String domainId, UserGroupInput userInput) {
        JSONObject jsonObject = new JSONObject(userInput.getUserGroupJson());
        JSONArray userArray = JSON.parseArray(jsonObject.getString("users"));
        Map<String, String> id2Name = new HashMap<>();
        for(int i=0; i< userArray.size(); i++){
            JSONObject userInfo = userArray.getJSONObject(i);
            id2Name.put(userInfo.getString("id"), userInfo.getString("name"));
        }
        HashSet<String> remoteUserSet = new HashSet<>(id2Name.keySet());
        try (TransactionContext context = storeTransaction.openTransaction()) {
            HashSet<String> localUserSet =  userGroupStore.getAllUserIdByDomain(context, projectId, domainId);
            HashSet<String> result = new HashSet<String>();

            result.clear();
            result.addAll(remoteUserSet);
            result.removeAll(localUserSet);
            // remote exist but loacl not exist, need add user
            for (String remoteUserId : result) {
                userGroupStore.insertUserObject(context, projectId, PrincipalSource.IAM.getNum(), id2Name.get(remoteUserId), remoteUserId);
            }

            result.clear();
            result.addAll(localUserSet);
            result.removeAll(remoteUserSet);
            // loacl exist but remote not exist,  need delete user
            for (String localUserId : result) {
                userGroupStore.deleteUserObjectById(context, projectId, localUserId);
            }

            context.commit();
        }
    }

    @Override
    public void syncGroup(String projectId, String domainId, UserGroupInput groupInput) {
        JSONObject jsonObject = new JSONObject(groupInput.getUserGroupJson());
        JSONArray groupArray = JSON.parseArray(jsonObject.getString("groups"));
        Map<String, String> id2Name = new HashMap<>();
        for(int i=0; i< groupArray.size(); i++){
            JSONObject groupInfo = groupArray.getJSONObject(i);
            id2Name.put(groupInfo.getString("id"), groupInfo.getString("name"));
        }
        HashSet<String> remoteGroupSet = new HashSet<>(id2Name.keySet());
        try (TransactionContext context = storeTransaction.openTransaction()) {
            HashSet<String> localGroupSet =  userGroupStore.getAllGroupIdByDomain(context, projectId, domainId);
            HashSet<String> result = new HashSet<>();

            result.clear();
            result.addAll(remoteGroupSet);
            result.removeAll(localGroupSet);
            // remote exist but loacl not exist, need add user
            for (String remoteGroupId : result) {
                userGroupStore.insertGroupObject(context, projectId, id2Name.get(remoteGroupId), remoteGroupId);
            }

            result.clear();
            result.addAll(localGroupSet);
            result.removeAll(remoteGroupSet);
            // loacl exist but remote not exist,  need delete user
            for (String localGroupId : result) {
                userGroupStore.deleteGroupObjectById(context,projectId, localGroupId);
            }

            context.commit();
        }
    }

    @Override
    public void syncGroupUserIndex(String projectId, String domainId, Map<String, Object> groupUserInput) {
        try (TransactionContext context = storeTransaction.openTransaction()) {
            for (String groupId : groupUserInput.keySet())  {
                HashSet<String> localUserSet = userGroupStore.getUserIdSetByGroupId(context, projectId, groupId);
                HashSet<String> remoteUserSet = (HashSet<String>) groupUserInput.get(groupId);
                HashSet<String> result = new HashSet<>();

                result.clear();
                result.addAll(remoteUserSet);
                result.removeAll(localUserSet);
                // remote exist but local not exist, need add user
                for (String remoteUserId : result) {
                    userGroupStore.insertGroupUser(context, projectId, groupId, remoteUserId);
                }

                result.clear();
                result.addAll(localUserSet);
                result.removeAll(remoteUserSet);
                // local exist but remote not exist,  need delete user
                for (String localUserId : result) {
                    userGroupStore.deleteGroupUser(context,projectId, groupId, localUserId);
                }
            }

            context.commit();
        }
    }


    @Override
    public List<User> getUserListByDomain(String projectId, String domainId) {
        List<User> userList = new ArrayList<>();
        try (TransactionContext context = storeTransaction.openTransaction()) {
            HashSet<String> userSet = userGroupStore.getAllUserIdByDomain(context,projectId,domainId);
            for (String userId : userSet)  {
                User u = new User(domainId,userId);
                userList.add(u);
            }
            context.commit();
        }
        return userList;
    }

    @Override
    public List<User> getGroupListByDomain(String projectId, String domainId) {
        List<User> groupList = new ArrayList<>();
        try (TransactionContext context = storeTransaction.openTransaction()) {
            HashSet<String> userSet = userGroupStore.getAllGroupIdByDomain(context,projectId,domainId);
            for (String groupId : userSet)  {
                User u = new User(domainId,groupId);
                groupList.add(u);
            }
            context.commit();
        }
        return groupList;
    }

    @Override
    public HashSet<String> getUserSetByGroupId(String projectId, String groupId) {
        HashSet<String> userSet = new HashSet<>();
        try (TransactionContext context = storeTransaction.openTransaction()) {
            userSet = userGroupStore.getUserIdSetByGroupId(context,projectId,groupId);
            context.commit();
        }
        return userSet;
    }

    @Override
    public HashSet<String> getGroupSetByUserId(String projectId, String userId) {
        HashSet<String> groupSet = new HashSet<>();
        try (TransactionContext context = storeTransaction.openTransaction()) {
            groupSet = userGroupStore.getGroupIdSetByUserId(context,projectId,userId);
            context.commit();
        }
        return groupSet;
    }
}
