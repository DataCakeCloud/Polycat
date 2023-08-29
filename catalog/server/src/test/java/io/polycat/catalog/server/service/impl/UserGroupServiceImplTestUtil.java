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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.common.model.User;
import io.polycat.catalog.common.plugin.request.input.ShareInput;
import io.polycat.catalog.common.plugin.request.input.UserGroupInput;
import io.polycat.catalog.service.api.GlobalShareService;
import io.polycat.catalog.service.api.UserGroupService;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @Author: d00428635
 * @Create: 2022-05-10
 **/
@SpringBootTest
@Disabled
public class UserGroupServiceImplTestUtil extends  TestUtil {

    @Autowired
    private UserGroupService userGroupService;

    private String DOMAIN1 = "domainid11111111";
    private String DOMAIN2 = "domainid22222222";

    private String USER1A = "userid1111aaaa";
    private String USER1B = "userid1111bbbb";
    private String USER1C = "userid1111cccc";
    private String USER2A = "userid2222aaaa";
    private String USER2B = "userid2222bbbb";

    private String GROUP1A = "groupid1111aaaa";
    private String GROUP1B = "groupid1111bbbb";
    private String GROUP1C = "groupid1111cccc";
    private String GROUP2A = "groupid2222aaaa";
    private String GROUP2B = "groupid2222bbbb";

    private String STR_DOMAIN_ID = "domain_id";
    private String STR_USER_NAME = "name";
    private String STR_USER_ID = "id";
    private String STR_GROUP_NAME = "name";
    private String STR_GROUP_ID = "id";


    private Map<String,Object> generateUserInput(String domainId, String user1, String user2) {
        JSONArray jsonArray1 = new JSONArray();

        JSONObject userJson1 = new JSONObject();
        userJson1.put(STR_DOMAIN_ID, domainId);
        userJson1.put(STR_USER_NAME, "IAM_" + user1);
        userJson1.put(STR_USER_ID, user1);

        JSONObject userJson2 = new JSONObject();
        userJson2.put(STR_DOMAIN_ID, domainId);
        userJson2.put(STR_USER_NAME, "IAM_" + user2);
        userJson2.put(STR_USER_ID,user2);

        jsonArray1.add(userJson1);
        jsonArray1.add(userJson2);

        JSONObject users = new JSONObject();
        users.put("users", jsonArray1);

        Map<String,Object> jsonMap = new HashMap<>();
        for(Map.Entry<String,Object> entry : users.entrySet()) {
            jsonMap.put(entry.getKey(),entry.getValue());
        }
        return jsonMap;
    }

    private Map<String,Object> generateGroupInput(String domainId, String group1, String group2) {
        JSONArray jsonArray1 = new JSONArray();

        JSONObject groupJson1 = new JSONObject();
        groupJson1.put(STR_DOMAIN_ID, domainId);
        groupJson1.put(STR_GROUP_NAME, "IAM_" + group1);
        groupJson1.put(STR_GROUP_ID, group1);

        JSONObject groupJson2 = new JSONObject();
        groupJson2.put(STR_DOMAIN_ID, domainId);
        groupJson2.put(STR_GROUP_NAME, "IAM_" + group2);
        groupJson2.put(STR_GROUP_ID,group2);

        jsonArray1.add(groupJson1);
        jsonArray1.add(groupJson2);

        JSONObject groups = new JSONObject();
        groups.put("groups", jsonArray1);

        Map<String,Object> jsonMap = new HashMap<>();
        for(Map.Entry<String,Object> entry : groups.entrySet()) {
            jsonMap.put(entry.getKey(),entry.getValue());
        }
        return jsonMap;
    }

    @Test
    public void syncUserTest() {
        UserGroupInput userGroupInput = new UserGroupInput();

        Map<String,Object> jsonMap1 = generateUserInput(DOMAIN1, USER1A, USER1B);
        userGroupInput.setUserGroupJson(jsonMap1);
        userGroupService.syncUser(projectId, DOMAIN1, userGroupInput);

        List<User> userList = userGroupService.getUserListByDomain(projectId, DOMAIN1);
        assertEquals(2, userList.size());

        Map<String,Object> jsonMap2= generateUserInput(DOMAIN1, USER1A, USER1C);
        userGroupInput.setUserGroupJson(jsonMap2);
        userGroupService.syncUser(projectId, DOMAIN1, userGroupInput);

        userList = userGroupService.getUserListByDomain(projectId, DOMAIN1);
        assertEquals(2, userList.size());
    }

    @Test
    public void syncGroupTest() {
        UserGroupInput userGroupInput = new UserGroupInput();

        Map<String,Object> jsonMap1  = generateGroupInput(DOMAIN1, GROUP1A, GROUP1B);
        userGroupInput.setUserGroupJson(jsonMap1);
        userGroupService.syncGroup(projectId, DOMAIN1, userGroupInput);

        List<User> groupList = userGroupService.getGroupListByDomain(projectId, DOMAIN1);
        assertEquals(2, groupList.size());

        Map<String,Object> jsonMap2  = generateGroupInput(DOMAIN1, GROUP1A, GROUP1C);
        userGroupInput.setUserGroupJson(jsonMap2);
        userGroupService.syncGroup(projectId, DOMAIN1, userGroupInput);

        groupList = userGroupService.getGroupListByDomain(projectId, DOMAIN1);
        assertEquals(2, groupList.size());
    }

    @Test
    public void syncGroupUserIndexTest() {

        HashSet<String> userIdSet = new HashSet<>();
        userIdSet.add(USER1A);
        userIdSet.add(USER1B);

        JSONObject groupJson = new JSONObject();
        groupJson.put(GROUP1A, userIdSet);
        groupJson.put(GROUP1B, userIdSet);


        Map<String,Object> jsonMap = new HashMap<>();
        for(Map.Entry<String,Object> entry : groupJson.entrySet()) {
            jsonMap.put(entry.getKey(),entry.getValue());
        }

        userGroupService.syncGroupUserIndex(projectId, DOMAIN1, jsonMap);

        HashSet<String> userSet = userGroupService.getUserSetByGroupId(projectId, GROUP1A);
        assertEquals(2, userSet.size());

        userSet = userGroupService.getUserSetByGroupId(projectId, GROUP1B);
        assertEquals(2, userSet.size());

        HashSet<String> groupSet = userGroupService.getGroupSetByUserId(projectId, USER1A);
        assertEquals(2, groupSet.size());

        groupSet = userGroupService.getGroupSetByUserId(projectId, USER1B);
        assertEquals(2, groupSet.size());
    }
}