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
package io.polycat.catalog.client;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import io.polycat.catalog.client.authorization.PolicyCache;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.PrincipalSource;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.model.PrivilegeType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SDKPrivilegeCacheTest {

    private static PolicyCache policyCache;

    @BeforeAll
    public static void setUp() {
        PolyCatClient polyCatClient = SDKTestUtil.getClient();
        policyCache = new PolicyCache(polyCatClient, ".", 1000);
    }

    @Test
    public void writePolicyToFile() {
        writeFile();
        deleteFile("PolyCat_UserGroup_DataEngineer.gson");
    }

    @Test
    public void readPolicyToFile() {
        writeFile();
        assertEquals(3,readFile("PolyCat_UserGroup_DataEngineer.gson").size());
        deleteFile("PolyCat_UserGroup_DataEngineer.gson");
    }

    @Test
    public void updatePolicyToFile(){
        writeFile();
        List<MetaPrivilegePolicy> policies = new ArrayList<>();
        MetaPrivilegePolicy.Builder selectPolicy = new MetaPrivilegePolicy.Builder();
        selectPolicy.setPolicyId(UUID.randomUUID().toString().toLowerCase());
        selectPolicy.setProjectId("CD_Storage");
        selectPolicy.setEffect(true);
        selectPolicy.setObjectId("c1.d1.t2");
        selectPolicy.setObjectType(ObjectType.TABLE.getNum());
        selectPolicy.setPrivilege(PrivilegeType.SELECT.getType());
        selectPolicy.setPrincipalSource(PrincipalSource.IAM.getNum());
        selectPolicy.setPrincipalType(PrincipalType.GROUP.getNum());
        selectPolicy.setPrincipalId("DataEngineer");
        selectPolicy.setGrantAble(false);
        selectPolicy.setObligation("ROW_FILTER:(c1>2);DATA_MASK:c2:SHA2");
        policies.add(selectPolicy.build());

        MetaPrivilegePolicy.Builder denyPolicy = new MetaPrivilegePolicy.Builder();
        denyPolicy.setPolicyId("Test_Write_003");
        denyPolicy.setProjectId("CD_Storage");
        denyPolicy.setEffect(false);
        denyPolicy.setObjectId("c1.d1.t1");
        denyPolicy.setObjectType(ObjectType.TABLE.getNum());
        denyPolicy.setPrivilege(PrivilegeType.ALTER.getType());
        denyPolicy.setPrincipalSource(PrincipalSource.IAM.getNum());
        denyPolicy.setPrincipalType(PrincipalType.GROUP.getNum());
        denyPolicy.setPrincipalId("DataEngineer");
        denyPolicy.setGrantAble(true);
        policies.add(denyPolicy.build());

        List<String> deletePolicyIds = new ArrayList<>();
        deletePolicyIds.add("Test_Write_001");
        deletePolicyIds.add("Test_Write_002");
        String fileName = "PolyCat_UserGroup_DataEngineer.gson";

        try {
            Class[] parmTypes = new Class[3];
            parmTypes[0] = List.class;
            parmTypes[1] = List.class;
            parmTypes[2] = String.class;
            Method method = policyCache.getClass().getDeclaredMethod("updatePolicyToFile",
                parmTypes);
            method.setAccessible(true);
            method.invoke(policyCache, policies, deletePolicyIds, fileName);
            assertEquals(2, readFile(fileName).size());
            deleteFile(fileName);

        } catch(NoSuchMethodException e) {
            assert(false);
        } catch (InvocationTargetException e) {
            assert(false);;
        } catch (IllegalAccessException e) {
            assert(false);;
        }
    }

    private void writeFile() {
        List<MetaPrivilegePolicy> policies = new ArrayList<>();
        MetaPrivilegePolicy.Builder allowPolicy = new MetaPrivilegePolicy.Builder();
        allowPolicy.setPolicyId("Test_Write_001");
        allowPolicy.setProjectId("CD_Storage");
        allowPolicy.setEffect(true);
        allowPolicy.setObjectId("c1.d1.t1");
        allowPolicy.setObjectType(ObjectType.TABLE.getNum());
        allowPolicy.setPrivilege(PrivilegeType.DROP.getType());
        allowPolicy.setPrincipalSource(PrincipalSource.IAM.getNum());
        allowPolicy.setPrincipalType(PrincipalType.GROUP.getNum());
        allowPolicy.setPrincipalId("DataEngineer");
        allowPolicy.setGrantAble(true);
        policies.add(allowPolicy.build());

        MetaPrivilegePolicy.Builder selectPolicy = new MetaPrivilegePolicy.Builder();
        selectPolicy.setPolicyId("Test_Write_002");
        selectPolicy.setProjectId("CD_Storage");
        selectPolicy.setEffect(true);
        selectPolicy.setObjectId("c1.d1.t1");
        selectPolicy.setObjectType(ObjectType.TABLE.getNum());
        selectPolicy.setPrivilege(PrivilegeType.SELECT.getType());
        selectPolicy.setPrincipalSource(PrincipalSource.IAM.getNum());
        selectPolicy.setPrincipalType(PrincipalType.GROUP.getNum());
        selectPolicy.setPrincipalId("DataEngineer");
        selectPolicy.setGrantAble(false);
        selectPolicy.setObligation("ROW_FILTER:(c1>2);DATA_MASK:c2:SHA2");
        policies.add(selectPolicy.build());

        MetaPrivilegePolicy.Builder denyPolicy = new MetaPrivilegePolicy.Builder();
        denyPolicy.setPolicyId("Test_Write_003");
        denyPolicy.setProjectId("CD_Storage");
        denyPolicy.setEffect(true);
        denyPolicy.setObjectId("c1.d1.t1");
        denyPolicy.setObjectType(ObjectType.TABLE.getNum());
        denyPolicy.setPrivilege(PrivilegeType.ALTER.getType());
        denyPolicy.setPrincipalSource(PrincipalSource.IAM.getNum());
        denyPolicy.setPrincipalType(PrincipalType.GROUP.getNum());
        denyPolicy.setPrincipalId("DataEngineer");
        denyPolicy.setGrantAble(false);
        policies.add(denyPolicy.build());

        try {
            Class[] parmTypes = new Class[2];
            parmTypes[0] = List.class;
            parmTypes[1] = String.class;
            Method method = policyCache.getClass().getDeclaredMethod("writePolicyToFile", parmTypes);
            method.setAccessible(true);
            method.invoke(policyCache, policies, "PolyCat_UserGroup_DataEngineer.gson");
        } catch (NoSuchMethodException e) {
            assert (false);
        } catch (InvocationTargetException e) {
            assert (false);
            ;
        } catch (IllegalAccessException e) {
            assert (false);
        }
    }

    private List<MetaPrivilegePolicy> castList(Object obj) {
        List<MetaPrivilegePolicy> result = new ArrayList<MetaPrivilegePolicy>();
        if(obj instanceof List<?>)
        {
            for (Object o : (List<?>) obj)
            {
                result.add(MetaPrivilegePolicy.class.cast(o));
            }
            return result;
        }
        return result;
    }

    private void deleteFile(String fileName) {
        try{
            File file = new File(fileName);
            if(file.delete()){
                assert (true);
            }else{
                assert (false);
            }
        }catch(Exception e){
            assert (false);
        }
    }

    private List<MetaPrivilegePolicy> readFile(String fileName) {
        try {
            Class[] parameterType = new Class[1];
            parameterType[0] = String.class;
            Method readMethod = policyCache.getClass().getDeclaredMethod("readPolicyFromFile", parameterType);
            readMethod.setAccessible(true);
            Object obj = readMethod.invoke(policyCache, "PolyCat_UserGroup_DataEngineer.gson");
            return castList(obj);
        } catch (NoSuchMethodException e) {
            assert (false);
        } catch (InvocationTargetException e) {
            assert (false);
        } catch (IllegalAccessException e) {
            assert (false);
        }
        return  new ArrayList<>();
    }

    @Test
    public void getPrincipalPolicies() {
        writeFile();
        List<MetaPrivilegePolicy> policies = policyCache.getPolicyByPrincipal("UserGroup",
            "DataEngineer");
        assertEquals(policies.size(), 3);
        for (MetaPrivilegePolicy policy : policies) {
            assertEquals(policy.getPrincipalId(), "DataEngineer");
            assertEquals(policy.getObjectType(), ObjectType.TABLE.getNum());
        }
        deleteFile("PolyCat_UserGroup_DataEngineer.gson");
    }

    @Disabled
    public void  startUpdatePolicy() {
        policyCache.startUpdatePolicy();
    }

}
