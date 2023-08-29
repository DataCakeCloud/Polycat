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
package io.polycat.catalog.client.authorization;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.PolyCatConf;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.model.MetaPrivilegePolicy;
import io.polycat.catalog.common.model.Principal;
import io.polycat.catalog.common.utils.GsonUtil;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;

public class PolicyCache {

    private static final Logger logger = Logger.getLogger(PolicyCache.class);

    private final String cacheDir;
    private final PolyCatClient client;
    private final Gson gson;
    private long lastUpdateTime;
    private long getLastUpdateTime;
    private final long policyUpdateCycleTime;
    private final int policyStatusForDelete = 0;

    private ScheduledExecutorService scheduledExecutorService;

    /**
     * PolicyCache
     *
     * @param polyCatClient polyCatClient
     * @param cacheDir cacheDir
     * @param policyUpdateCycleTime policyUpdateCycleTime
     */
    public PolicyCache(PolyCatClient polyCatClient, String cacheDir, long policyUpdateCycleTime) {
        this.client = polyCatClient;
        this.gson = GsonUtil.create();
        this.cacheDir = cacheDir;
        this.policyUpdateCycleTime = policyUpdateCycleTime;
        this.lastUpdateTime = 0;
        this.scheduledExecutorService  = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * getInstance
     *
     * @param conf conf
     * @param polyCatClient polyCatClient
     */
    public static PolicyCache getInstance(Configuration conf, PolyCatClient polyCatClient) {
        // default values
        String cacheDir = conf.get(PolyCatConf.CACHE_DIR, ".");
        long policyUpdateCycleTime = Long.parseLong(PolyCatConf.POLICY_UPDATE_CYCLE_TIME, 1000);
        // use configuration
        String confPath = PolyCatConf.getConfPath(conf);
        if (confPath != null) {
            PolyCatConf polyCatConf = new PolyCatConf(confPath);
            cacheDir = polyCatConf.getCacheDir();
            policyUpdateCycleTime = polyCatConf.getPolicyUpdateCycleTime();
        }
        PolicyCache policyCache = new PolicyCache(polyCatClient, cacheDir, policyUpdateCycleTime);
        return policyCache;
    }

    /**
     * startUpdatePolicy
     *
     */
    public void startUpdatePolicy() {
        logger.info("start scheduled of policy cache!");
        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                updatePolicy();
            }
        }, policyUpdateCycleTime, TimeUnit.MILLISECONDS);
    }

    /**
     * stopUpdatePolicy
     *
     */
    public void stopUpdatePolicy() {
        logger.info("stop scheduled of policy cache!");
        scheduledExecutorService.shutdown();
    }

    private void updatePolicy() {
        Date date = new Date();
        Map<Principal, Map<String, Integer>> principalMapMap = new HashMap<>();
        principalMapMap = getUpdatePolicyIds(lastUpdateTime);
        if (principalMapMap.size() == 0) {
            lastUpdateTime = date.getTime();
            return;
        }
        savePolicyCache(principalMapMap);
        lastUpdateTime = date.getTime();
    }

    public List<MetaPrivilegePolicy> getPolicyByPrincipal(String principalType, String principalId) {
        List<MetaPrivilegePolicy> policyList = new ArrayList<>();
        File file = new File(cacheDir);
        File[] policyFiles = file.listFiles();
        if (policyFiles == null) {
            return policyList;
        }
        String principalPolicyFile = "PolyCat_" + principalType + "_" + principalId + ".gson";
        for (File policyFile : policyFiles) {
            if (policyFile.getName().equals(principalPolicyFile)) {
                return readPolicyFromFile(policyFile.getPath());
            }
        }
        return policyList;
    }

    private  void close(Closeable closeable) {
        if(closeable != null) {
            try {
                closeable.close();
            } catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    private List<MetaPrivilegePolicy> readPolicyFromFile(String fileName) {
        List<MetaPrivilegePolicy> policyList = new ArrayList<>();
        BufferedReader reader = null;
        FileLock fileLock = null;
        FileChannel fileChannel = null;
        FileInputStream fileInputStream = null;
        InputStreamReader inputStreamReader = null;
        PrincipalPrivilegePolicy policies = null;
        try {
            File file = new File(fileName);
            if(file.isFile()){
                fileInputStream = new FileInputStream(file);
            }
            fileChannel = fileInputStream.getChannel();
            fileLock = fileChannel.lock(0, Long.MAX_VALUE, true);
            if(fileLock != null) {
                inputStreamReader = new InputStreamReader(fileInputStream, StandardCharsets.UTF_8);
                reader = new BufferedReader(inputStreamReader);
                policies = gson.fromJson(reader, PrincipalPrivilegePolicy.class);
            }
            if((fileLock != null) && fileLock.isValid()) {
                fileLock.release();
            }

        } catch (FileNotFoundException e) {
            logger.error(fileName + "is not found.");
            return policyList;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(reader);
            close(inputStreamReader);
            close(fileChannel);
            close(fileInputStream);
        }
        if(policies == null) {
            return policyList;
        }
        return policies.policyList;
    }

    private void writePolicyToFile(List<MetaPrivilegePolicy> policies, String fileName) {
        BufferedWriter writer = null;
        FileLock fileLock = null;
        FileChannel fileChannel = null;
        FileOutputStream fileOutputStream = null;
        OutputStreamWriter outputStreamWriter = null;
        try {
            fileOutputStream = new FileOutputStream(fileName);
            fileChannel = fileOutputStream.getChannel();
            fileLock = fileChannel.tryLock();
            if(fileLock != null) {
                outputStreamWriter = new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8);
                writer = new BufferedWriter(outputStreamWriter);
                PrincipalPrivilegePolicy principalPrivilegePolicy = new PrincipalPrivilegePolicy(lastUpdateTime,
                    policies);
                gson.toJson(principalPrivilegePolicy, PrincipalPrivilegePolicy.class, writer);
                writer.flush();
            }
            if((fileLock != null) && fileLock.isValid()) {
                fileLock.release();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(writer);
            close(outputStreamWriter);
            close(fileChannel);
            close(fileOutputStream);
        }
    }

    private void updatePolicyToFile(List<MetaPrivilegePolicy> policies, List<String> deletePolicyIds, String fileName) {
        if ((policies.size() == 0) && (deletePolicyIds.size() == 0)) {
            return;
        }
        List<MetaPrivilegePolicy> policyList = readPolicyFromFile(fileName);
        if (policyList.size() == 0) {
            writePolicyToFile(policies, fileName);
            return;
        }
        List<MetaPrivilegePolicy> principalPolicies = new ArrayList<>();
        for (MetaPrivilegePolicy policy : policyList) {
            if(policy == null) {
                break;
            }
            boolean isDelete = false;
            boolean isModify = false;
            for (String policyId : deletePolicyIds) {
                if (policyId.equals(policy.getPolicyId())) {
                    isDelete = true;
                    break;
                }
            }
            for (MetaPrivilegePolicy updatePolicy : policies) {
                if (updatePolicy.getPolicyId().equals(policy.getPolicyId())) {
                    isModify = true;
                    break;
                }
            }
            if ((!isDelete) && (!isModify)) {
                principalPolicies.add(policy);
            }
        }
        principalPolicies.addAll(policies);
        writePolicyToFile(principalPolicies, fileName);
    }

    private void savePolicyCache(Map<Principal, Map<String, Integer>> updatePolicyIds) {
        if (updatePolicyIds.size() == 0) {
            return;
        }
        for (Principal principal : updatePolicyIds.keySet()) {
            List<String> deletePolicyIds = new ArrayList<>();
            List<String> addPolicyIds = new ArrayList<>();
            Map<String, Integer> policyIds = new HashMap<>();
            policyIds = updatePolicyIds.get(principal);
            for (String key : policyIds.keySet()) {
                if (policyIds.get(key) == policyStatusForDelete) {
                    deletePolicyIds.add(key);
                } else {
                    addPolicyIds.add(key);
                }
                List<MetaPrivilegePolicy> updatePolicies = getUpdatePolicies(addPolicyIds);
                String policyFileName = cacheDir + "/" + "PolyCat_" + principal.getPrincipalType() + "_"
                    + principal.getPrincipalId() + ".gson";
                updatePolicyToFile(updatePolicies, deletePolicyIds, policyFileName);
            }
        }
    }

    private Map<Principal, Map<String, Integer>> getUpdatePolicyIds(long lastUpdateTime) {
        Map<Principal, Map<String, Integer>> principalMapMap = new HashMap<>();
        return principalMapMap;
    }

    private List<MetaPrivilegePolicy> getUpdatePolicies(List<String> policyIds) {
        List<MetaPrivilegePolicy> updatePolicies = new ArrayList<>();
        return updatePolicies;
    }
}
