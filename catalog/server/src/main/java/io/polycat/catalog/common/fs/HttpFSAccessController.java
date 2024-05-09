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
package io.polycat.catalog.common.fs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.polycat.catalog.common.http.CatalogClientHelper;
import io.polycat.catalog.common.plugin.response.CatalogWebServiceResult;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

/**
 * @author liangyouze
 * @date 2023/7/20
 */

@Slf4j
public class HttpFSAccessController implements FSAccessController {

    @Value("${http.access-control.url:localhost}")
    private String accessControlUrl;

    @Override
    public void grantPrivilege(String userName, String userGroupName, FSOperation operation, String location, Boolean recursive) {
        if (accessControlUrl != null) {
            final String url = accessControlUrl + "/grantPrivilege";
            try {
                postRequest(url, userName, userGroupName, operation, location, recursive);
                log.info("Succeed to grant %s %s privilege to %s:%s", operation, location, userName, userGroupName);
            } catch (Exception e) {
                log.warn("Failed to grant %s %s privilege to %s:%s", operation, location, userName, userGroupName);
            }
        }
    }
    @Override
    public void revokePrivilege(String userName, String userGroupName, FSOperation operation, String location, Boolean recursive) {
        if (accessControlUrl != null) {
            final String url = accessControlUrl + "/revokePrivilege";
            try {
                postRequest(url, userName, userGroupName, operation, location, recursive);
                log.info("Succeed to revoke %s %s privilege to %s:%s", operation, location, userName, userGroupName);
            } catch (Exception e) {
                log.warn("Failed to revoke %s %s privilege to %s:%s", operation, location, userName, userGroupName);
            }
        }
    }

    private void postRequest(String url, String userName, String userGroupName, FSOperation operation, String location, Boolean recursive) throws IOException {
        final Map<String, String> entry = new HashMap<>();
        entry.put("userName", userName);
        entry.put("userGroupName", userGroupName);
        entry.put("operation", operation.name().toLowerCase());
        entry.put("location", location);
        entry.put("recursive", recursive.toString());
        final CatalogWebServiceResult result = CatalogClientHelper.post(url, entry);
        final String entity = result.getEntity();
        final JSONObject entityJson = JSON.parseObject(entity);
        final Integer code = entityJson.getInteger("code");
        if (code != 0) {
            throw new IOException(entityJson.getString("message"));
        }
    }


}
