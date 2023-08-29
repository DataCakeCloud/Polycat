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
package io.polycat.catalog.common.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import lombok.Getter;


public class CatalogToken {

    private String checkSum;

    @Getter
    private String readVersion;

    private Map<String, String> contextMap;

    public CatalogToken(String checkInfo, Map<String, String> map, String version) {
        readVersion = version;
        checkSum = checkInfo;
        contextMap = map;

    }
    public static CatalogToken buildCatalogToken(String checkInfo, String key, String value, String readVersion) {
        Map<String,String> map = new HashMap<>();
        map.put(key, value);
        return new CatalogToken(checkInfo, map, readVersion);
    }

    public CatalogToken(String checkInfo, String version) {
        readVersion = version;
        checkSum = checkInfo;
        contextMap = Collections.emptyMap();;
    }

    public CatalogToken(String checkInfo, Map<String, String> map) {
        checkSum = checkInfo;
        contextMap = map;
    }

    public static CatalogToken buildCatalogToken(String checkInfo, String key, String value) {
        Map<String,String> map = new HashMap<>();
        map.put(key, value);
        return new CatalogToken(checkInfo, map);
    }

    public Boolean checkValid(String checkInfo) {
        return checkSum.equals(checkInfo);
    }

    public static Optional<CatalogToken> parseToken(String token, String checkSum) {
        if (token == null) {
            return Optional.empty();
        }

        try {
            String code = new String(CodecUtil.base642Bytes(token));
            CatalogToken catalogToken = GsonUtil.fromJson(code, CatalogToken.class);
            if (catalogToken.checkValid(checkSum)) {
                return Optional.of(catalogToken);
            }
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public String toString() {
        String code = GsonUtil.toJson(this);
        return CodecUtil.bytes2Base64(code.getBytes());
    }

    public String getContextMapValue(String key) {
        return contextMap.get(key);
    }

    public void putContextMap(String key, String value) {
        contextMap.put(key, value);
    }
}
