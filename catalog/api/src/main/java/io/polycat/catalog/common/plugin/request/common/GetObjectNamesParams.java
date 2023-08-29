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
package io.polycat.catalog.common.plugin.request.common;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;


import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.exception.CatalogException;

import lombok.Setter;

public class GetObjectNamesParams implements IGetObjectNamesParams {

    String filter;
    String pattern;
    @Setter
    String maxResults;
    @Setter
    String pageToken;

    public void setPattern(String pattern) {
        try {
            this.pattern = URLEncoder.encode(pattern, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new CatalogException(e);
        }
        this.filter = null;
    }

    public void setFilter(String filter) {
        this.pattern = null;
        try {
            this.filter = URLEncoder.encode(filter, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new CatalogException(e);
        }
    }

    public Map<String, String> getParamsMap() {
        Map<String, String> map = new HashMap<>(10);
        if (this.pattern != null) {
            map.put(Constants.PATTERN, pattern);
        }

        if (this.filter != null) {
            map.put(Constants.FILTER, filter);
        }

        if (maxResults != null) {
            map.put(Constants.MAX_RESULTS, maxResults);
        }

        if (pageToken != null) {
            map.put(Constants.PAGE_TOKEN, pageToken);
        }
        return map;
    }
}
