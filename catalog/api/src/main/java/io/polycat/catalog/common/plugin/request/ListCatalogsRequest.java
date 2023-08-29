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
package io.polycat.catalog.common.plugin.request;

import java.util.Map;

import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.plugin.request.base.ProjectRequestBase;
import io.polycat.catalog.common.utils.RequestParameters;

import lombok.Data;

@Data
public class ListCatalogsRequest extends ProjectRequestBase<Void> {
    private String limit;

    private String marker;

    private String pattern;

    private boolean includeDrop = false;

    public Map<String, String> getParams() {
        RequestParameters params = new RequestParameters();
        params.put(Constants.INCLUDE_DROP, Boolean.toString(includeDrop), "false");
        params.put(Constants.PATTERN, pattern);
        params.put(Constants.MARKER, marker, Constants.EMPTY_MARKER);
        params.put(Constants.LIMIT, limit, Constants.MAX_LIMIT_NUM);
        return params.getMap();
    }

    public ListCatalogsRequest() {
    }

    public ListCatalogsRequest(String projectId) {
        this.projectId = projectId;
    }

    @Override
    public Operation getOperation() {
        return Operation.SHOW_CATALOG;
    }
}
