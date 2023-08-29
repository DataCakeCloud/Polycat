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
import io.polycat.catalog.common.plugin.request.base.CatalogRequestBase;
import io.polycat.catalog.common.utils.RequestParameters;

import lombok.Data;

@Data
public class ListCatalogCommitsRequest extends CatalogRequestBase<Void> {

    private String all;

    private String limits;

    private String pageToken;

    public Map<String, String> getParams() {
        RequestParameters params = new RequestParameters();
        params.put(Constants.LIMIT, limits, "1000");
        params.put(Constants.MARKER, pageToken, "");
        return params.getMap();
    }

    @Override
    public Operation getOperation() {
        return Operation.DESC_CATALOG;
    }
}
