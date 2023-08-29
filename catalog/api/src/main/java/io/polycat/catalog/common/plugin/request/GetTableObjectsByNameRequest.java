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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.plugin.request.base.DatabaseRequestBase;
import io.polycat.catalog.common.utils.GsonUtil;

public class GetTableObjectsByNameRequest extends DatabaseRequestBase<Void> {

    List<String> tblNames;

    public GetTableObjectsByNameRequest(String projectId, String catalogName, String databaseName,
        List<String> tblNames) {
        super(projectId, catalogName, databaseName, null);
        this.tblNames = tblNames;
    }

    public Map<String, String> getParams() {
        Map<String, String> params = new HashMap<>();
        try {
            params.put(Constants.TBL_NAMES, GsonUtil.toURLJson(tblNames));
        } catch (UnsupportedEncodingException e) {
            throw new CatalogException(e);
        }
        return params;
    }

    @Override
    public Operation getOperation() {
        return Operation.SHOW_TABLE;
    }
}
