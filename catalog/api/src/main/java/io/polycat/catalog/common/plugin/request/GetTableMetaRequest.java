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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.plugin.request.base.CatalogRequestBase;
import io.polycat.catalog.common.utils.GsonUtil;


public class GetTableMetaRequest extends CatalogRequestBase<Void> {
    String dbPattern;
    String tablePattern;
    List<String> tableTypes;

    public GetTableMetaRequest(String projectId, String catName, String dbPattern, String tblPattern) {
        this.projectId = projectId;
        this.catalogName = catName;
        this.dbPattern = dbPattern;
        this.tablePattern = tblPattern;
    }

    public GetTableMetaRequest(String projectId, String catName, String dbPattern, String tblPattern, List<String> tblTypes) {
        this.projectId = projectId;
        this.catalogName = catName;
        this.dbPattern = dbPattern;
        this.tablePattern = tblPattern;
        this.tableTypes = tblTypes;
    }

    public Map<String, String> getParams() {
        Map<String, String> params = new HashMap<>();
        params.put(Constants.DB_PATTERN, this.dbPattern);
        params.put(Constants.PATTERN, this.tablePattern);
        if (tablePattern != null) {
            params.put(Constants.TABLE_TYPE, GsonUtil.toJson(tableTypes));
        }
        return params;
    }

    @Override
    public Operation getOperation() {
        return Operation.GET_TABLE_META;
    }
}
