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
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.plugin.request.base.TableRequestBase;
import io.polycat.catalog.common.utils.GsonUtil;

public class GetTableColumnStatisticRequest extends TableRequestBase<Void> {
    List<String> columns;

    public Map<String, String> getParams() {
        Map<String, String> params = new HashMap<>();
        try {
            params.put(Constants.COLUMNS, GsonUtil.toURLJson(columns));
        } catch (UnsupportedEncodingException e) {
            throw new CatalogException(e);
        }
        return params;
    }

    public GetTableColumnStatisticRequest(String projectId, String catalogName, String databaseName, String tableName,
        List<String> columns) {
        super(projectId, catalogName, databaseName, tableName, null);
        this.columns = columns;
    }
}
