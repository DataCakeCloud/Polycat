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
import java.util.Map;
import java.util.Optional;

import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.exception.CatalogException;

import lombok.Data;

@Data
public class ListFunctionRequest extends FunctionRequestBase {
    private Optional<String> expression;

    public ListFunctionRequest(String projectId, String catalogName, String dbName, String pattern) {
        this.projectId = projectId;
        this.catalogName = catalogName;
        this.databaseName = dbName;
        this.expression = Optional.ofNullable(pattern);
    }

    public Map<String, String> getParams() {
        Map<String, String> params = new HashMap<>();
        expression.ifPresent(x -> {
            try {
                params.put(Constants.PATTERN, URLEncoder.encode(x, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new CatalogException(e.getMessage());
            }
        });
        return params;
    }
    @Override
    public Operation getOperation() {
        return Operation.GET_FUNCTION;
    }
}
