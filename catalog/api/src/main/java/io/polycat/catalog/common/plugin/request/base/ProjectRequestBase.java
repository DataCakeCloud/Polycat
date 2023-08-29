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
package io.polycat.catalog.common.plugin.request.base;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.model.GrantObject;
import io.polycat.catalog.common.plugin.CatalogContext;

import lombok.Data;

@Data
public class ProjectRequestBase<T> {

    protected String projectId;

    protected T input;

    public ProjectRequestBase() {
    }

    public ProjectRequestBase(String projectId, T input) {
        this.projectId = projectId;
        this.input = input;
    }

    public void initContext(CatalogContext context) {
        if (Objects.isNull(this.projectId)) {
            this.projectId = context.getProjectId();
        }
    }

    public Map<String, String> getHeader(String token) {
        Map<String, String> header = new HashMap<>();
        header.put("Authorization", token);
        return header;
    }

    public T getInput() {
        return input;
    }

    public Operation getOperation() {
        return Operation.ILLEGAL_OPERATION;
    }

    public CatalogInnerObject getCatalogObject(CatalogContext context) {
        return new CatalogInnerObject(projectId, null, null, projectId);
    }

    public GrantObject getGrantObject() {
        return null;
    }
}
