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

import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.plugin.request.base.TableRequestBase;

import lombok.Data;

@Data
public class GetTableRequest extends TableRequestBase<Void> {

    // Params below is optional in the sql statement
    private boolean extended = false;

    public GetTableRequest(String tableName) {
        this.tableName = tableName;
    }

    public GetTableRequest(String projectId, String catalogName, String databaseName, String tableName) {
        this.projectId = projectId;
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public Operation getOperation() {
        return Operation.DESC_TABLE;
    }
}
