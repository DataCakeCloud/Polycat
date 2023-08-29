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

import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.plugin.request.base.TableRequestBase;

import lombok.Data;

@Data
public class DeletePartitionColumnStatisticsRequest extends TableRequestBase<Void> {

    String partName;

    String colName;

    public DeletePartitionColumnStatisticsRequest(String projectId, String catalogName, String databaseName,
        String tableName, String partName, String colName) {
        super(projectId, catalogName, databaseName, tableName, null);
        this.partName = partName;
        this.colName = colName;
    }

    public Map<String, String> getParams() {
        Map<String, String> params = new HashMap<>();
        params.put(Constants.PARTITION_NAME, URLEncoder.encode(partName));
        params.put(Constants.COLUMN_NAME, URLEncoder.encode(colName));
        return params;
    }
    @Override
    public Operation getOperation() {
        return Operation.DELETE_PARTITION_COLUMN_STATISTICS;
    }
}
