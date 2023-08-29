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
package io.polycat.catalog.service.api;

import java.util.List;

import io.polycat.catalog.common.DataLineageType;
import io.polycat.catalog.common.model.DataLineage;
import io.polycat.catalog.common.plugin.request.input.DataLineageInput;

public interface DataLineageService {
    /**
     * record data lineage
     * @param dataLineageInput dataLineageInput
     */
    void recordDataLineage(DataLineageInput dataLineageInput);

    /**
     * get SourceTables by table
     *
     * @param projectId    projectId
     * @param catalogName  catalogName
     * @param databaseName databaseName
     * @param tableName    tableName
     * @param dataLineageType    dataLineageType
     * @return List<DataLineage>
     */
    List<DataLineage> getDataLineageByTable(String projectId, String catalogName, String databaseName,
        String tableName, DataLineageType dataLineageType);
}
