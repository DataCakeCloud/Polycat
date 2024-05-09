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
package io.polycat.hiveService.impl;

import java.util.List;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.DataLineageType;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.DataLineage;
import io.polycat.catalog.common.model.LineageInfo;
import io.polycat.catalog.common.plugin.request.input.DataLineageInput;
import io.polycat.catalog.common.plugin.request.input.LineageInfoInput;
import io.polycat.catalog.common.lineage.*;
import io.polycat.catalog.service.api.DataLineageService;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HiveDataLineageServiceImpl implements DataLineageService {

    @Override
    public void recordDataLineage(DataLineageInput dataLineageInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "recordDataLineage");
    }

    @Override
    public List<DataLineage> getDataLineageByTable(String projectId, String catalogName, String databaseName,
        String tableName, DataLineageType dataLineageType) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getDataLineageByTable");
    }

    @Override
    public void updateDataLineage(String projectId, LineageInfoInput lineageInput) {

    }

    @Override
    public LineageFact getLineageJobFact(String projectId, String jobFactId) {
        return null;
    }

    @Override
    public LineageInfo getLineageGraph(String projectId, EDbType dbType, ELineageObjectType objectType, String qualifiedName, int depth, ELineageDirection lineageDirection, ELineageType lineageType, Long startTime) {
        return null;
    }
}
