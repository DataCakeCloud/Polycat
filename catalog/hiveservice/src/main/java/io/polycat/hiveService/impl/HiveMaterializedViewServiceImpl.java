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
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.IndexInfo;
import io.polycat.catalog.common.model.IndexName;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.IndexInput;
import io.polycat.catalog.common.plugin.request.input.IndexRefreshInput;
import io.polycat.catalog.service.api.IndexService;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HiveMaterializedViewServiceImpl implements IndexService {

    @Override
    public String createIndex(DatabaseName databaseName, IndexInput indexInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "createIndex");
    }

    @Override
    public void dropIndex(IndexName indexName, boolean isHMSTable) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "dropIndex");
    }

    @Override
    public TraverseCursorResult<List<IndexInfo>> listIndexes(TableName tableName, boolean includeDrop,
        Integer maximumToScan, String pageToken, Boolean isHMSTable, DatabaseName databaseName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "listIndexes");
    }

    @Override
    public IndexInfo getIndexByName(IndexName indexName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getIndexByName");
    }

    @Override
    public void alterIndexByName(IndexName indexName, IndexRefreshInput indexRefreshInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "alterIndexByName");
    }
}
