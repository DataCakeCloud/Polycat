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
package io.polycat.catalog.store.gaussdb;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.DataLineageObject;
import io.polycat.catalog.common.model.DataSourceType;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.DataLineageStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.List;


@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class DataLineageStoreImpl implements DataLineageStore {
    @Override
    public void upsertDataLineage(TransactionContext ctx, DataLineageObject dataLineageObject) throws MetaStoreException {

    }

    @Override
    public List<DataLineageObject> listDataLineageByTableId(TransactionContext ctx, String projectId, String catalogId, String databaseId, String tableId) throws MetaStoreException {
        return null;
    }

    @Override
    public List<DataLineageObject> listDataLineageByDataSource(TransactionContext ctx, String projectId, DataSourceType dataSourceType, String dataSourceContent) throws MetaStoreException {
        return null;
    }
}
