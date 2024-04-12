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
package io.polycat.catalog.server.service.impl;

import java.util.Optional;

import io.polycat.catalog.server.util.TransactionFrameRunner;
import io.polycat.catalog.common.model.CatalogCommitObject;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.store.api.CatalogStore;
import io.polycat.catalog.store.common.StoreConvertor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class CatalogCommitHelper {
    private static CatalogStore catalogStore;

    @Autowired
    public void setCatalogStore(CatalogStore catalogStore) {
        this.catalogStore = catalogStore;
    }

    public static Optional<CatalogCommitObject> getCatalogCommit(CatalogIdent catalogIdent, String commitId) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        return runner.run(context -> {
            return catalogStore.getCatalogCommit(context, catalogIdent, commitId);
        }).getResult();
    }

    public static Boolean catalogCommitExist(CatalogIdent catalogIdent, String commitId) {
        TransactionFrameRunner runner = new TransactionFrameRunner();
        return runner.run(context -> {
            return catalogStore.catalogCommitExist(context, catalogIdent, commitId);
        }).getResult();
    }

    public static Boolean catalogCommitExist(DatabaseIdent databaseIdent, String commitId) {
        CatalogIdent catalogIdent = StoreConvertor
            .catalogIdent(databaseIdent.getProjectId(), databaseIdent.getCatalogId(), databaseIdent.getRootCatalogId());
        return catalogCommitExist(catalogIdent, commitId);
    }

    public static Boolean catalogCommitExist(TableIdent tableIdent, String commitId) {
        CatalogIdent catalogIdent = StoreConvertor
            .catalogIdent(tableIdent.getProjectId(), tableIdent.getCatalogId(), tableIdent.getRootCatalogId());
        return catalogCommitExist(catalogIdent, commitId);
    }

}
