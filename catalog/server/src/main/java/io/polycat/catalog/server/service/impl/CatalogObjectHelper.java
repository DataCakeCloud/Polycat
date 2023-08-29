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

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogId;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.server.util.TransactionRunnerUtil;
import io.polycat.catalog.store.api.CatalogStore;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.util.CheckUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class CatalogObjectHelper {
    private static CatalogStore catalogStore;

    @Autowired
    public void setCatalogStore(CatalogStore catalogStore) {
        CatalogObjectHelper.catalogStore = catalogStore;
    }

    public static CatalogIdent getCatalogIdent(TransactionContext context, CatalogName catalogName) {
        CatalogId catalogId = catalogStore
            .getCatalogId(context, catalogName.getProjectId(), catalogName.getCatalogName());
        CheckUtil.assertNotNull(catalogId, ErrorCode.CATALOG_NOT_FOUND, catalogName.getCatalogName());

        CatalogIdent catalogIdent = StoreConvertor
            .catalogIdent(catalogName.getProjectId(), catalogId.getCatalogId(), catalogId.getRootCatalogId());
        return catalogIdent;
    }

    public static Catalog getCatalog(CatalogName catalogName) {
        CatalogObject catalogObject = getCatalogObject(catalogName);
        return CatalogObjectConvertHelper.toCatalog(catalogObject);
    }


    public static Catalog getCatalog(CatalogIdent catalogIdent) {
        CatalogObject catalogObject = getCatalogObject(catalogIdent);
        return CatalogObjectConvertHelper.toCatalog(catalogObject);
    }

    public static CatalogObject getCatalogObject(CatalogName catalogName) {
        return TransactionRunnerUtil.transactionReadRunThrow(context -> {
            return getCatalogObject(context, catalogName);
        }).getResult();
    }

    public static CatalogObject getCatalogObject(CatalogIdent catalogIdent) {
        return TransactionRunnerUtil.transactionReadRunThrow(context -> {
            return getCatalogObject(context, catalogIdent);
        }).getResult();
    }

    public static CatalogObject getCatalogObject(TransactionContext context, CatalogName catalogName) {
        // get catalog id by name
        CatalogIdent catalogIdent = getCatalogIdent(context, catalogName);
        if (catalogIdent == null) {
            return null;
        }

        CatalogObject catalogRecord = catalogStore.getCatalogById(context, catalogIdent);
        return catalogRecord;
    }

    public static CatalogObject getCatalogObject(TransactionContext context, CatalogIdent catalogIdent) {
        CatalogObject catalogRecord = catalogStore.getCatalogById(context, catalogIdent);
        return catalogRecord;
    }

}
