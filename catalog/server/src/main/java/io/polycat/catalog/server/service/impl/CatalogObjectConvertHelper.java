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

import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.store.common.StoreConvertor;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class CatalogObjectConvertHelper {
    public static Catalog toCatalog(CatalogObject catalogObject) {
        Catalog catalog = new Catalog();
        catalog.setCatalogName(catalogObject.getName());
        catalog.setOwner(catalogObject.getUserId());
        catalog.setCreateTime(catalogObject.getCreateTime());

        if (catalogObject.hasParentId()) {
            CatalogIdent parentCatalogIdent = StoreConvertor
                .catalogIdent(catalogObject.getProjectId(), catalogObject.getParentId(),
                    catalogObject.getRootCatalogId());
            catalog.setParentName(CatalogObjectHelper.getCatalogObject(parentCatalogIdent).getName());
            catalog.setParentVersion(catalogObject.getParentVersion());
        } else {
            catalog.setParentName("");
            catalog.setParentVersion("");
        }

        catalog.setLocation(catalogObject.getLocationUri());
        catalog.setDescription(catalogObject.getDescriptions());
        return catalog;
    }

}
