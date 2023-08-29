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
package io.polycat.catalog.store.common;

import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.CatalogHistoryObject;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogObject;
import io.polycat.catalog.store.protos.common.CatalogInfo;

public class CatalogStoreConvertor {
    public static CatalogInfo getCatalogInfo(CatalogObject catalogObject) {
        CatalogInfo catalogInfo = CatalogInfo.newBuilder()
            .setCreateTime(catalogObject.getCreateTime())
            .setOwner(catalogObject.getUserId())
            .setLocation(catalogObject.getLocationUri())
            .setDescription(catalogObject.getDescriptions())
            .setParentId(catalogObject.getParentId())
            .setParentType(catalogObject.getParentType().name())
            .setParentVersion(catalogObject.getParentVersion())
            .build();
        return catalogInfo;
    }

    public static CatalogHistoryObject trans2CatalogHistoryObject(CatalogIdent catalogIdent, String eventId, String version,
        String catalogName, String rootCatalogId, CatalogInfo catalogInfo) {
        CatalogHistoryObject catalogHistoryObject = new CatalogHistoryObject();

        catalogHistoryObject.setProjectId(catalogIdent.getProjectId());
        catalogHistoryObject.setCatalogId(catalogIdent.getCatalogId());
        catalogHistoryObject.setEventId(eventId);
        catalogHistoryObject.setName(catalogName);
        catalogHistoryObject.getProperties().putAll(catalogInfo.getParametersMap());
        catalogHistoryObject.setDropped(false);
        catalogHistoryObject.setVersion(version);
        catalogHistoryObject.setRootCatalogId(rootCatalogId);

        if (catalogInfo.hasParentId()) {
            catalogHistoryObject.setParentType(ObjectType.valueOf(catalogInfo.getParentType()));
            catalogHistoryObject.setParentId(catalogInfo.getParentId());
            catalogHistoryObject.setParentVersion(catalogInfo.getParentVersion());
        }

        catalogHistoryObject.setInvisible(catalogInfo.getInvisible());

        return catalogHistoryObject;
    }
}
