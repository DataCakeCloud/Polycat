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
package io.polycat.catalog.common.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.store.protos.common.CatalogInfo;

import lombok.Data;

@Data
public class CatalogObject {
    private String projectId = "";
    private String catalogId = "";
    private String name = "";
    private Map<String, String> properties = Collections.emptyMap();
    private ObjectType parentType = ObjectType.CATALOG; // CATALOG, SHARE
    private String parentProjectId = ""; //secondary index required，the value must be the same as that of project_id.
    private String parentId = "";
    private String parentVersion = "";
    private String rootCatalogId = "";
    private long createTime;
    private boolean invisible;
    private String userId = "";
    private String descriptions = "";
    private String locationUri = "";

    public CatalogObject() {
        this.properties = new LinkedHashMap<>();
    }

    public CatalogObject(CatalogObject src) {
        this.projectId = src.getProjectId();
        this.catalogId = src.getCatalogId();
        this.name = src.getName();
        this.properties = new LinkedHashMap<>();
        this.properties.putAll(src.getProperties());
        this.parentType = src.getParentType();
        this.parentProjectId = src.getParentProjectId();
        this.parentId = src.getParentId();
        this.parentVersion = src.getParentVersion();
        this.rootCatalogId = src.getRootCatalogId();
        this.createTime = src.getCreateTime();
        this.invisible = src.isInvisible();
        this.userId = src.getUserId();
        this.descriptions = src.getDescriptions();
    }

    public CatalogObject(String projectId, String catalogId, String catalogName, String rootCatalogId,
        CatalogInfo catalogInfo) {
        this.projectId = projectId;
        this.catalogId = catalogId;
        this.name = catalogName;
        this.rootCatalogId = rootCatalogId;
        this.createTime = catalogInfo.getCreateTime();
        this.userId = catalogInfo.getOwner();
        this.locationUri = catalogInfo.getLocation();
        this.descriptions = catalogInfo.getDescription();
        if (catalogInfo.getParentId() != null) {
            this.parentId = catalogInfo.getParentId();
            this.parentVersion = catalogInfo.getParentVersion();
            this.parentType = ObjectType.valueOf(catalogInfo.getParentType());
        }
    }

    public boolean hasParentId() {
        return !this.getParentId().isEmpty();
    }

    public boolean hasParentVersion() {
        return !this.getParentVersion().isEmpty();
    }

}
