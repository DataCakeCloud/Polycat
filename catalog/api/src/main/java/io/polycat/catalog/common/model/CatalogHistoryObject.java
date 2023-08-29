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
import lombok.Data;

@Data
public class CatalogHistoryObject {
    private String projectId = "";
    private String catalogId = "";
    private String eventId = "";
    private String name = "";
    private Map<String, String> properties = Collections.emptyMap();
    private boolean dropped;
    private String version = "";
    private ObjectType parentType ; // CATALOG, SHARE
    private String parentId = "";
    private String parentVersion = "";
    private String rootCatalogId = "";
    private boolean invisible;

    public CatalogHistoryObject() {
        this.properties = new LinkedHashMap<>();
    }


    public boolean hasParentId() {
        return !this.getParentId().isEmpty();
    }

    public boolean hasParentVersion() {
        return !this.getParentVersion().isEmpty();
    }

}

