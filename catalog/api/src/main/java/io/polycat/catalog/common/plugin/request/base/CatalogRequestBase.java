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
package io.polycat.catalog.common.plugin.request.base;

import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.plugin.CatalogContext;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

@Data
public class CatalogRequestBase<T> extends ProjectRequestBase<T> {

    protected String catalogName;

    protected  String shareName;

    public CatalogRequestBase() {
    }

    public CatalogRequestBase(String projectId, String catalogName, T input) {
        super(projectId, input);
        this.catalogName = catalogName;
    }

    @Override
    public void initContext(CatalogContext context) {
        super.initContext(context);
        if (StringUtils.isBlank(this.catalogName)) {
            this.catalogName = context.getCurrentCatalogName();
        }
    }

    public CatalogInnerObject getCatalogObject(CatalogContext context) {
        return new CatalogInnerObject(projectId, catalogName, null, catalogName);
    }
}
