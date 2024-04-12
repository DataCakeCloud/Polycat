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
package io.polycat.catalog.common.plugin.request;

import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.plugin.request.base.ProjectRequestBase;
import io.polycat.catalog.common.plugin.request.input.ShareInput;
import io.polycat.catalog.common.model.CatalogInnerObject;
import io.polycat.catalog.common.model.GrantObject;
import io.polycat.catalog.common.plugin.CatalogContext;


import lombok.Data;

@Data
public class AlterShareRequest extends ProjectRequestBase<ShareInput> {

    @Override
    public void initContext(CatalogContext context) {
        super.initContext(context);
        if (input.getUserId() == null) {
            input.setAccountId(context.getTenantName());
            input.setUserId(context.getUserName());
        }
    }

    @Override
    public Operation getOperation() {
        return Operation.ALTER_SHARE;
    }

    @Override
    public CatalogInnerObject getCatalogObject(CatalogContext context) {
        return new CatalogInnerObject(projectId, null, null, input.getObjectName());
    }

    @Override
    public GrantObject getGrantObject() {
        return new GrantObject(projectId, ObjectType.SHARE, input.getShareName());
    }
}