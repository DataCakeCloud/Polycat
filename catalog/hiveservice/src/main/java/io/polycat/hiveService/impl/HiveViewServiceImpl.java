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

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.ViewIdent;
import io.polycat.catalog.common.model.ViewName;
import io.polycat.catalog.common.model.ViewRecordObject;
import io.polycat.catalog.common.plugin.request.input.ViewInput;
import io.polycat.catalog.service.api.ViewService;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HiveViewServiceImpl implements ViewService {

    @Override
    public void createView(DatabaseName databaseName, ViewInput viewInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "createView");
    }

    @Override
    public void dropView(ViewName viewName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "dropView");
    }

    @Override
    public ViewRecordObject getViewByName(ViewName viewName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getViewByName");
    }

    @Override
    public ViewRecordObject getViewById(ViewIdent viewIdent) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getViewById");
    }

    @Override
    public void alterView(ViewName viewName, ViewInput viewInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "alterView");
    }
}
