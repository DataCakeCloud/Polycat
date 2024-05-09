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

import java.util.List;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.AcceleratorObject;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.plugin.request.input.AcceleratorInput;
import io.polycat.catalog.service.api.AcceleratorService;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HiveAcceleratorServiceImpl implements AcceleratorService {

    @Override
    public void createAccelerator(DatabaseName database, AcceleratorInput acceleratorInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "createAccelerator");
    }

    @Override
    public List<AcceleratorObject> showAccelerators(DatabaseName database) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "showAccelerators");
    }

    @Override
    public void dropAccelerators(String projectId, String catalogName, String databaseName, String acceleratorName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "dropAccelerators");
    }

    @Override
    public void alterAccelerator(String projectId, String catalogName, String databaseName, String acceleratorName,
        AcceleratorInput acceleratorInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "alterAccelerator");
    }
}
