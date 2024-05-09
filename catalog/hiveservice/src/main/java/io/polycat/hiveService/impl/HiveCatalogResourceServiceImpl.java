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

import com.google.common.collect.Lists;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.service.api.CatalogResourceService;

import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HiveCatalogResourceServiceImpl implements CatalogResourceService {

    @Override
    public void init() {

    }

    @Override
    public void createResource(String projectId) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "createResource");
    }

    @Override
    public Boolean doesExistResource(String projectId) {
        return true;
    }

    /**
     * projectId exists
     *
     * @param projectId projectId
     * @return
     */
    @Override
    public Boolean doesExistsProjectId(String projectId) {
        return true;
    }

    @Override
    public void dropResource(String projectId) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "dropResource");
    }

    @Override
    public void resourceCheck() {

    }

    @Override
    public List<String> listProjects() {
        return Lists.newArrayList();
    }
}
