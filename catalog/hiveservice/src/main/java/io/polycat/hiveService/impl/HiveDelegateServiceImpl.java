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
import io.polycat.catalog.common.model.DelegateBriefInfo;
import io.polycat.catalog.common.model.DelegateOutput;
import io.polycat.catalog.common.plugin.request.input.DelegateInput;
import io.polycat.catalog.service.api.DelegateService;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HiveDelegateServiceImpl implements DelegateService {

    @Override
    public DelegateOutput createDelegate(String projectId, DelegateInput delegateInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "createDelegate");
    }

    @Override
    public DelegateOutput getDelegate(String projectId, String delegateName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getDelegate");
    }

    @Override
    public void dropDelegate(String projectId, String delegateName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "dropDelegate");
    }

    @Override
    public List<DelegateBriefInfo> listDelegates(String projectId, String pattern) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "listDelegates");
    }
}
