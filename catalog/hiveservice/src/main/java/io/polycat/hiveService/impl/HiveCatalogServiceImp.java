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
import java.util.stream.Collectors;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogCommit;
import io.polycat.catalog.common.model.CatalogHistoryObject;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.MergeBranchInput;
import io.polycat.catalog.service.api.CatalogService;
import io.polycat.hiveService.common.HiveServiceHelper;
import io.polycat.hiveService.data.accesser.HiveDataAccessor;
import io.polycat.hiveService.data.accesser.PolyCatDataAccessor;
import io.polycat.hiveService.hive.HiveMetaStoreClientUtil;

import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "hive")
public class HiveCatalogServiceImp implements CatalogService {

    @Override
    public Catalog createCatalog(String projectId, CatalogInput catalogInput) {
        return HiveServiceHelper.HiveExceptionHandler(() -> {
            org.apache.hadoop.hive.metastore.api.Catalog catalog =
                new CatalogBuilder().setLocation(catalogInput.getLocation())
                    .setName(catalogInput.getCatalogName())
                    .setDescription(catalogInput.getDescription())
                    .create(HiveMetaStoreClientUtil.getHMSClient());
            return PolyCatDataAccessor.toCatalog(catalog);
        });
    }

    @Override
    public void dropCatalog(CatalogName catalogName) {
        HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().dropCatalog(catalogName.getCatalogName()));
    }

    @Override
    public Catalog getCatalog(CatalogName catalogName) {
        return HiveServiceHelper.HiveExceptionHandler(() -> {
            org.apache.hadoop.hive.metastore.api.Catalog hiveCatalog = HiveMetaStoreClientUtil.getHMSClient()
                .getCatalog(catalogName.getCatalogName());
            return PolyCatDataAccessor.toCatalog(hiveCatalog);
        });
    }

    @Override
    public void alterCatalog(CatalogName catalogName, CatalogInput catalogInput) {
        HiveServiceHelper.HiveExceptionHandler(() -> {
            org.apache.hadoop.hive.metastore.api.Catalog hiveCatalog = HiveDataAccessor.toCatalog(catalogInput);
            HiveMetaStoreClientUtil.getHMSClient().alterCatalog(catalogName.getCatalogName(), hiveCatalog);
        });
    }

    @Override
    public CatalogHistoryObject getLatestCatalogVersion(CatalogName catalogName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getLatestCatalogVersion");
    }

    @Override
    public CatalogHistoryObject getCatalogByVersion(CatalogName catalogName, String version) {

        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getCatalogByVersion");
    }

    @Override
    public TraverseCursorResult<List<Catalog>> getCatalogs(String projectId, Integer maxResults,
        String pageToken, String pattern) {
        List<String> hiveCatalogNames = HiveServiceHelper.HiveExceptionHandler(
            () -> HiveMetaStoreClientUtil.getHMSClient().getCatalogs());
        List<Catalog> polyCatCatalog = hiveCatalogNames.stream().map(PolyCatDataAccessor::toCatalog)
            .collect(Collectors.toList());
        return new TraverseCursorResult<>(polyCatCatalog, null);
    }

    @Override
    public TraverseCursorResult<List<CatalogCommit>> getCatalogCommits(CatalogName name, Integer maxResults,
        String pageToken) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "getCatalogCommits");
    }

    @Override
    public List<Catalog> listSubBranchCatalogs(CatalogName catalogName) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "listSubBranchCatalogs");
    }

    @Override
    public void mergeBranch(String projectId, MergeBranchInput mergeBranchInput) {
        throw new CatalogServerException(ErrorCode.FEATURE_NOT_SUPPORT, "mergeBranch");
    }

}
