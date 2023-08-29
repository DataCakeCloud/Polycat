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
package io.polycat.catalog.service.api;

import java.util.List;

import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.CatalogCommit;
import io.polycat.catalog.common.model.CatalogHistoryObject;
import io.polycat.catalog.common.model.CatalogIdent;
import io.polycat.catalog.common.model.CatalogName;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.MergeBranchInput;


public interface CatalogService {

    /**
     * create catalog
     *
     * @param projectId
     * @param catalogInput
     * @return
     */
    Catalog createCatalog(String projectId, CatalogInput catalogInput);

    /**
     * drop catalog by name
     *
     * @param catalogName CatalogName
     */
    void dropCatalog(CatalogName catalogName);

    /**
     * 根据catalogName 查询catalog
     *
     * @param catalogName
     * @return catalog
     */
    Catalog getCatalog(CatalogName catalogName);

    /**
     * 修改catalog
     *
     * @param catalogName
     * @param catalogInput
     */
    void alterCatalog(CatalogName catalogName, CatalogInput catalogInput);
    /**
     * get latest version for catalog
     *
     * @param catalogName
     * @return
     */
    CatalogHistoryObject getLatestCatalogVersion(CatalogName catalogName);

    /**
     * get catalog for version
     *
     * @param catalogIdent
     * @param version
     * @return
     */
    CatalogHistoryObject getCatalogByVersion(CatalogName catalogName, String version);

    /**
     * @param projectId
     * @param pattern
     * @return
     */
    TraverseCursorResult<List<Catalog>> getCatalogs(String projectId, Integer maxResults, String pageToken,
        String pattern);

    TraverseCursorResult<List<CatalogCommit>> getCatalogCommits(CatalogName name, Integer maxResults, String pageToken);


    List<Catalog> listSubBranchCatalogs(CatalogName catalogName);

    void mergeBranch(String projectId, MergeBranchInput mergeBranchInput);

}
