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

import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.model.discovery.CatalogTableCount;
import io.polycat.catalog.common.model.discovery.Condition;
import io.polycat.catalog.common.model.discovery.DiscoverySearchBase;
import io.polycat.catalog.common.model.discovery.ObjectCount;
import io.polycat.catalog.common.model.discovery.TableCategories;

import java.util.List;

/**
 * interface: DiscoveryService
 */
public interface DiscoveryService {

    /**
     * Full Text Search
     *
     * @param projectId
     * @param catalogName
     * @param objectType
     * @param keywords
     * @param owner
     * @param logicalOperator
     * @param exactMatch
     * @param limit
     * @param pageToken
     * @param conditions
     * @param withCategories
     * @return
     */
    TraverseCursorResult<List<DiscoverySearchBase>> searchUsingFullText(String projectId, String catalogName, String objectType,
                                                                        String keywords, String owner, Integer categoryId, String logicalOperator, boolean exactMatch, boolean withCategories, Integer limit, String pageToken, List<Condition> conditions);

    /**
     * Initialize discovery record data, currently only supported ObjectType=Table
     *
     * @param projectId
     * @param objectType
     */
    void initDiscovery(String projectId, String objectType);

    /**
     * match discovery qualified names
     *
     * @param projectId
     * @param catalogName
     * @param objectType
     * @param keyword
     * @param owner
     * @param limit
     * @param pageToken
     * @param conditions
     * @return
     */
    TraverseCursorResult<List<String>> matchListNames(String projectId, String catalogName,
                                                      String objectType, String keyword, String owner, Integer categoryId, Integer limit, String pageToken, List<Condition> conditions);

    void addCategoryRelation(String projectId, String qualifiedName, Integer categoryId);

    void removeCategoryRelation(String projectId, String qualifiedName, Integer categoryId);

    TableCategories getTableCategories(String projectId, String qualifiedName);

    ObjectCount getObjectCountByCategory(String projectId, String catalogName, String objectType,
                                         String keywords, String owner, String logicalOperator, boolean exactMatch, Integer categoryId, List<Condition> conditions);

    List<CatalogTableCount> getTableCountByCatalog(String projectId, String catalogName, String keywords, String owner,
                                                   String logicalOperator, boolean exactMatch, List<Condition> conditions);
}
