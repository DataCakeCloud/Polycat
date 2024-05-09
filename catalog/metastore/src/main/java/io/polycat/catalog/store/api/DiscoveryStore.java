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
package io.polycat.catalog.store.api;

import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.discovery.CatalogTableCount;
import io.polycat.catalog.common.model.discovery.Condition;
import io.polycat.catalog.common.model.discovery.DiscoverySearchBase;
import io.polycat.catalog.common.model.discovery.ObjectCount;
import io.polycat.catalog.store.gaussdb.pojo.CategoryRelationRecord;
import io.polycat.catalog.store.gaussdb.pojo.DiscoveryCategoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.DiscoverySearchRecord;

import java.util.List;


public interface DiscoveryStore extends SubspaceStore {

    void createDiscoverySubspace(TransactionContext context, String projectId);

    void dropDiscoverySubspace(TransactionContext context, String projectId);

    void updateDiscoveryInfo(TransactionContext context, String projectId, DiscoverySearchRecord searchRecord);

    void dropDiscoveryInfo(TransactionContext context, String projectId, String qualifiedName, ObjectType objectType);

    List<DiscoverySearchBase> searchUsingFullText(
            TransactionContext context, String projectId, String catalogName,
            String objectType, String keywords, String owner, Integer CategoryId, String logicalOperator, int batchNum, long batchOffset, List<Condition> conditions);

    List<DiscoverySearchBase> searchUsingFullTextWithCategories(
            TransactionContext context, String projectId, String catalogName,
            String objectType, String keywords, String owner, Integer categoryId, String logicalOperator, int batchNum, long batchOffset,List<Condition> conditions);

    List<ObjectCount> countByCategoryUsingFullText(TransactionContext context, String projectId, String catalogName,
                                                   String objectType, String keywords, String owner, String logicalOperator, Integer categoryId, List<Condition> conditions);
    List<CatalogTableCount> countByCatalogUsingFullText(TransactionContext context, String projectId, String catalogName,
                                                        String keywords, String owner, String logicalOperator, List<Condition> conditions);

    List<DiscoverySearchBase> searchSimpleLike(TransactionContext context, String projectId, String catalogName, String objectType,
                                                                       String keywords, String owner, Integer categoryId, int batchNum, long batchOffset, List<Condition> conditions);

    List<DiscoverySearchBase> searchSimpleLikeWithCategories(TransactionContext context, String projectId,
                                                                                     String catalogName, String objectType, String keywords, String owner, Integer categoryId, int batchNum,
                                                                                     long batchOffset, List<Condition> conditions);

    List<ObjectCount> countByCategorySimpleLike(TransactionContext context, String projectId,
                                                          String catalogName, String objectType, String keywords, String owner, Integer categoryId,
                                                          List<Condition> conditions);

    List<CatalogTableCount> countByCatalogSimpleLike(TransactionContext context, String projectId, String catalogName,
                                                     String keywords, String owner, String logicalOperator, List<Condition> conditions);

    List<String> searchSimpleLikeNames(TransactionContext context, String projectId, String catalogName, String objectType,
                                                               String keyword, String owner, Integer categoryId, int batchNum, long batchOffset, List<Condition> conditions);

    void addCategoryRelation(TransactionContext context, String projectId, String qualifiedName, List<Integer> categoryIds);

    void removeCategoryRelation(TransactionContext context, String projectId, String qualifiedName, List<Integer> categoryIds);

    DiscoverySearchRecord getDiscoveryInfoByQualifiedName(TransactionContext context, String projectId, String qualifiedName);

    List<CategoryRelationRecord> getCategoryRelationByQualifiedName(TransactionContext context, String projectId, String qualifiedName);
    List<DiscoveryCategoryRecord> getDiscoveryCategories(TransactionContext context, String projectId, String qualifiedName);
}
