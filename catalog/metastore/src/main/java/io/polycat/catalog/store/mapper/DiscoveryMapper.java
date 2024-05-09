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
package io.polycat.catalog.store.mapper;

import io.polycat.catalog.store.gaussdb.pojo.CategoryRelationRecord;
import io.polycat.catalog.store.gaussdb.pojo.CountByCatalogRecord;
import io.polycat.catalog.store.gaussdb.pojo.CountByCategoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.DiscoveryCategoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.DiscoverySearchRecord;
import io.polycat.catalog.store.gaussdb.pojo.DiscoverySearchWithCategoriesRecord;

import java.util.List;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

@Component
public interface DiscoveryMapper {

    /**
     * discovery info subspace
     * @param projectId
     */
    void createDiscoverySubspace(@Param("projectId") String projectId);

    void createCategoryRelationSubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.discovery_info")
    void dropDiscoverySubspace(@Param("projectId") String projectId);

    void upsertDiscoveryInfo(@Param("projectId") String projectId, @Param("data") DiscoverySearchRecord searchRecord);

    List<DiscoverySearchRecord> search(@Param("projectId") String projectId, @Param("filter") String filterSql, @Param("categoryId") Integer categoryId,
                                       @Param("rankScore") String rankScoreExpress, @Param("offset") long offset, @Param("limit") int limit, @Param("isTable") boolean isTable);

    List<CountByCategoryRecord> countByCategorySearch(@Param("projectId") String projectId, @Param("filter") String filterSql, @Param("categoryId") Integer categoryId);

    List<CountByCatalogRecord> countByCatalogAndDatabaseSearch(@Param("projectId") String projectId, @Param("filter") String filterSql);

    @Delete("DELETE FROM schema_${projectId}.discovery_info WHERE qualified_name = #{qualifiedName} and object_type = #{objectType}")
    void dropDiscoveryInfo(@Param("projectId") String projectId, @Param("qualifiedName") String qualifiedName, @Param("objectType") int objectType);

    List<String> searchSimpleLikeNames(@Param("projectId") String projectId, @Param("filter") String filterSql, @Param("offset") long batchOffset, @Param("limit") int batchNum);

    DiscoverySearchRecord getDiscoveryInfoByQualifiedName(@Param("projectId") String projectId, @Param("qualifiedName") String qualifiedName);

    void addCategoryRelation(@Param("projectId") String projectId, @Param("qualifiedName") String qualifiedName, @Param("categoryIds") List<Integer> categoryIds);

    void removeCategoryRelation(@Param("projectId") String projectId, @Param("qualifiedName") String qualifiedName, @Param("categoryIds") List<Integer> categoryIds);

    List<CategoryRelationRecord> getCategoryRelationByQualifiedName(@Param("projectId") String projectId, @Param("qualifiedName") String qualifiedName);

    List<DiscoveryCategoryRecord> getDiscoveryCategories(@Param("projectId") String projectId, @Param("qualifiedName") String qualifiedName);

    List<DiscoverySearchWithCategoriesRecord> searchWithCategories(@Param("projectId") String projectId, @Param("filter") String filterSql,
         @Param("categoryId") Integer categoryId, @Param("rankScore") String rankScoreExpress, @Param("offset") long offset,
         @Param("limit") int limit, @Param("isTable") boolean isTable);
}
