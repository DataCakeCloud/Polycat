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

import java.util.List;

import io.polycat.catalog.common.model.CatalogId;
import io.polycat.catalog.store.gaussdb.pojo.BranchRecord;
import io.polycat.catalog.store.gaussdb.pojo.CatalogCommitRecord;
import io.polycat.catalog.store.gaussdb.pojo.CatalogHistoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.CatalogRecord;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Component;

@Component
public interface CatalogMapper {

    /*
    catalog subspace
     */
    void createCatalogSubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.catalog")
    void dropCatalogSubspace(@Param("projectId") String projectId);

    void insertCatalog(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("catalogName") String catalogName, @Param("rootCatalogId") String rootCatalogId,
        @Param("catalogInfo") byte[] catalogInfo);

    @Update("UPDATE schema_${projectId}.catalog SET catalog_name = #{catalogName}, catalog_info = #{catalogInfo} "
        + "WHERE catalog_id = #{catalogId}")
    void updateCatalog(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("catalogName") String catalogName, @Param("catalogInfo") byte[] catalogInfo);

    CatalogId getCatalogId(@Param("projectId") String projectId, @Param("catalogName") String catalogName);

    CatalogRecord getCatalogById(@Param("projectId") String projectId, @Param("catalogId") String catalogId);

    CatalogRecord getCatalogByName(@Param("projectId") String projectId, @Param("catalogName") String catalogName);

    @Delete("DELETE FROM schema_${projectId}.catalog WHERE catalog_id = #{catalogId}")
    void deleteCatalog(@Param("projectId") String projectId, @Param("catalogId") String catalogId);

    List<CatalogRecord> listCatalog(@Param("projectId") String projectId, @Param("offset") long offset, @Param("count") long count);

    /*
    catalog history subspace
     */
    void createCatalogHistorySubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.catalog_history")
    void dropCatalogHistorySubspace(@Param("projectId") String projectId);

    @Update("DELETE FROM schema_${projectId}.catalog_history WHERE catalog_id = #{catalogId}")
    void deleteCatalogHistory(@Param("projectId") String projectId, @Param("catalogId") String catalogId);

    void insertCatalogHistory(@Param("projectId") String projectId,  @Param("catalogId") String catalogId,
        @Param("chId") String chId, @Param("version") String version,
        @Param("catalogName") String catalogName, @Param("rootCatalogId") String rootCatalogId,
        @Param("catalogInfo") byte[] catalogInfo);

    CatalogHistoryRecord getLatestCatalogHistory(@Param("projectId") String projectId,  @Param("catalogId") String catalogId);

    CatalogHistoryRecord getCatalogHistory(@Param("projectId") String projectId,  @Param("catalogId") String catalogId,
        @Param("version") String version);

    /*
    catalog commit subspace
     */
    void createCatalogCommitSubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.catalog_commit")
    void dropCatalogCommitSubspace(@Param("projectId") String projectId);

    @Update("DELETE FROM schema_${projectId}.catalog_commit WHERE catalog_id = #{catalogId}")
    void deleteCatalogCommit(@Param("projectId") String projectId, @Param("catalogId") String catalogId);

    void insertCatalogCommit(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("commitId") String commitId, @Param("version") String version, @Param("commitTime") long commitTime,
        @Param("operation") String operation, @Param("detail") String detail);

    @Select("SELECT EXISTS(SELECT 1 FROM schema_${projectId}.catalog_commit WHERE catalog_id = #{catalogId} AND commit_id=#{commitId})")
    Boolean catalogCommitExist(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("commitId") String commitId);

    CatalogCommitRecord getLatestCatalogCommit(@Param("projectId") String projectId, @Param("catalogId") String catalogId);

    List<CatalogCommitRecord> listCatalogCommit(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
        @Param("offset") long offset, @Param("count") long count);

    CatalogCommitRecord getCatalogCommit(@Param("projectId") String projectId, @Param("catalogId") String catalogId, @Param("commitId") String commitId);

    /*branch subspace*/
    void createBranchSubspace(@Param("projectId") String projectId);

    @Update("DROP TABLE IF EXISTS schema_${projectId}.branch")
    void dropBranchSubspace(@Param("projectId") String projectId);

    @Update("DELETE FROM schema_${projectId}.branch WHERE catalog_id = #{catalogId}")
    void deleteBranch(@Param("projectId") String projectId, @Param("catalogId") String catalogId);

    void insertBranch(@Param("projectId") String projectId, @Param("catalogId") String catalogId,
                      @Param("branchCatalogId") String branchCatalogId, @Param("parentVersion") String parentVersion,
                      @Param("parentType") String parentType);

    List<BranchRecord> listBranch(@Param("projectId") String projectId, @Param("catalogId") String catalogId, @Param("count") long count);

}
