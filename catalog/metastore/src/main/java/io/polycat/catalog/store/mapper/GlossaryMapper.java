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

import io.polycat.catalog.store.gaussdb.pojo.CategoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.GlossaryRecord;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author liangyouze
 * @date 2023/12/4
 */
public interface GlossaryMapper {

    void createGlossarySubspace(@Param("projectId") String projectId);

    void createCategorySubspace(@Param("projectId") String projectId);

    void insertGlossary(@Param("projectId") String projectId, @Param("data") GlossaryRecord glossaryRecord);

    void deleteGlossary(@Param("projectId") String projectId, @Param("id") Integer id);

    void updateGlossary(@Param("projectId") String projectId, @Param("id") Integer id, @Param("name")String name, @Param("description")String description);

    List<GlossaryRecord> listGlossaryWithoutCategory(@Param("projectId") String projectId);

    void insertCategory(@Param("projectId") String projectId, @Param("data") CategoryRecord categoryRecord);

    void updateCategory(@Param("projectId") String projectId, @Param("data") CategoryRecord categoryRecord);

    void deleteCategory(@Param("projectId") String projectId, @Param("id") Integer id);

    CategoryRecord getCategoryById(@Param("projectId") String projectId, @Param("id") Integer id);

    GlossaryRecord getGlossaryById(@Param("projectId") String projectId, @Param("id") Integer id);
    List<CategoryRecord> listCategoryByGlossary(@Param("projectId") String projectId, @Param("glossaryId") Integer glossaryId, @Param("glossaryName") String glossaryName);

    List<CategoryRecord> getCategoryRecordAndChildren(@Param("projectId") String projectId, @Param("id") Integer id);
    List<CategoryRecord> getCategoryRecordAndParents(@Param("projectId") String projectId, @Param("id") Integer id, @Param("ignoreDelete") Boolean ignoreDelete);
    GlossaryRecord getGlossaryRecordWithCategories(@Param("projectId") String projectId, @Param("glossaryId") Integer glossaryId, @Param("glossaryName") String glossaryName);
}
