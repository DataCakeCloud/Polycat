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

import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.glossary.Category;
import io.polycat.catalog.store.gaussdb.pojo.CategoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.GlossaryRecord;

import java.util.List;

/**
 * @author liangyouze
 * @date 2023/12/4
 */
public interface GlossaryStore extends SubspaceStore {

    GlossaryRecord insertGlossary(TransactionContext context, String projectId, GlossaryRecord glossaryRecord);

    void deleteGlossary(TransactionContext context, String projectId, Integer id);

    void updateGlossary(TransactionContext context, String projectId, Integer id, String name, String description);

    List<GlossaryRecord> listGlossaryWithoutCategory(TransactionContext context, String projectId);

    CategoryRecord insertCategory(TransactionContext context, String projectId, CategoryRecord categoryRecord);

    void updateCategory(TransactionContext context, String projectId, CategoryRecord categoryRecord);

    void deleteCategory(TransactionContext context, String projectId, Integer id);

    CategoryRecord getCategoryById(TransactionContext context, String projectId, Integer id);

    GlossaryRecord getGlossaryById(TransactionContext context, String projectId, Integer id);

    List<CategoryRecord> listCategoryByGlossary(TransactionContext context, String projectId, Integer glossaryId, String glossaryName);

    List<Category> getCategoryRecordAndChildren(TransactionContext context, String projectId, Integer id);

    List<CategoryRecord> getCategoryRecordAndParents(TransactionContext context, String projectId, Integer id, Boolean ignoreDelete);

    GlossaryRecord getGlossaryRecordWithCategories(TransactionContext context, String projectId, Integer glossaryId,  String glossaryName);
}
