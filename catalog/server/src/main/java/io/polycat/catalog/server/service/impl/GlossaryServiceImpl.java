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
package io.polycat.catalog.server.service.impl;

import io.polycat.catalog.server.util.TransactionRunnerUtil;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.glossary.Category;
import io.polycat.catalog.common.model.glossary.Glossary;
import io.polycat.catalog.common.plugin.request.input.CategoryInput;
import io.polycat.catalog.common.plugin.request.input.GlossaryInput;
import io.polycat.catalog.common.utils.TreeBuilder;
import io.polycat.catalog.service.api.GlossaryService;
import io.polycat.catalog.store.api.GlossaryStore;
import io.polycat.catalog.store.gaussdb.pojo.CategoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.GlossaryRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author liangyouze
 * @date 2023/12/4
 */

@Slf4j
@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class GlossaryServiceImpl implements GlossaryService {

    @Autowired
    GlossaryStore glossaryStore;

    @Override
    public Glossary createGlossary(String projectId, GlossaryInput glossaryInput) {
        final GlossaryRecord glossaryRecord = new GlossaryRecord(glossaryInput.getName(), glossaryInput.getDescription());
        final GlossaryRecord insertGlossary = glossaryStore.insertGlossary(null, projectId, glossaryRecord);
        return new Glossary(insertGlossary.getId(), insertGlossary.getName(), insertGlossary.getDescription());
    }

    @Override
    public void deleteGlossary(String projectId, Integer id) {
        glossaryStore.deleteGlossary(null, projectId, id);
    }

    @Override
    public void alterGlossary(String projectId, Integer id, GlossaryInput glossaryInput) {
        glossaryStore.updateGlossary(null, projectId, id, glossaryInput.getName(), glossaryInput.getDescription());
    }

    @Override
    public List<Glossary> listGlossaryWithoutCategory(String projectId) {
        final List<GlossaryRecord> glossaryRecords = glossaryStore.listGlossaryWithoutCategory(null, projectId);
        return glossaryRecords.stream().map(glossaryRecord -> new Glossary(glossaryRecord.getId(), glossaryRecord.getName(), glossaryRecord.getDescription(),
                        glossaryRecord.getCreateTime().getTime(), glossaryRecord.getUpdateTime().getTime(), null))
                .collect(Collectors.toList());
    }

    @Override
    public Glossary getGlossary(String projectId, Integer id, String name) {
        if (id == null && (name == null || name.isEmpty())) {
            throw new CatalogServerException("At least one of the id or name must be specified", ErrorCode.ARGUMENT_ILLEGAL);
        }
        final GlossaryRecord glossaryRecord = glossaryStore.getGlossaryRecordWithCategories(null, projectId, id, name);
        if (glossaryRecord != null) {
            final List<Category> categories = glossaryRecord.getCategoryRecords().stream()
                    .map(categoryRecord -> new Category(categoryRecord.getId(), categoryRecord.getName(),
                            categoryRecord.getDescription(), categoryRecord.getParentId(), categoryRecord.getCreateTime().getTime(),
                            categoryRecord.getUpdateTime().getTime())).collect(Collectors.toList());
            final Glossary glossary = new Glossary(glossaryRecord.getId(), glossaryRecord.getName(), glossaryRecord.getDescription(),
                    glossaryRecord.getCreateTime().getTime(), glossaryRecord.getUpdateTime().getTime(), null);
            glossary.setCategories(TreeBuilder.buildTree(categories, -1));
            return glossary;
        }
        return null;
    }

    @Override
    public Category createCategory(String projectId, CategoryInput categoryInput) {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            final Integer glossaryId = categoryInput.getGlossaryId();
            final GlossaryRecord glossary = glossaryStore.getGlossaryById(context, projectId, glossaryId);
            if (glossary == null) {
                throw new CatalogServerException("glossary is not exists", ErrorCode.ARGUMENT_ILLEGAL);
            }
            final Integer parentCategoryId = categoryInput.getParentCategoryId();
            if (parentCategoryId != null) {
                final CategoryRecord parentCategory = glossaryStore.getCategoryById(context, projectId, parentCategoryId);
                if (parentCategory == null) {
                    throw new CatalogServerException("parent category is not exists", ErrorCode.ARGUMENT_ILLEGAL);
                }
            }

            final CategoryRecord categoryRecord =
                    new CategoryRecord(categoryInput.getName(), categoryInput.getDescription(), categoryInput.getParentCategoryId(), categoryInput.getGlossaryId());
            final CategoryRecord insertCategory = glossaryStore.insertCategory(context, projectId, categoryRecord);
            return new Category(insertCategory.getId(), insertCategory.getName(), insertCategory.getDescription(), insertCategory.getParentId(), insertCategory.getGlossaryId());
        }).getResult();
    }

    @Override
    public void alterCategory(String projectId, Integer id, CategoryInput categoryInput) {

        TransactionRunnerUtil.transactionRunThrow(context -> {
            final Integer glossaryId = categoryInput.getGlossaryId();
            if (glossaryId != null) {
                final GlossaryRecord glossary = glossaryStore.getGlossaryById(context, projectId, glossaryId);
                if (glossary == null) {
                    throw new CatalogServerException("glossary is not exists", ErrorCode.ARGUMENT_ILLEGAL);
                }
            }
            final Integer parentCategoryId = categoryInput.getParentCategoryId();
            if (parentCategoryId != null) {
                if (parentCategoryId.equals(id)) {
                    throw new CatalogServerException("parent category cannot be self", ErrorCode.ARGUMENT_ILLEGAL);
                }
                final CategoryRecord parentCategory = glossaryStore.getCategoryById(context, projectId, parentCategoryId);
                if (parentCategory == null) {
                    throw new CatalogServerException("parent category is not exists", ErrorCode.ARGUMENT_ILLEGAL);
                }
            }
            final CategoryRecord categoryRecord =
                    new CategoryRecord(id, categoryInput.getName(), categoryInput.getDescription(), categoryInput.getParentCategoryId(), categoryInput.getGlossaryId());
            glossaryStore.updateCategory(null, projectId, categoryRecord);
            return null;
        }).getResult();

    }

    @Override
    public void deleteCategory(String projectId, Integer id) {
        glossaryStore.deleteCategory(null, projectId, id);
    }

    @Override
    public Category getCategory(String projectId, Integer id) {
        final List<Category> categories = glossaryStore.getCategoryRecordAndChildren(null, projectId, id);
        final Optional<Category> optional = categories.stream().filter(record -> record.getId().equals(id)).findFirst();
        if (!optional.isPresent()) {
            return null;
        }
        Integer parentId = optional.get().getParentId() == null ? -1 : optional.get().getParentId();
        final List<Category> trees = TreeBuilder.buildTree(categories, parentId);
        return trees.get(0);
    }
}
