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
package io.polycat.catalog.store.gaussdb;

import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.GlossaryStore;
import io.polycat.catalog.store.gaussdb.common.MetaTableConsts;
import io.polycat.catalog.store.gaussdb.pojo.CategoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.GlossaryRecord;
import io.polycat.catalog.store.mapper.GlossaryMapper;
import io.polycat.catalog.store.mapper.ResourceMapper;
import io.polycat.catalog.util.ResourceUtil;
import io.polycat.catalog.common.model.glossary.Category;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author liangyouze
 * @date 2023/12/4
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class GlossaryStoreImpl implements GlossaryStore {

    @Autowired
    GlossaryMapper glossaryMapper;

    @Autowired
    ResourceMapper resourceMapper;

    @Override
    public GlossaryRecord insertGlossary(TransactionContext context, String projectId, GlossaryRecord glossaryRecord) {
        glossaryMapper.insertGlossary(projectId, glossaryRecord);
        return glossaryRecord;
    }

    @Override
    public void deleteGlossary(TransactionContext context, String projectId, Integer id) {
        glossaryMapper.deleteGlossary(projectId, id);
    }

    @Override
    public void updateGlossary(TransactionContext context, String projectId, Integer id, String name, String description) {
        glossaryMapper.updateGlossary(projectId, id, name, description);
    }

    @Override
    public List<GlossaryRecord> listGlossaryWithoutCategory(TransactionContext context, String projectId) {
        return glossaryMapper.listGlossaryWithoutCategory(projectId);
    }

    @Override
    public CategoryRecord insertCategory(TransactionContext context, String projectId, CategoryRecord categoryRecord) {
        glossaryMapper.insertCategory(projectId, categoryRecord);
        return categoryRecord;
    }

    @Override
    public void updateCategory(TransactionContext context, String projectId, CategoryRecord categoryRecord) {
        glossaryMapper.updateCategory(projectId, categoryRecord);
    }

    @Override
    public void deleteCategory(TransactionContext context, String projectId, Integer id) {
        glossaryMapper.deleteCategory(projectId, id);
    }

    @Override
    public CategoryRecord getCategoryById(TransactionContext context, String projectId, Integer id) {
        return glossaryMapper.getCategoryById(projectId, id);
    }

    @Override
    public GlossaryRecord getGlossaryById(TransactionContext context, String projectId, Integer id) {
        return glossaryMapper.getGlossaryById(projectId, id);
    }

    @Override
    public List<CategoryRecord> listCategoryByGlossary(TransactionContext context, String projectId, Integer glossaryId, String glossaryName) {
        return glossaryMapper.listCategoryByGlossary(projectId, glossaryId, glossaryName);
    }

    @Override
    public List<Category> getCategoryRecordAndChildren(TransactionContext context, String projectId, Integer id) {
        final List<CategoryRecord> categoryRecords = glossaryMapper.getCategoryRecordAndChildren(projectId, id);
        return categoryRecords.stream()
                .map(categoryRecord -> new Category(categoryRecord.getId(), categoryRecord.getName(),
                        categoryRecord.getDescription(), categoryRecord.getParentId(), categoryRecord.getCreateTime().getTime(),
                        categoryRecord.getUpdateTime().getTime())).collect(Collectors.toList());
    }

    @Override
    public List<CategoryRecord> getCategoryRecordAndParents(TransactionContext context, String projectId, Integer id, Boolean ignoreDelete) {
        return glossaryMapper.getCategoryRecordAndParents(projectId, id, ignoreDelete);
    }

    @Override
    public GlossaryRecord getGlossaryRecordWithCategories(TransactionContext context, String projectId, Integer glossaryId, String glossaryName) {
        return glossaryMapper.getGlossaryRecordWithCategories(projectId, glossaryId, glossaryName);
    }


    @Override
    public void createSubspace(TransactionContext context, String projectId) {
        if (!resourceMapper.doesExistTable(ResourceUtil.getSchema(projectId), MetaTableConsts.GLOSSARY)) {
            glossaryMapper.createGlossarySubspace(projectId);
        }
        if (!resourceMapper.doesExistTable(ResourceUtil.getSchema(projectId), MetaTableConsts.CATEGORY)) {
            glossaryMapper.createCategorySubspace(projectId);
        }

    }


}
