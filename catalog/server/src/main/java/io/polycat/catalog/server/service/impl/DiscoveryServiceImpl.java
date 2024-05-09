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
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.DatabaseIdent;
import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.DatabaseObject;
import io.polycat.catalog.common.model.TableIdent;
import io.polycat.catalog.common.model.TableName;
import io.polycat.catalog.common.model.TableObject;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.model.discovery.CatalogTableCount;
import io.polycat.catalog.common.model.discovery.Condition;
import io.polycat.catalog.common.model.discovery.DatabaseSearch;
import io.polycat.catalog.common.model.discovery.DatabaseTableCount;
import io.polycat.catalog.common.model.discovery.DiscoverySearchBase;
import io.polycat.catalog.common.model.discovery.ObjectCount;
import io.polycat.catalog.common.model.discovery.TableCategories;
import io.polycat.catalog.common.model.glossary.Category;
import io.polycat.catalog.common.utils.ScanCursorUtils;
import io.polycat.catalog.common.utils.TreeBuilder;
import io.polycat.catalog.service.api.CatalogService;
import io.polycat.catalog.service.api.DatabaseService;
import io.polycat.catalog.service.api.DiscoveryService;
import io.polycat.catalog.service.api.TableService;
import io.polycat.catalog.store.api.DatabaseStore;
import io.polycat.catalog.store.api.DiscoveryStore;
import io.polycat.catalog.store.api.GlossaryStore;
import io.polycat.catalog.store.api.TableMetaStore;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.gaussdb.pojo.CategoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.CategoryRelationRecord;
import io.polycat.catalog.store.gaussdb.pojo.DiscoveryCategoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.DiscoverySearchRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * DiscoveryService implementation
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "metastore.type", havingValue = "polyCat")
public class DiscoveryServiceImpl implements DiscoveryService {

    private final String discoveryCheckSumStore = "discoveryCheckSumStore";

    private final int maxBatchRowNum = 2000;

    @Autowired
    private DiscoveryStore discoveryStore;

    @Autowired
    private GlossaryStore glossaryStore;

    @Autowired
    private CatalogService catalogService;

    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private TableService tableService;

    @Autowired
    private TableMetaStore tableMetaStore;

    @Autowired
    private DatabaseStore databaseStore;


    /**
     * Full Text Search
     *
     * @param projectId
     * @param catalogName
     * @param objectType
     * @param keywords
     * @param logicalOperator
     * @param exactMatch
     * @param limit
     * @param pageToken
     * @param conditions
     * @param withCategories
     * @return
     */
    @Override
    public TraverseCursorResult<List<DiscoverySearchBase>> searchUsingFullText(String projectId, String catalogName,
                                                                               String objectType, String keywords, String owner, Integer categoryId,
                                                                               String logicalOperator, boolean exactMatch, boolean withCategories, Integer limit,
                                                                               String pageToken, List<Condition> conditions) {
        checkPageLimitNumberic(limit);
        return ScanCursorUtils.scanMultiStrategy("", discoveryCheckSumStore, pageToken,
                getCurrentMethodName(), limit, maxBatchRowNum, (batchNum, batchOffset) -> TransactionRunnerUtil.transactionRunThrow(
                        context -> {
                            // priority strategy
                            return searchUsingFullTextInternal(context, projectId, catalogName, objectType,
                                    keywords, owner, categoryId, logicalOperator, exactMatch, withCategories, batchNum, batchOffset, conditions);
                        }).getResult(), (batchNum, batchOffset) -> TransactionRunnerUtil.transactionRunThrow(
                        context -> {
                            // base strategy
                            return searchSimpleLikeInternal(context, projectId, catalogName, objectType,
                                    keywords, owner, categoryId, withCategories, batchNum, batchOffset, conditions);
                        }).getResult());
    }

    private void checkPageLimitNumberic(Integer limit) {
        if (limit < 0 && limit != -1) {
            throw new CatalogServerException(ErrorCode.ARGUMENT_ILLEGAL, " limit=[" + limit + "]");
        }
    }

    /**
     * Initialize discovery record data, currently only supported ObjectType=Table
     *
     * @param projectId
     * @param objectType
     */
    @Override
    public void initDiscovery(String projectId, String objectType) {
        long start = System.currentTimeMillis();
        int count = 0;
        // get all catalogs
        List<Catalog> catalogs = catalogService.getCatalogs(projectId, Integer.MAX_VALUE, null, "*").getResult();
        if (CollectionUtils.isNotEmpty(catalogs)) {
            List<Database> databases;
            for (Catalog catalog : catalogs) {
                databases = databaseService.listDatabases(StoreConvertor.catalogName(projectId, catalog.getCatalogName()),
                        false, Integer.MAX_VALUE, null, null).getResult();
                String[] tables;
                for (Database database : databases) {
                    switch (ObjectType.valueOf(objectType)) {
                        case  DATABASE:
                            final DatabaseName databaseName = StoreConvertor.databaseName(projectId, catalog.getCatalogName(), database.getDatabaseName());
                            initDatabaseDiscoveryInternal(databaseName);
                            count += 1;
                            break;
                        case TABLE:
                            tables = tableService.listTableNames(StoreConvertor.databaseName(projectId, catalog.getCatalogName(), database.getDatabaseName()),
                                    false, Integer.MAX_VALUE, null, null).getObjects();
                            for (String tableName : tables) {
                                TableName tableNameParm = StoreConvertor.tableName(projectId, catalog.getCatalogName(), database.getDatabaseName(), tableName);
                                initTableDiscoveryInternal(tableNameParm);
                                count += 1;
                            }
                    }
                }

            }
        }
        long end = System.currentTimeMillis();
        log.info("Init discovery time cost: {} ms, init count: {}.", end - start, count);
    }

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
    @Override
    public TraverseCursorResult<List<String>> matchListNames(String projectId, String catalogName, String objectType, String keyword,
                                                             String owner, Integer categoryId, Integer limit, String pageToken, List<Condition> conditions) {
        checkPageLimitNumberic(limit);
        return ScanCursorUtils.scan("", discoveryCheckSumStore, pageToken,
                getCurrentMethodName(), limit, maxBatchRowNum, (batchNum, batchOffset) -> TransactionRunnerUtil.transactionRunThrow(
                        context -> {
                            return matchListNamesInternal(context, projectId, catalogName, objectType,
                                    keyword, owner, categoryId, batchNum, batchOffset, conditions);
                        }).getResult());
    }

    private List<String> matchListNamesInternal(TransactionContext context, String projectId, String catalogName,
                                                                        String objectType, String keyword, String owner, Integer categoryId, int batchNum, long batchOffset, List<Condition> conditions) {
        return discoveryStore.searchSimpleLikeNames(context, projectId, catalogName, objectType, keyword, owner, categoryId, batchNum, batchOffset, conditions);
    }

    private void initTableDiscoveryInternal(TableName tableName) {
        TransactionRunnerUtil.transactionRun(
                context -> {
                    try {
                        TableIdent tableIdent = TableObjectHelper.getTableIdent(context, tableName);
                        TableObject tableObject = tableMetaStore.getTable(context, tableIdent, tableName);
                        DiscoveryHelper.updateTableDiscoveryInfo(context, tableName, tableObject.getTableBaseObject(), tableObject.getTableSchemaObject(), tableObject.getTableStorageObject());
                    } catch (Exception e) {
                        log.warn("Discovery init record: {}, cause: {}.", tableName.getQualifiedName(), e.getMessage());
                    }
                    return null;
                });
    }

    private void initDatabaseDiscoveryInternal(DatabaseName databaseName) {
        TransactionRunnerUtil.transactionRun(
                context -> {
                    try {
                        final DatabaseIdent databaseIdent = DatabaseObjectHelper.getDatabaseIdent(context, databaseName);
                        final DatabaseObject databaseObject = databaseStore.getDatabase(context, databaseIdent);
                        DiscoveryHelper.updateDatabaseDiscoveryInfo(context, databaseName, databaseObject);
                    } catch (Exception e) {
                        log.warn("Discovery init record: {}, cause: {}.", databaseName.getQualifiedName(), e.getMessage());
                    }
                    return null;
                }
        );
    }

    private List<DiscoverySearchBase> searchSimpleLikeInternal(TransactionContext context, String projectId, String catalogName, String objectType,
                                                                                       String keywords, String owner, Integer categoryId, boolean withCategories, int batchNum,
                                                                                       long batchOffset, List<Condition> conditions) {
        if (withCategories) {
            return discoveryStore.searchSimpleLikeWithCategories(context, projectId, catalogName, objectType, keywords, owner, categoryId, batchNum, batchOffset, conditions);
        } else {
            return discoveryStore.searchSimpleLike(context, projectId, catalogName, objectType, keywords, owner, categoryId, batchNum, batchOffset, conditions);
        }
    }

    private List<DiscoverySearchBase> searchUsingFullTextInternal(TransactionContext context,
                                                                                          String projectId, String catalogName,
                                                                                          String objectType, String keywords, String owner, Integer categoryId,
                                                                                          String logicalOperator, boolean exactMatch, boolean withCategories, int batchNum,
                                                                                          long batchOffset, List<Condition> conditions) {
        List<DiscoverySearchBase> result = null;
        if (!exactMatch) {
            if (withCategories) {
                result = discoveryStore.searchUsingFullTextWithCategories(context, projectId, catalogName, objectType, keywords, owner, categoryId, logicalOperator, batchNum, batchOffset, conditions);
            } else {
                result = discoveryStore.searchUsingFullText(context, projectId, catalogName, objectType, keywords, owner, categoryId, logicalOperator, batchNum, batchOffset, conditions);
            }
        } else {
            if (withCategories) {
                result = discoveryStore.searchSimpleLikeWithCategories(context, projectId, catalogName, objectType, keywords, owner, categoryId, batchNum, batchOffset, conditions);
            } else {
                result = discoveryStore.searchSimpleLike(context, projectId, catalogName, objectType, keywords, owner, categoryId, batchNum, batchOffset, conditions);
            }
        }

        return result;
    }
    private String getCurrentMethodName() {
        return Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    @Override
    public void addCategoryRelation(String projectId, String qualifiedName, Integer categoryId) {

        TransactionRunnerUtil.transactionRunThrow(context -> {
            final DiscoverySearchRecord discoveryInfo = discoveryStore.getDiscoveryInfoByQualifiedName(context, projectId, qualifiedName);
            if (discoveryInfo == null) {
                throw new CatalogServerException(ErrorCode.TABLE_NOT_FOUND, qualifiedName);
            }
            final List<CategoryRecord> categoryRecords = glossaryStore.getCategoryRecordAndParents(context, projectId, categoryId, false);
            if (categoryRecords.isEmpty()) {
                throw new CatalogServerException("category is not exists", ErrorCode.ARGUMENT_ILLEGAL);
            }
            final List<CategoryRelationRecord> categoryRelations = discoveryStore.getCategoryRelationByQualifiedName(context, projectId, qualifiedName);
            final List<Integer> categoryIds = categoryRecords.stream().filter(record -> {
                for (CategoryRelationRecord categoryRelationRecord : categoryRelations) {
                    if (categoryRelationRecord.getCategoryId().equals(record.getId())) {
                        return false;
                    }
                }
                return true;
            }).map(CategoryRecord::getId).collect(Collectors.toList());
            discoveryStore.addCategoryRelation(context, projectId, qualifiedName, categoryIds);
            return null;
        });
    }

    @Override
    public void removeCategoryRelation(String projectId, String qualifiedName, Integer categoryId) {
        TransactionRunnerUtil.transactionRunThrow(context -> {
            final List<Integer> categoryIds = glossaryStore.getCategoryRecordAndParents(context, projectId, categoryId, true).stream()
                    .map(CategoryRecord::getId).collect(Collectors.toList());
            discoveryStore.removeCategoryRelation(context, projectId, qualifiedName, categoryIds);
            return null;
        });
    }

    @Override
    public TableCategories getTableCategories(String projectId, String qualifiedName) {
        final List<DiscoveryCategoryRecord> discoveryCategories = discoveryStore.getDiscoveryCategories(null, projectId, qualifiedName);
        final List<Category> categories = discoveryCategories.stream()
                .map(record -> new Category(record.getCategoryId(), record.getCategoryName(), record.getDescription(), record.getParentId(), record.getGlossaryId()))
                .collect(Collectors.toList());
        return new TableCategories(qualifiedName,  TreeBuilder.buildTree(categories, -1));
    }

    @Override
    public ObjectCount getObjectCountByCategory(String projectId, String catalogName, String objectType,
                                                String keywords, String owner, String logicalOperator, boolean exactMatch, Integer categoryId, List<Condition> conditions) {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            List<ObjectCount> objectCounts = discoveryStore.countByCategoryUsingFullText(context, projectId, catalogName, objectType, keywords, owner, logicalOperator, categoryId, conditions);
            if (objectCounts.isEmpty()) {
                objectCounts = discoveryStore.countByCategorySimpleLike(context, projectId, catalogName, objectType, keywords, owner, categoryId, conditions);
            }

            Map<Integer, ObjectCount> objectCountMap = new LinkedHashMap<>();
            glossaryStore.getCategoryRecordAndChildren(context, projectId, categoryId).forEach(category ->
                    objectCountMap.put(category.getId(), new ObjectCount(category.getId(), category.getName(), objectType, category.getParentId(), 0)));

            objectCounts.forEach(objectCount -> objectCountMap.put(objectCount.getId(), objectCount));
            final Optional<ObjectCount> optional = objectCountMap.values().stream().filter(record -> record.getId().equals(categoryId)).findFirst();
            Integer parentId = optional.isPresent() ? optional.get().getParentId() : -1;
            final List<ObjectCount> trees = TreeBuilder.buildTree(new ArrayList<>(objectCountMap.values()), parentId);
            return trees.get(0);
        }).getResult();
    }

    @Override
    public List<CatalogTableCount> getTableCountByCatalog(String projectId, String catalogName,
                                                    String keywords, String owner, String logicalOperator, boolean exactMatch, List<Condition> conditions) {
        return TransactionRunnerUtil.transactionRunThrow(context -> {
            List<CatalogTableCount> catalogTableCounts = discoveryStore.countByCatalogUsingFullText(context, projectId, catalogName, keywords, owner, logicalOperator, conditions);
            if (catalogTableCounts.isEmpty()) {
                catalogTableCounts = discoveryStore.countByCatalogSimpleLike(context, projectId, catalogName, keywords, owner, logicalOperator, conditions);
            }

            final List<DiscoverySearchBase> allDatabases = discoveryStore.searchSimpleLike(context, projectId, catalogName, ObjectType.DATABASE.name(), null, null, null, Integer.MAX_VALUE, 0, null);
            Map<String, Map<String, DatabaseTableCount>> catalogDatabaseTableCountMap = new LinkedHashMap<>();
            catalogTableCounts.forEach(catalogTableCount -> {
                final String c = catalogTableCount.getCatalogName();
                catalogTableCount.getDatabaseTableCountList().forEach(databaseTableCount -> {
                    if (catalogDatabaseTableCountMap.containsKey(c)) {
                        final Map<String, DatabaseTableCount> databaseTableCountMap = catalogDatabaseTableCountMap.get(c);
                        databaseTableCountMap.put(databaseTableCount.getDatabaseName(), databaseTableCount);
                    } else {
                        Map<String, DatabaseTableCount> databaseTableCountMap = new LinkedHashMap<>();
                        databaseTableCountMap.put(databaseTableCount.getDatabaseName(), databaseTableCount);
                        catalogDatabaseTableCountMap.put(c, databaseTableCountMap);
                    }
                });
            });
            allDatabases.forEach(base -> {
                final DatabaseSearch databaseSearch = (DatabaseSearch) base;
                final String c = databaseSearch.getCatalogName();
                if (catalogDatabaseTableCountMap.containsKey(c)) {
                    final Map<String, DatabaseTableCount> databaseTableCountMap = catalogDatabaseTableCountMap.get(c);
                    if (!databaseTableCountMap.containsKey(databaseSearch.getDatabaseName())) {
                        databaseTableCountMap.put(databaseSearch.getDatabaseName(), new DatabaseTableCount(databaseSearch.getDatabaseName(), 0));
                    }
                } else {
                    Map<String, DatabaseTableCount> databaseTableCountMap = new LinkedHashMap<>();
                    databaseTableCountMap.put(databaseSearch.getDatabaseName(), new DatabaseTableCount(databaseSearch.getDatabaseName(), 0));
                    catalogDatabaseTableCountMap.put(c, databaseTableCountMap);
                }
            });

            return catalogDatabaseTableCountMap.entrySet().stream()
                    .map(entry -> {
                        final List<DatabaseTableCount> databaseTableCounts = entry.getValue().values().stream()
                                .sorted(Comparator.comparing(DatabaseTableCount::getDatabaseName)).collect(Collectors.toList());
                        return new CatalogTableCount(entry.getKey(), databaseTableCounts);
                    }).collect(Collectors.toList());
        }).getResult();
    }
}
