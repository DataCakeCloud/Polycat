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

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.model.TransactionContext;
import io.polycat.catalog.store.api.DiscoveryStore;
import io.polycat.catalog.store.common.StoreSqlConvertor;
import io.polycat.catalog.store.gaussdb.common.MetaTableConsts;
import io.polycat.catalog.store.gaussdb.pojo.CategoryRelationRecord;
import io.polycat.catalog.store.gaussdb.pojo.CountByCatalogRecord;
import io.polycat.catalog.store.gaussdb.pojo.CountByCategoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.DiscoveryCategoryRecord;
import io.polycat.catalog.store.gaussdb.pojo.DiscoverySearchRecord;
import io.polycat.catalog.store.gaussdb.pojo.DiscoverySearchWithCategoriesRecord;
import io.polycat.catalog.store.mapper.DiscoveryMapper;
import io.polycat.catalog.store.mapper.ResourceMapper;
import io.polycat.catalog.util.PGDataUtil;
import io.polycat.catalog.util.ResourceUtil;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.discovery.CatalogTableCount;
import io.polycat.catalog.common.model.discovery.Condition;
import io.polycat.catalog.common.model.discovery.DatabaseSearch;
import io.polycat.catalog.common.model.discovery.DatabaseTableCount;
import io.polycat.catalog.common.model.discovery.DiscoverySearchBase;
import io.polycat.catalog.common.model.discovery.DiscoverySearchBase.Fields;
import io.polycat.catalog.common.model.discovery.ObjectCount;
import io.polycat.catalog.common.model.discovery.TableCategories;
import io.polycat.catalog.common.model.discovery.TableSearch;
import io.polycat.catalog.common.model.glossary.Category;
import io.polycat.catalog.common.utils.SegmenterUtils;
import io.polycat.catalog.common.utils.TreeBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "database.type", havingValue = "gauss")
public class DiscoveryStoreImpl implements DiscoveryStore {

    private static final String PG_VECTORIZED_SYMBOL = "@@";

    private static final int SCORE_DIGITS = 7;

    @Autowired
    DiscoveryMapper discoveryMapper;

    @Autowired
    private ResourceMapper resourceMapper;

    @Override
    public void createDiscoverySubspace(TransactionContext context, String projectId) {
        discoveryMapper.createDiscoverySubspace(projectId);
    }

    @Override
    public void dropDiscoverySubspace(TransactionContext context, String projectId) {
        discoveryMapper.dropDiscoverySubspace(projectId);
    }

    @Override
    public void updateDiscoveryInfo(TransactionContext context, String projectId, DiscoverySearchRecord searchRecord) {
        discoveryMapper.upsertDiscoveryInfo(projectId, searchRecord);
    }

    @Override
    public void dropDiscoveryInfo(TransactionContext context, String projectId, String qualifiedName,
                                  ObjectType objectType) {
        discoveryMapper.dropDiscoveryInfo(projectId, qualifiedName, objectType.getNum());
    }

    @Override
    public List<DiscoverySearchBase> searchUsingFullText(
            TransactionContext context, String projectId, String catalogName,
            String objectType, String keywords, String owner, Integer categoryId, String logicalOperator, int batchNum, long batchOffset,List<Condition> conditions) {
        String segmentWords = String.join(getLogicalDelimiter(logicalOperator), getKeywordsSegment(keywords));
        String vectorExpress = getVectorExpress(segmentWords);
        String filterSql = getUsingFullTextFilterSql(catalogName, objectType, owner, vectorExpress, conditions);
        List<DiscoverySearchRecord> searchRecords = discoveryMapper.search(projectId, filterSql, categoryId,
                getRankScoreExpress(StoreSqlConvertor.hump2underline(DiscoverySearchRecord.Fields.tsTokens), vectorExpress, keywords),
                batchOffset, batchNum, isTableObjectType(objectType));
        return toSearchObjectList(searchRecords);
    }

    @Override
    public List<DiscoverySearchBase> searchUsingFullTextWithCategories(
            TransactionContext context, String projectId, String catalogName,
            String objectType, String keywords, String owner, Integer categoryId, String logicalOperator, int batchNum, long batchOffset,List<Condition> conditions) {
        String segmentWords = String.join(getLogicalDelimiter(logicalOperator), getKeywordsSegment(keywords));
        String vectorExpress = getVectorExpress(segmentWords);
        String filterSql = getUsingFullTextFilterSql(catalogName, objectType, owner, vectorExpress, conditions);
        final List<DiscoverySearchWithCategoriesRecord> records = discoveryMapper.searchWithCategories(projectId, filterSql, categoryId,
                getRankScoreExpress(StoreSqlConvertor.hump2underline(DiscoverySearchRecord.Fields.tsTokens), vectorExpress, keywords),
                batchOffset, batchNum, isTableObjectType(objectType));
        return toTableCategories(records);
    }

    private List<DiscoverySearchBase> toTableCategories(List<DiscoverySearchWithCategoriesRecord> records) {
        Map<String, TableCategories> tableSearchListMap = new LinkedHashMap<>();

        records.forEach(record -> {
            if (tableSearchListMap.containsKey(record.getQualifiedName())) {
                final TableCategories tableCategories = tableSearchListMap.get(record.getQualifiedName());
                if (record.getCategoryId() != null) {
                    final Category category = new Category(record.getCategoryId(), record.getCategoryName(), record.getGlossaryId(), record.getParentId());
                    tableCategories.getCategories().add(category);
                }
            } else {
                final List<Category> categories = new ArrayList<>();
                if (record.getCategoryId() != null) {
                    final Category category = new Category(record.getCategoryId(), record.getCategoryName(), record.getGlossaryId(), record.getParentId());
                    categories.add(category);
                }
                //TODO support database
                tableSearchListMap.put(record.getQualifiedName(), new TableCategories((TableSearch) toSearchObject(record), categories));
            }
        });

        return tableSearchListMap.values().stream().peek(tableCategory -> {
            final List<Category> categories = TreeBuilder.buildTree(tableCategory.getCategories(), -1);
            tableCategory.setCategories(categories);
        }).collect(Collectors.toList());
    }

    private String getUsingFullTextFilterSql(String catalogName, String objectType, String owner, String vectorExpress, List<Condition> conditions) {
        return StoreSqlConvertor.get().equals(Fields.catalogName,
                        "".equals(catalogName) ? null : catalogName).AND()
                .equals(Fields.objectType, getObjectTypeNum(objectType)).AND()
                .equals(Fields.owner, owner).AND()
                .specConditions(DiscoverySearchRecord.Fields.tsTokens, PG_VECTORIZED_SYMBOL, vectorExpress)
                .appendPGJsonbFilter(DiscoverySearchRecord.Fields.searchInfo, conditions)
                .getFilterSql();
    }

    @Override
    public List<ObjectCount> countByCategoryUsingFullText(TransactionContext context, String projectId, String catalogName,
                                                                    String objectType, String keywords, String owner, String logicalOperator, Integer categoryId, List<Condition> conditions) {
        String segmentWords = String.join(getLogicalDelimiter(logicalOperator), getKeywordsSegment(keywords));
        String vectorExpress = getVectorExpress(segmentWords);
        String filterSql = getUsingFullTextFilterSql(catalogName, objectType, owner, vectorExpress, conditions);
        return toObjectCount(discoveryMapper.countByCategorySearch(projectId, filterSql, categoryId), objectType);
    }

    private List<ObjectCount> toObjectCount(List<CountByCategoryRecord> countByCategoryRecords, String objectType) {
       return countByCategoryRecords.stream()
               .map(record -> new ObjectCount(record.getCategoryId(), record.getCategoryName(), objectType, record.getParentId(), record.getCount()))
               .collect(Collectors.toList());
    }

    @Override
    public List<CatalogTableCount> countByCatalogUsingFullText(TransactionContext context, String projectId, String catalogName,
                                                               String keywords, String owner, String logicalOperator, List<Condition> conditions) {
        String segmentWords = String.join(getLogicalDelimiter(logicalOperator), getKeywordsSegment(keywords));
        String vectorExpress = getVectorExpress(segmentWords);
        String filterSql = getUsingFullTextFilterSql(catalogName, ObjectType.TABLE.name(), owner, vectorExpress, conditions);
        return toCatalogTableCounts(discoveryMapper.countByCatalogAndDatabaseSearch(projectId, filterSql));
    }

    private List<CatalogTableCount> toCatalogTableCounts(List<CountByCatalogRecord> countByCatalogRecords) {
        Map<String, CatalogTableCount> catalogMap = new LinkedHashMap<>();
        countByCatalogRecords.forEach(record -> {
            if (catalogMap.containsKey(record.getCatalogName())) {
                final CatalogTableCount catalogTableCount = catalogMap.get(record.getCatalogName());
                catalogTableCount.getDatabaseTableCountList().add(new DatabaseTableCount(record.getDatabaseName(), record.getCount()));
            } else {
                final List<DatabaseTableCount> databaseTableCounts = new ArrayList<>();
                databaseTableCounts.add(new DatabaseTableCount(record.getDatabaseName(), record.getCount()));
                catalogMap.put(record.getCatalogName(), new CatalogTableCount(record.getCatalogName(), databaseTableCounts));
            }
        });
        return new ArrayList<>(catalogMap.values());
    }

    private List<String> getKeywordsSegment(String keywords) {
        List<String> segment = SegmenterUtils.hanLPSegment(keywords);
        if (!SegmenterUtils.needSegment(keywords)) {
            segment.addAll(SegmenterUtils.simpleSegment(keywords, "\\."));
        }
        return segment;
    }

    private boolean isTableObjectType(String objectType) {
        return "TABLE".equalsIgnoreCase(objectType);
    }

    @Override
    public List<DiscoverySearchBase> searchSimpleLike(TransactionContext context, String projectId,
                                                                              String catalogName, String objectType, String keywords, String owner, Integer categoryId, int batchNum,
                                                                              long batchOffset, List<Condition> conditions) {
        String filterSql = getSimpleLikeFilterSql(catalogName, objectType, owner, keywords, conditions);
        List<DiscoverySearchRecord> searchRecords = discoveryMapper.search(projectId, filterSql, categoryId, getSimilarityRankScoreExpress(StoreSqlConvertor.hump2underline(DiscoverySearchRecord.Fields.keywords), keywords), batchOffset, batchNum, isTableObjectType(objectType));
        return toSearchObjectList(searchRecords);

    }

    @Override
    public List<DiscoverySearchBase> searchSimpleLikeWithCategories(TransactionContext context, String projectId,
                                                                              String catalogName, String objectType, String keywords, String owner, Integer categoryId, int batchNum,
                                                                              long batchOffset, List<Condition> conditions) {
        String filterSql = getSimpleLikeFilterSql(catalogName, objectType, owner, keywords, conditions);
        final List<DiscoverySearchWithCategoriesRecord> records = discoveryMapper.searchWithCategories(projectId, filterSql, categoryId, getSimilarityRankScoreExpress(StoreSqlConvertor.hump2underline(DiscoverySearchRecord.Fields.keywords), keywords), batchOffset, batchNum, isTableObjectType(objectType));
        return toTableCategories(records);
    }

    private String getSimpleLikeFilterSql(String catalogName, String objectType, String owner, String keywords, List<Condition> conditions) {
        return StoreSqlConvertor.get().equals(Fields.catalogName,
                        "".equals(catalogName) ? null : catalogName).AND()
                .equals(Fields.objectType, getObjectTypeNum(objectType)).AND()
                .equals(Fields.owner, owner).AND()
                .like(DiscoverySearchRecord.Fields.keywords, keywords)
                .appendPGJsonbFilter(DiscoverySearchRecord.Fields.searchInfo, conditions)
                .getFilterSql();
    }

    @Override
    public List<ObjectCount> countByCategorySimpleLike(TransactionContext context, String projectId,
                                                                 String catalogName, String objectType, String keywords, String owner, Integer categoryId,
                                                                 List<Condition> conditions) {
        String filterSql = getSimpleLikeFilterSql(catalogName, objectType, owner, keywords, conditions);
        return toObjectCount(discoveryMapper.countByCategorySearch(projectId, filterSql, categoryId), objectType);
    }

    @Override
    public List<CatalogTableCount> countByCatalogSimpleLike(TransactionContext context, String projectId, String catalogName,
                                                            String keywords, String owner, String logicalOperator, List<Condition> conditions) {
        String filterSql = getSimpleLikeFilterSql(catalogName, ObjectType.TABLE.name(), owner, keywords, conditions);
        return toCatalogTableCounts(discoveryMapper.countByCatalogAndDatabaseSearch(projectId, filterSql));
    }

    private <T> ScanRecordCursorResult<List<T>> getScanCursorResult(List<T> result, long batchOffset, int batchNum) {
        if (result.size() < batchNum) {
            return new ScanRecordCursorResult<List<T>>(result, 0);
        } else {
            return new ScanRecordCursorResult<List<T>>(result, batchOffset + batchNum);
        }
    }

    @Override
    public List<String> searchSimpleLikeNames(TransactionContext context, String projectId, String catalogName, String objectType, String keyword, String owner, Integer categoryId, int batchNum, long batchOffset,  List<Condition> conditions) {
        String filterSql = StoreSqlConvertor.get().equals(Fields.catalogName,
                        "".equals(catalogName) ? null : catalogName).AND()
                .equals(Fields.objectType, getObjectTypeNum(objectType)).AND()
                .equals(Fields.owner, owner).AND()
                .like(DiscoverySearchRecord.Fields.keywords, keyword)
                .appendPGJsonbFilter(DiscoverySearchRecord.Fields.searchInfo, conditions)
                .getFilterSql();
        return discoveryMapper.searchSimpleLikeNames(projectId, filterSql, batchOffset, batchNum);
    }

    private Integer getObjectTypeNum(String objectType) {
        if (StringUtils.isBlank(objectType)) {
            return null;
        }
        try {
            return ObjectType.valueOf(objectType).getNum();
        } catch (Exception e) {
            throw new MetaStoreException(ErrorCode.ARGUMENT_ILLEGAL, e.getMessage());
        }
    }

    private List<DiscoverySearchBase> toSearchObjectList(List<DiscoverySearchRecord> searchRecords) {
        List<DiscoverySearchBase> result = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(searchRecords)) {
            for (DiscoverySearchRecord dsr : searchRecords) {
                result.add(toSearchObject(dsr));
            }
        }
        return result;
    }

    private DiscoverySearchBase toSearchObject(DiscoverySearchRecord searchRecord) {
        DiscoverySearchBase searchObject = null;
        ObjectType objectType = ObjectType.forNum(searchRecord.getObjectType());
        switch (objectType) {
            case TABLE:
                searchObject = toTableSearchRecord(searchRecord);
                break;
            case DATABASE:
                searchObject =  toDatabaseSearchRecord(searchRecord);
                break;
            default:
                break;
        }
        searchObject.setScore(searchRecord.getScore());
        return searchObject;
    }

    private TableSearch toTableSearchRecord(DiscoverySearchRecord dsr) {
        TableSearch tableSearch = PGDataUtil.getJsonBean(dsr.getSearchInfo(), TableSearch.class);
        tableSearch.setRecentVisitCount(dsr.getRecentVisitCount());
        tableSearch.setLastAccessTime(dsr.getLastAccessTime());
        return tableSearch;
    }

    private DatabaseSearch toDatabaseSearchRecord(DiscoverySearchRecord dsr) {
        final DatabaseSearch databaseSearch = PGDataUtil.getJsonBean(dsr.getSearchInfo(), DatabaseSearch.class);
        return databaseSearch;
    }

    private String getVectorExpress(String segmentWords) {
        if (StringUtils.isNotEmpty(segmentWords)) {
            return "to_tsquery('simple', '" + handlePointToTsQuery(segmentWords) + "')";
        }
        return segmentWords;
    }

    /**
     * "." is a stopword query and it is invalid to use it directly with tsquery.
     *
     * @param segmentWords
     * @return
     */
    private String handlePointToTsQuery(String segmentWords) {
        return segmentWords.replaceAll("\\.", "\\\\\\\\.");
    }

    private String getSimilarityRankScoreExpress(String likeFieldName, String keywords) {
        if (StringUtils.isNotEmpty(keywords)) {
            return "round(length('" + keywords + "') * 1.0/length(" + likeFieldName + "), 7)";
        }
        return "0";
    }

    private String getRankScoreExpress(String tsVectorField, String segmentWords, String keywords) {
        if (keywords != null && keywords.trim().contains(".") && keywords.trim().length() > 2) {
            String fieldName = StoreSqlConvertor.hump2underline(DiscoverySearchRecord.Fields.qualifiedName);
            if (StringUtils.isNotEmpty(segmentWords)) {
                return "CASE WHEN qualified_name ILIKE '%" + keywords.trim() + "%' THEN " + getSimilarityRankScoreExpress(fieldName, keywords.trim()) + " + " + getTsRankScoreExpress(tsVectorField, segmentWords) + " ELSE " + getTsRankScoreExpress(tsVectorField, segmentWords) + " END";
            }
        }
        if (StringUtils.isNotEmpty(segmentWords)) {
            return "ts_rank(" + tsVectorField + ", " + segmentWords + ")";
        }
        return "0";
    }

    private String getTsRankScoreExpress(String tsVectorField, String segmentWords) {
        if (StringUtils.isNotEmpty(segmentWords)) {
            return "ts_rank(" + tsVectorField + ", " + segmentWords + ")";
        }
        return "0";
    }

    private CharSequence getLogicalDelimiter(String logicalOperator) {
        if ("AND".equalsIgnoreCase(logicalOperator)) {
            return "&";
        } else if ("OR".equalsIgnoreCase(logicalOperator)) {
            return "|";
        }
        throw new CatalogServerException(ErrorCode.ARGUMENT_ILLEGAL, "logicalOperator values is [AND|OR]");
    }

    @Override
    public void addCategoryRelation(TransactionContext context, String projectId, String qualifiedName, List<Integer> categoryIds) {
        if (categoryIds != null && !categoryIds.isEmpty()) {
            discoveryMapper.addCategoryRelation(projectId, qualifiedName, categoryIds);
        }
    }

    @Override
    public void removeCategoryRelation(TransactionContext context, String projectId, String qualifiedName, List<Integer> categoryIds) {
        if (categoryIds != null && !categoryIds.isEmpty()) {
            discoveryMapper.removeCategoryRelation(projectId, qualifiedName, categoryIds);
        }

    }

    @Override
    public DiscoverySearchRecord getDiscoveryInfoByQualifiedName(TransactionContext context, String projectId, String qualifiedName) {
        return discoveryMapper.getDiscoveryInfoByQualifiedName(projectId, qualifiedName);
    }

    @Override
    public List<CategoryRelationRecord> getCategoryRelationByQualifiedName(TransactionContext context, String projectId, String qualifiedName) {
        return discoveryMapper.getCategoryRelationByQualifiedName(projectId, qualifiedName);
    }

    @Override
    public List<DiscoveryCategoryRecord> getDiscoveryCategories(TransactionContext context, String projectId, String qualifiedName) {
        return discoveryMapper.getDiscoveryCategories(projectId, qualifiedName);
    }

    @Override
    public void createSubspace(TransactionContext context, String projectId) {
        if (!resourceMapper.doesExistTable(ResourceUtil.getSchema(projectId), MetaTableConsts.DISCOVERY_INFO)) {
            discoveryMapper.createDiscoverySubspace(projectId);
        }
        if (!resourceMapper.doesExistTable(ResourceUtil.getSchema(projectId), MetaTableConsts.DISCOVERY_CATEGORY)) {
            discoveryMapper.createCategoryRelationSubspace(projectId);
        }
    }
}
