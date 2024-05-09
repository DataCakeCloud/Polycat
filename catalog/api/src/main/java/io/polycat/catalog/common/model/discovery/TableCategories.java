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
package io.polycat.catalog.common.model.discovery;

import io.polycat.catalog.common.model.glossary.Category;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author liangyouze
 * @date 2023/12/13
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class TableCategories extends TableSearch{
    private List<Category> categories;

    public TableCategories(String qualifiedName, List<Category> categories) {
        this.setQualifiedName(qualifiedName);
        this.categories = categories;
    }

    public TableCategories(TableSearch tableSearch,  List<Category> categories) {
        this.setId(tableSearch.getId());
        this.setCatalogName(tableSearch.getCatalogName());
        this.setQualifiedName(tableSearch.getQualifiedName());
        this.setObjectType(tableSearch.getObjectType());
        this.setDescription(tableSearch.getDescription());
        this.setOwner(tableSearch.getOwner());
        this.setCreateTime(tableSearch.getCreateTime());
        this.setScore(tableSearch.getScore());
        this.setDatabaseName(tableSearch.getDatabaseName());
        this.setTableName(tableSearch.getTableName());
        this.setLocation(tableSearch.getLocation());
        this.setSdFileFormat(tableSearch.getSdFileFormat());
        this.setPartitionKeys(tableSearch.getPartitionKeys());
        this.setRecentVisitCount(tableSearch.getRecentVisitCount());
        this.setLastAccessTime(tableSearch.getLastAccessTime());
        this.setTransientLastDdlTime(tableSearch.getTransientLastDdlTime());
        this.categories = categories;
    }

}
