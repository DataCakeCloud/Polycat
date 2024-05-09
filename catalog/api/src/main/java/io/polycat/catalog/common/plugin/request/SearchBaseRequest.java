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
package io.polycat.catalog.common.plugin.request;

import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.plugin.request.base.ProjectRequestBase;
import io.polycat.catalog.common.plugin.request.input.FilterConditionInput;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@EqualsAndHashCode(callSuper = true)
@Setter
@Getter
@NoArgsConstructor
public class SearchBaseRequest extends ProjectRequestBase<FilterConditionInput> {

    /**
     * Keyword space logical connector.
     */
    public enum LogicalOperator {
        /**
         * AND
         */
        AND, OR
    }

    protected String catalogName;

    @Setter(AccessLevel.NONE)
    private ObjectType objectType;

    protected String keyword;

    protected String owner;

    protected Integer categoryId;

    protected LogicalOperator logicalOperator = LogicalOperator.AND;

    protected boolean exactMatch = false;

    protected boolean withCategories = false;

    protected String pageToken;

    protected int limit = 100;

    public SearchBaseRequest(String projectId, String keyword, String catalogName, ObjectType objectType) {
        this.projectId = projectId;
        this.catalogName = catalogName;
        this.objectType = objectType;
        this.keyword = keyword;
    }

    public SearchBaseRequest(String projectId, String keyword, String catalogName, ObjectType objectType, FilterConditionInput input) {
        this.projectId = projectId;
        this.catalogName = catalogName;
        this.objectType = objectType;
        this.keyword = keyword;
        this.input = input;
    }

    public SearchBaseRequest(String projectId, String keyword, String catalogName, Integer categoryId, ObjectType objectType, FilterConditionInput input) {
        this.projectId = projectId;
        this.catalogName = catalogName;
        this.categoryId = categoryId;
        this.objectType = objectType;
        this.keyword = keyword;
        this.input = input;
    }
}
