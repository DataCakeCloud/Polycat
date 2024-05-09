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
package io.polycat.catalog.common.model.glossary;

import io.polycat.catalog.common.model.base.TreeBase;
import io.swagger.annotations.ApiModel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * @author liangyouze
 * @date 2023/12/4
 */
@EqualsAndHashCode(callSuper = true)
@ApiModel(description = "category")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Category extends TreeBase<Category, Integer> {
    private String name;
    private String description;
    private Integer glossaryId;
    private Long createTime;
    private Long updateTime;

    public Category(Integer id, String name, String description, Integer parentId, Long createTime, Long updateTime) {
        this.setId(id);
        this.name = name;
        this.description = description;
        this.setParentId(parentId == null ? -1 : parentId);
        this.createTime = createTime;
        this.updateTime = updateTime;
    }

    public Category(Integer id, String name, String description, Integer parentId, Integer glossaryId) {
        this.setId(id);
        this.name = name;
        this.description = description;
        this.setParentId(parentId == null ? -1 : parentId);
        this.glossaryId = glossaryId;
    }

    public Category(Integer id, String name, Integer glossaryId, Integer parentId) {
        this.setId(id);
        this.name = name;
        this.glossaryId = glossaryId;
        this.setParentId(parentId == null ? -1 : parentId);
    }
}
