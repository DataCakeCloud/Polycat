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
package io.polycat.catalog.common.plugin.request.base;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
@ApiModel(description = "Base page list request")
public class PageListRequestBase<T> extends ProjectRequestBase<T> {

    @ApiModelProperty(value = "maxResults", required = false)
    protected Integer maxResults = 100;
    @ApiModelProperty(value = "pageToken", required = false)
    protected String pageToken;

    public PageListRequestBase() {
    }

    public PageListRequestBase(String projectId, T input) {
        super(projectId, input);
    }

    public PageListRequestBase(String projectId, T input, Integer maxResults, String pageToken) {
        super(projectId, input);
        checkMaxResultParam(maxResults);
        this.maxResults = maxResults;
        this.pageToken = pageToken;
    }

    private void checkMaxResultParam(Integer maxResults) {
        if (maxResults <= 0) {
            throw new IllegalArgumentException("param maxResults must greater than 0");
        }
    }

    public void setMaxResults(Integer maxResults) {
        checkMaxResultParam(maxResults);
        this.maxResults = maxResults;
    }
}
