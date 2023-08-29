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
package io.polycat.catalog.common.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.store.protos.common.TableBaseInfo;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;


@Data
public class TableBaseObject {
    private long createTime;
    private String authSourceType = "";
    private String accountId = "";
    private String ownerType = "";
    private String owner = "";
    private String description = "";
    private Integer retention = 0;
    private String tableType = "";
    private Map<String, String> parameters = Collections.emptyMap();
    private String viewOriginalText = "";
    private String viewExpandedText = "";

    //todo: after delete commit_partition
    private boolean lmsMvcc;

    public TableBaseObject() {
        this.parameters = new LinkedHashMap<>();
    }

    public TableBaseObject(Map<String, String> parameters) {
        this.parameters = new LinkedHashMap<>();
        this.parameters.putAll(parameters);
    }

    public TableBaseObject(TableInput tableInput) {
        if (tableInput.getCreateTime() == null || tableInput.getCreateTime() <= 0) {
            this.createTime = System.currentTimeMillis();
        } else {
            this.createTime = tableInput.getCreateTime();
        }

        if (StringUtils.isNotBlank(tableInput.getAuthSourceType())) {
            this.authSourceType = tableInput.getAuthSourceType();
        }
        if (StringUtils.isNotBlank(tableInput.getAccountId())) {
            this.accountId = tableInput.getAccountId();
        }
        if (StringUtils.isNotBlank(tableInput.getOwnerType())) {
            this.ownerType = tableInput.getOwnerType();
        }
        if (StringUtils.isNotBlank(tableInput.getOwner())) {
            this.owner = tableInput.getOwner();
        }
        if (StringUtils.isNotBlank(tableInput.getDescription())) {
            this.description = tableInput.getDescription();
        }
        if (tableInput.getRetention() != null) {
            this.retention = tableInput.getRetention();
        }
        if (StringUtils.isNotBlank(tableInput.getTableType())) {
            this.tableType = tableInput.getTableType();
        }
        if (tableInput.getParameters() != null) {
            this.parameters = tableInput.getParameters();
        }
        if (StringUtils.isNotBlank(tableInput.getViewOriginalText())) {
            this.viewOriginalText = tableInput.getViewOriginalText();
        }
        if (StringUtils.isNotBlank(tableInput.getViewExpandedText())) {
            this.viewExpandedText = tableInput.getViewExpandedText();
        }

        this.lmsMvcc = tableInput.isLmsMvcc();
    }

    public TableBaseObject(TableBaseInfo tableBaseInfo) {
        this.createTime = tableBaseInfo.getCreateTime();
        this.authSourceType = tableBaseInfo.getAuthSourceType();
        this.accountId = tableBaseInfo.getAccountId();
        this.ownerType = tableBaseInfo.getOwnerType();
        this.owner = tableBaseInfo.getOwner();
        this.description = tableBaseInfo.getDescription();
        this.retention = tableBaseInfo.getRetention();
        this.tableType = tableBaseInfo.getTableType();
        this.parameters = new LinkedHashMap<>(tableBaseInfo.getParametersMap());
        this.viewOriginalText = tableBaseInfo.getViewOriginalText();
        this.viewExpandedText = tableBaseInfo.getViewExpandedText();

        this.lmsMvcc = tableBaseInfo.getLmsMvcc();
    }

}
