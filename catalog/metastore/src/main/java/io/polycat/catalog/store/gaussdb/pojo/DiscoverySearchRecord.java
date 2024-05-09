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
package io.polycat.catalog.store.gaussdb.pojo;

import com.baomidou.mybatisplus.annotation.TableField;
import io.polycat.catalog.store.gaussdb.handler.JsonbTypeHandler;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.experimental.FieldNameConstants;

@Data
@Accessors(chain = true)
@FieldNameConstants
public class DiscoverySearchRecord {
    private String id;
    String qualifiedName;
    Integer objectType;
    double score;
    long recentVisitCount;
    long lastAccessTime;
    private String owner;
    private String catalogName;
    private Long createTime;
    private Long updateTime;
    private String keywords;
    @TableField(value = "search_info", typeHandler = JsonbTypeHandler.class)
    Object searchInfo;
    private String tsTokens;
    private String tsATokens;
    /**
     * Index position tokens have been set.
     */
    private String tsASetIndexTokens;
    private String tsBTokens;
    private String tsBSetIndexTokens;
    private String tsCTokens;
    private String tsCSetIndexTokens;
    private String tsDTokens;
    private String tsDSetIndexTokens;
}
