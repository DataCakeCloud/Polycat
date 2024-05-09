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

import com.baomidou.mybatisplus.annotation.TableField;
import io.polycat.catalog.store.gaussdb.handler.JsonbTypeHandler;
import io.polycat.catalog.util.PGDataUtil;
import io.polycat.catalog.common.lineage.EDbType;
import io.polycat.catalog.common.lineage.ELineageObjectType;
import io.polycat.catalog.common.lineage.ELineageSourceType;
import lombok.Data;
import lombok.experimental.FieldNameConstants;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
@FieldNameConstants
public class LineageVertex {
    
    private Integer id;
    /**
     * qualifiedName
     */
    private String qualifiedName;
    /**
     * objectType {@link ELineageObjectType}
     */
    private Short objectType;
    /**
     * sourceType {@link ELineageSourceType}
     */
    private Short sourceType;
    /**
     * dbType {@link EDbType}
     */
    private Short dbType;
    private Boolean active = true;
    private Integer updateCount = 0;
    private Integer deleteCount = 0;
    private Long lastDelTime = 0L;
    private Long createTime = System.currentTimeMillis();
    /**
     * Is it temporary
     */
    private boolean tmpFlag;

    private String factId;
    @TableField(value = "params", typeHandler = JsonbTypeHandler.class)
    private Object params = new LinkedHashMap<>();

    public Map<String, Object> getParamsBean() {
        return PGDataUtil.getMapBean(params);
    }
}
