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
package io.polycat.catalog.common.lineage;

import io.polycat.catalog.common.utils.LineageUtils;
import lombok.Data;

import java.util.Map;

@Data
public class LineageNode {

    private Integer id;
    /**
     * qualifiedName
     */
    private String qn;
    /**
     * objectType {@link ELineageObjectType}
     */
    private Integer ot;
    /**
     * source {@link ELineageSourceType}
     */
    private Integer st;
    /**
     * dbType {@link EDbType}
     */
    private Integer dt;
    /**
     * Is it temporary
     */
    private boolean tf;

    private Map<String, Object> params;

    public LineageNode() {
    }

    public LineageNode(String qualifiedName, EDbType dbType, ELineageObjectType objectType,
                       boolean tmpFlag) {
        initNode(qualifiedName, dbType, objectType, null, tmpFlag);
    }

    public LineageNode(String qualifiedName, ELineageObjectType objectType, ELineageSourceType source,
                       boolean tmpFlag) {
        initNode(qualifiedName, EDbType.HIVE, objectType, source, tmpFlag);
    }

    public LineageNode(String qualifiedName, EDbType dbType, ELineageObjectType objectType, ELineageSourceType source,
                       boolean tmpFlag) {
        initNode(qualifiedName, dbType, objectType, source, tmpFlag);
    }

    private void initNode(String qualifiedName, EDbType dbType, ELineageObjectType objectType,
                          ELineageSourceType source,
                          boolean tmpFlag) {
        if (!tmpFlag) {
            LineageUtils.checkQualifiedName(qualifiedName, dbType, objectType);
        }
        this.qn = qualifiedName;
        setTmpFlag(tmpFlag);
        setDbType(dbType);
        setSourceType(source);
        setObjectType(objectType);
    }

    public boolean getTmpFlag() {
        return tf;
    }

    public void setTmpFlag(boolean tmpFlag) {
        this.tf = tmpFlag;
    }

    public Integer getDbType() {
        return dt;
    }

    public void setDbType(EDbType dbType) {
        this.dt = dbType.getNum();
    }

    public String getQualifiedName() {
        return qn;
    }

    public void setQualifiedName(String qualifiedName) {
        this.qn = qualifiedName;
    }

    public Integer getObjectType() {
        return ot;
    }

    public void setObjectType(ELineageObjectType objectType) {
        this.ot = objectType.getNum();
    }

    public Integer getSourceType() {
        return st;
    }

    public void setSourceType(ELineageSourceType sourceType) {
        if (sourceType != null) {
            this.st = sourceType.getNum();
        }
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
}
