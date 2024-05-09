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
package io.polycat.catalog.common.plugin.request.input;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import io.polycat.catalog.common.lineage.*;
import io.polycat.catalog.common.utils.LineageUtils;
import lombok.Data;

@Data
public class LineageInfoInput {
    private Map<String, LineageRsInput> rsMap;

    private Map<String, LineageNode> nodeMap;

    private LineageFact jobFact;

    private Map<String, Set<LineageColumnInfo>> tableColumnsCache;
    /**
     *  insert or created table name
     */
    private String targetTable;

    /**
     * entity create time
     */
    private long createTime;

    @Data
    public static class LineageRsInput {
        /**
         * downstream/output LineageNode lineageUniqueName
         */
        private String dn;
        /**
         * upstream/input LineageNode lineageUniqueName
         */
        private Set<String> uns;
        /**
         * lineage type {@link ELineageType}
         */
        private int lt;
        /**
         * When calculating dependency way {@link EDependWay}
         */
        private Integer dw;
        /**
         * calculating dependency code segment.
         */
        private String dcs;

        private Map<String, Object> params;

        public LineageRsInput() {
        }

        public LineageRsInput(ELineageType lt) {
            setLineageType(lt);
        }

        public LineageRsInput(ELineageType lineageType, EDependWay dependWay, String codeSegment) {
            setLineageType(lineageType);
            setDependencyWay(dependWay);
            setDependCodeSegment(codeSegment);
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public void putParam(String key, Object value) {
            if (params == null) {
                params = new LinkedHashMap<>();
            }
            this.params.put(key, value);
        }

        public Set<String> getUns() {
            return uns;
        }

        public void addUpstreamNode(LineageNode node) {
            addUpstreamNode(node.getDbType(), node.getObjectType(), node.getQualifiedName());
        }

        public void addUpstreamNode(EDbType dbType, ELineageObjectType objectType, String qualifiedName) {
            addUpstreamNode(dbType.getNum(), objectType.getNum(), qualifiedName);
        }

        public void addUpstreamNode(int dbType, int objectType, String qualifiedName) {
            if (uns == null) {
                uns = new LinkedHashSet<>();
            }
            uns.add(LineageUtils.getLineageUniqueName(dbType, objectType, qualifiedName));
        }

        public void mergeUpstreamNodes(Set<String> nodeSet) {
            if (uns == null) {
                uns = nodeSet;
                return;
            }
            if (nodeSet != null) {
                uns.addAll(nodeSet);
            }
        }

        public String getDn() {
            return dn;
        }

        public void setDownstreamNode(LineageNode node) {
            this.dn = LineageUtils.getLineageUniqueName(node.getDbType(), node.getObjectType(), node.getQualifiedName());
        }

        public void setDownstreamNode(EDbType dbType, ELineageObjectType objectType, String qualifiedName) {
            this.dn = LineageUtils.getLineageUniqueName(dbType, objectType, qualifiedName);
        }

        public void setDownstreamNode(int dbType, int objectType, String qualifiedName) {
            this.dn = LineageUtils.getLineageUniqueName(dbType, objectType, qualifiedName);
        }

        public boolean existsDownstreamCol() {
            if (uns != null && uns.size() > 0) {
                return true;
            }
            return false;
        }

        public int getLineageType() {
            return lt;
        }

        public void setLineageType(ELineageType lt) {
            this.lt = lt.getNum();
        }

        public void setDependencyWay(EDependWay dependWay) {
            this.dw = dependWay.getNum();
            putParam(LineageConstants.LINEAGE_KEY_COLUMN_DW, this.dw);
        }

        public void setDependCodeSegment(String codeSegment) {
            this.dcs = codeSegment;
            putParam(LineageConstants.LINEAGE_KEY_COLUMN_DCS, this.dcs);
        }
    }

    public LineageInfoInput() {
        this.createTime = System.currentTimeMillis();
    }

    public Map<String, LineageNode> getNodeMap() {
        return nodeMap;
    }

    public Map<String, LineageRsInput> getRsMap() {
        return rsMap;
    }

    public void addRelationShipMap(LineageRsInput rs) {
        if (this.rsMap == null) {
            this.rsMap = new LinkedHashMap<>();
        }
        validRsNodeExists(rs);
        String uniqueName = LineageUtils.getLineageRsUniqueName(rs.getLineageType(), rs.getDn());
        if (this.rsMap.containsKey(uniqueName)) {
            this.rsMap.get(uniqueName).mergeUpstreamNodes(rs.getUns());
        } else {
            this.rsMap.put(uniqueName, rs);
        }
    }

    private void validRsNodeExists(LineageRsInput rs) {
        if (this.nodeMap == null) {
            throw new NullPointerException("Lineage node can not be null or empty.");
        }
        validNodeExists(rs.getDn());
        Set<String> uns = rs.getUns();
        if (uns != null) {
            for (String un : uns) {
                validNodeExists(un);
            }
        }
    }

    private void validNodeExists(String lineageUniqueName) {
        if (this.nodeMap == null) {
            throw new NullPointerException("Lineage node can not be null or empty.");
        }
        if (!this.nodeMap.containsKey(lineageUniqueName)) {
            throw new IllegalArgumentException("Lineage node=[" + lineageUniqueName + "] does not exist.");
        }
    }

    public LineageFact getJobFact() {
        return jobFact;
    }

    public void setJobFact(LineageFact jobFact) {
        this.jobFact = jobFact;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public void addNode(LineageNode node) {
        if (this.nodeMap == null) {
            this.nodeMap = new LinkedHashMap<>();
        }
        if (node == null) {
            throw new NullPointerException("LineageNode is null.");
        }
        if (node.getObjectType() == null || node.getQualifiedName() == null) {
            throw new NullPointerException("LineageNode objectType or qualifiedName is null.");
        }
        this.nodeMap.put(getLineageUniqueName(node), node);
    }

    private static String getLineageUniqueName(LineageNode node) {
        return LineageUtils.getLineageUniqueName(node.getDbType(), node.getObjectType(), node.getQualifiedName());
    }

    public LineageNode getNode(EDbType dbType, ELineageObjectType objectType, String qualifiedName) {
        if (this.nodeMap == null) {
            this.nodeMap = new LinkedHashMap<>();
        }
        if (qualifiedName == null) {
            throw new NullPointerException("LineageNode qualifiedName is null.");
        }
        return this.nodeMap.get(LineageUtils.getLineageUniqueName(dbType, objectType, qualifiedName));
    }

    public List<String> getDownstreamCacheTableByUpstreamTableNode(EDbType dbType, String tableName) {
        List<String> downstreamTableNode = new ArrayList<>();
        if (getRsMap() == null) {
            return downstreamTableNode;
        }
        String qualifiedName = LineageUtils.getLineageUniqueName(dbType, ELineageObjectType.TABLE, tableName);
        for (LineageRsInput input : getRsMap().values()) {
            if (!input.getUns().contains(qualifiedName)) {
                continue;
            }
            downstreamTableNode.add(input.getDn());
        }
        // processing with only one downstream table
        if (downstreamTableNode.isEmpty() && qualifiedName.equalsIgnoreCase(targetTable)) {
            return Collections.singletonList(qualifiedName);
        }
        return downstreamTableNode;
    }

    public List<LineageColumnInfo> getCachedTableColumns(String tableName) {
        List<LineageColumnInfo> colNodes = new ArrayList<>();
        for (Map.Entry<String, Set<LineageColumnInfo>> entry : tableColumnsCache.entrySet()) {
            if (tableName.contains(entry.getKey())) {
                colNodes.addAll(entry.getValue());
            }
        }
        return colNodes;
    }

    public void cacheTableColumns(String tableName, LineageColumnInfo columnInfo) {
        if (null == tableColumnsCache) {
            tableColumnsCache = new HashMap<>();
        }
        Set<LineageColumnInfo> columns = tableColumnsCache.computeIfAbsent(tableName, k -> new HashSet<>());
        columns.add(columnInfo);
    }

    @Data
    public static class LineageColumnInfo {
        /**
         * Is it temporary
         */
        private boolean tf;
        /**
         * temporary name
         */
        private String tfName;
        /**
         * temporary name source express
         */
        private String tfSourceExpress;
        /**
         * temporary name source name
         */
        private List<String> tfSourceName;

    }


    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
