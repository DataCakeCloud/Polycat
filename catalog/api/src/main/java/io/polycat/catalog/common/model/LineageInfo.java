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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import io.polycat.catalog.common.lineage.*;

public class LineageInfo {

    private List<LineageRs> relations;
    private List<LineageNode> nodes;
    private LineageNode baseNode;

    public LineageInfo() {
    }

    public List<LineageRs> getRelations() {
        return relations;
    }

    public void setRelations(List<LineageRs> relations) {
        this.relations = relations;
    }

    public List<LineageNode> getNodes() {
        return nodes;
    }

    public void setNodes(List<LineageNode> nodes) {
        this.nodes = nodes;
    }

    public LineageNode getBaseNode() {
        return baseNode;
    }

    public void setBaseNode(LineageNode baseNode) {
        this.baseNode = baseNode;
    }

    public void addRelation(LineageRs rs) {
        if (this.relations == null) {
            this.relations = new LinkedList<>();
        }
        if (rs == null) {
            throw new NullPointerException("LineageRs is null.");
        }
        this.relations.add(rs);
    }

    public void addNode(LineageNode node) {
        if (this.nodes == null) {
            this.nodes = new LinkedList<>();
        }
        if (node == null) {
            throw new NullPointerException("LineageNode is null.");
        }
        if (node.getObjectType() == null || node.getQualifiedName() == null) {
            throw new NullPointerException("LineageNode objectType or qualifiedName is null.");
        }
        this.nodes.add(node);
    }

    public static class LineageRs {
        private Integer upId;
        private Integer downId;
        private Integer lineageType;
        private Integer jobStatus;
        private String executeUser;
        private String jobFactId;
        private Map<String, Object> params;

        public LineageRs() {
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public void setParams(Map<String, Object> params) {
            this.params = params;
        }

        public Integer getUpId() {
            return upId;
        }

        public void setUpId(Integer upId) {
            this.upId = upId;
        }

        public Integer getDownId() {
            return downId;
        }

        public void setDownId(Integer downId) {
            this.downId = downId;
        }

        public Integer getLineageType() {
            return lineageType;
        }

        public void setLineageType(Integer lineageType) {
            this.lineageType = lineageType;
        }

        public Integer getJobStatus() {
            return jobStatus;
        }

        public void setJobStatus(Integer jobStatus) {
            this.jobStatus = jobStatus;
        }

        public String getExecuteUser() {
            return executeUser;
        }

        public void setExecuteUser(String executeUser) {
            this.executeUser = executeUser;
        }

        public String getJobFactId() {
            return jobFactId;
        }

        public void setJobFactId(String jobFactId) {
            this.jobFactId = jobFactId;
        }
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
