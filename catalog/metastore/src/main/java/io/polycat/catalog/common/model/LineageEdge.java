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

import io.polycat.catalog.util.PGDataUtil;
import lombok.Data;

import java.util.Map;

@Data
public class LineageEdge {

    private Integer upstreamId;

    private Integer downstreamId;

    private Integer lineageType;

    private Object edgeInfo;

    private int updateCount;

    public EdgeInfo getEdgeInfoBean() {
        return PGDataUtil.getJsonBean(edgeInfo, EdgeInfo.class);
    }

    @Data
    public static class EdgeInfo {

        private long ct;
        private String dn;
        private Short dot;
        private Short ddt;
        private Boolean dtf;
        private String un;
        private Short uot;
        private Short udt;
        private Boolean utf;
        private String eu;
        private Short js;

        private String tt;
        /**
         * edge_fact_id
         */
        private String efi;

        private Map<String, Object> params;
    }
}
