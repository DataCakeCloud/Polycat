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
import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class LineageEdgeFact {
    /**
     * default null
     */
    private String id;
    /**
     * execute user
     */
    private String executeUser;

    private Short jobStatus;
    /**
     * job type, spark-sql/trino/hive
     */
    private String jobType;
    /**
     * task id  is spark: applicationId  or  hive: jobId
     */
    private String jobId;
    /**
     * job name
     */
    private String jobName;
    /**
     * process type  scheduler/once/other
     */
    private String processType;
    /**
     * job status:FAILED error message.
     */
    private String errorMsg;
    /**
     * execute sql
     */
    private String sql;
    /**
     * exec cluster
     */
    private String cluster;
    /**
     * job start time  queryPlan startTime
     */
    private Long startTime;
    /**
     * job end time
     */
    private Long endTime;

    private Long createTime;

    /**
     * other properties
     */
    @TableField(value = "params", typeHandler = JsonbTypeHandler.class)
    private Object params = new LinkedHashMap<>();

    public Map<String, Object> getParamsBean() {
        return PGDataUtil.getMapBean(params);
    }

}
