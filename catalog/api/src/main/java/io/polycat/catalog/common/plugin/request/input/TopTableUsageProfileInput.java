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

import lombok.Data;

@Data
public class TopTableUsageProfileInput {

    private long startTime;

    private long endTime;

    private String opTypes;

    private long topNum;

    private int usageProfileType;

    private String userId;

    private String taskId;

    public TopTableUsageProfileInput(long startTime, long endTime, String opTypes, long topNum, int usageProfileType) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.opTypes = opTypes;
        this.topNum = topNum;
        this.usageProfileType = usageProfileType;
    }

    public TopTableUsageProfileInput(long startTime, long endTime, String opTypes, long topNum, int usageProfileType, String taskId, String userId) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.opTypes = opTypes;
        this.topNum = topNum;
        this.usageProfileType = usageProfileType;
        this.taskId = taskId;
        this.userId = userId;
    }
}
