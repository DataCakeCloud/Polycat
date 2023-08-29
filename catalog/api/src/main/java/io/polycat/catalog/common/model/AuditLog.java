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


import io.polycat.catalog.common.ObjectType;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.Map;

@Data
@Builder
@Accessors(chain = true)
public class AuditLog {
    private String id;

    private long timestamp;

    private String sourceIp;

    private String pathUri;
    private String method;
    private long timeConsuming;

    private int remotePort;

    private String projectId;

    private String userId;

    private String operation;

    private ObjectType objectType;

    private String objectId;

    private String objectName;

    private String  state;

    private Map<String, Object> requestParams;
    private String detail;
    private BaseResponse response;

}
