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

import lombok.Data;

@Data
public class BackendTaskObject {
    private String taskId = "";
    // primary key end
    private String taskName = "";
    private BackendTaskType taskType;
    private String projectId = "";
    private Map<String, String> params = Collections.emptyMap();

    public BackendTaskObject() {
        this.params = new LinkedHashMap<>();
    }

    public String getParamsOrThrow(String key) {
        if (key == null) { throw new java.lang.NullPointerException(); }
        if (!this.params.containsKey(key)) {
            throw new java.lang.IllegalArgumentException();
        }
        return this.params.get(key);
    }
}
