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

import java.util.List;

import io.polycat.catalog.common.plugin.request.input.FunctionResourceUri;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FunctionObject {
    private String functionName;
    private String databaseName;
    private String className;
    private String ownerName;
    private String ownerType;
    private String functionType;
    private long createTime;
    List <FunctionResourceUri> resourceUriObjectList;

    public FunctionObject(String functionName, String className, String ownerName, String ownerType,
                          String functionType, long createTime, List <FunctionResourceUri> resourceUriObjectList) {
        this.functionName = functionName;
        this.className = className;
        this.ownerName = ownerName;
        this.ownerType = ownerType;
        this.functionType = functionType;
        this.createTime = createTime;
        this.resourceUriObjectList = resourceUriObjectList;
    }

}
