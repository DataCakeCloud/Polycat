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

import java.util.Map;

import lombok.Data;

@Data
public class AcceleratorPropertiesObject {
    private String acceleratorId;
    private String acceleratorName;
    private String lib;
    private String sqlStatement;
    private String location;
    private boolean compiled;
    private Map<String, String> properties;

    public AcceleratorPropertiesObject(String acceleratorId, String acceleratorName, String lib,
        String sqlStatement, String location, boolean compiled, Map<String, String> properties) {
        this.acceleratorId = acceleratorId;
        this.acceleratorName = acceleratorName;
        this.lib = lib;
        this.sqlStatement = sqlStatement;
        this.location = location;
        this.compiled = compiled;
        this.properties = properties;
    }
}
