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
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@ApiModel(description = "table storage descriptor")
@NoArgsConstructor
@AllArgsConstructor
public class SerDeInfo implements Serializable {

    @ApiModelProperty(value = "name")
    private String name = "";

    @ApiModelProperty(value = "serialization library")
    private String serializationLibrary = "";

    @ApiModelProperty(value = "parameters")
    private Map<String, String> parameters = new LinkedHashMap<>();

    public SerDeInfo(SerDeInfo other) {
        name = other.getName();
        serializationLibrary = other.getSerializationLibrary();
        parameters = other.getParameters();
    }

    @ApiModelProperty(hidden = true)
    public SerDeInfo deepCopy() {
        SerDeInfo newSerDeInfo = new SerDeInfo();
        newSerDeInfo.setName(name);
        newSerDeInfo.setSerializationLibrary(serializationLibrary);
        if (parameters != null) {
            Map<String, String> params = new HashMap<>(parameters);
            newSerDeInfo.setParameters(params);
        }
        return newSerDeInfo;
    }
}
