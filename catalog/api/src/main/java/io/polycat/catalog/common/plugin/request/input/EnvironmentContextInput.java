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

import java.util.HashMap;
import java.util.Map;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(value = "environment Context", description = "some additional parameters")
@Data
public class EnvironmentContextInput {

    @ApiModelProperty(value = "environment context")
    private Map<String, String> envContext;

    public void setEnvContext(Map<String, String> envContext) {
        this.envContext = envContext == null ? null : new HashMap<>(envContext);
    }

    public void addEnvContext(String param, String value) {
        if (this.envContext == null) {
            this.envContext = new HashMap<>();
        }
        this.envContext.put(param, value);
    }
}
