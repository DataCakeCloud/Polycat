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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(description = "delegate")
@Data
public class DelegateOutput {
    @ApiModelProperty(value = "storage provider", required = true)
    private String storageProvider;

    @ApiModelProperty(value = "provider domain name", required = true)
    private String providerDomainName;

    @ApiModelProperty(value = "agency name", required = true)
    private String agencyName;

    @ApiModelProperty(value = "allowed location list", required = true)
    private List<String> allowedLocationList;

    @ApiModelProperty(value = "blocked location list", required = true)
    private List<String> blockedLocationsList;

    public DelegateOutput(String storageProvider, String providerDomainName, String agencyName,
        List<String> allowedLocationList, List<String> blockedLocationsList) {
        this.storageProvider = storageProvider;
        this.providerDomainName = providerDomainName;
        this.agencyName = agencyName;
        this.allowedLocationList = allowedLocationList;
        this.blockedLocationsList = blockedLocationsList;
    }
}
