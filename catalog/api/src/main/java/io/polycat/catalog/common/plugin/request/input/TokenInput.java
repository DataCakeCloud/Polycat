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

import io.polycat.catalog.common.Constants;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

@ApiModel(description = "MRS kerberos token api")
@Getter
public class TokenInput {
    @NotNull
    @ApiModelProperty(value = "token type like: token/ masterKey", required = true)
    String tokenType;

    @ApiModelProperty(value = "token identity")
    String tokenId;

    @ApiModelProperty(value = "token")
    String token;

    public void setMasterKey(String Key) {
        tokenType = Constants.MASTER_KEY;
        token = Key;
    }

    public void setMasterKey(Integer keySeq, String Key) {
        tokenType = Constants.MASTER_KEY;
        tokenId = keySeq.toString();
        token = Key;
    }

    public void setMRSToken(String tokenId, String token) {
        tokenType = Constants.MRS_TOKEN;
        this.tokenId = tokenId;
        this.token = token;
    }
}
