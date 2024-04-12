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
package io.polycat.catalog.server.controller;

import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.Constants;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.KerberosToken;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.plugin.request.input.TokenInput;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.service.api.TokenService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "token api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/mrsTokens")
public class TokenController extends BaseController{
    @Autowired
    private TokenService tokenService;

    @ApiOperation(value = "create MRS token")
    @PostMapping(value = "/{tokenType}", produces = "application/json;charset=UTF-8")
    public CatalogResponse<KerberosToken> createToken(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "token Type", required = true) @PathVariable("tokenType") String tokenType,
        @ApiParam(value = "token input body", required = true) @Valid @RequestBody TokenInput tokenBody,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            KerberosToken kerberosToken;
            if (tokenType.equals(Constants.MRS_TOKEN)) {
                kerberosToken = tokenService.setToken(projectId, tokenBody);
            } else if (tokenType.equals(Constants.MASTER_KEY)) {
                kerberosToken = tokenService.addMasterKey(projectId, tokenBody);
            } else {
                throw new CatalogServerException(ErrorCode.INVALID_OPERATION,
                    "createToken with TokenType:" + tokenType);
            }
            return ResponseUtil.responseSuccess(kerberosToken);
        });
    }

    @ApiOperation(value = "get MRS token")
    @GetMapping(value = "/{tokenType}", produces = "application/json;charset=UTF-8")
    public CatalogResponse<KerberosToken> getTokenByTokenStrForm(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "token Type", required = true) @PathVariable("tokenType") String tokenType,
        @ApiParam(value = "tokenStrForm", required = true) @RequestParam("tokenId") String tokenStrForm,
        @ApiParam(value = "tokenOwner", required = true) @RequestParam(value = "owner", required = false) String owner,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            KerberosToken kerberosToken;
            if (tokenType.equals(Constants.MRS_TOKEN)) {
                kerberosToken = tokenService.getDelegationToken(projectId, owner, tokenStrForm);
            } else {
                throw new CatalogServerException(ErrorCode.INVALID_OPERATION, "getToken with TokenType:" + tokenType);
            }
            return ResponseUtil.responseSuccess(kerberosToken);
        });
    }

    @ApiOperation(value = "get MRS token")
    @GetMapping(value = "/{tokenType}/{tokenId}", produces = "application/json;charset=UTF-8")
    public CatalogResponse<KerberosToken> getToken(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "token Type", required = true) @PathVariable("tokenType") String tokenType,
        @ApiParam(value = "tokenStrForm", required = true) @PathVariable("tokenId") String tokenId,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            KerberosToken kerberosToken;
            if (tokenType.equals(Constants.MRS_TOKEN)) {
                kerberosToken = tokenService.getToken(projectId, tokenId);
            } else {
                throw new CatalogServerException(ErrorCode.INVALID_OPERATION, "getToken with TokenType:" + tokenType);
            }
            return ResponseUtil.responseSuccess(kerberosToken);
        });
    }

    @ApiOperation(value = "drop MRS token")
    @DeleteMapping(value = "/{tokenType}/{tokenId}", produces = "application/json;charset=UTF-8")
    public BaseResponse dropToken(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "token id", required = true) @PathVariable("tokenId") String tokenId,
        @ApiParam(value = "token Type", required = true) @PathVariable("tokenType") String tokenType,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            if (tokenType.equals(Constants.MRS_TOKEN)) {
                tokenService.deleteToken(projectId, tokenId);
            } else if(tokenType.equals(Constants.MASTER_KEY)){
                tokenService.removeMasterKey(projectId, projectId);
            } else {
                throw new CatalogServerException(ErrorCode.INVALID_OPERATION, "dropToken with TokenType:" + tokenType);
            }
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "list MRS token")
    @GetMapping(value = "/{tokenType}/list", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<KerberosToken>> listTokens(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "token Type", required = true) @PathVariable("tokenType") String tokenType,
        @ApiParam(value = "token pattern") @RequestParam("pattern") String pattern,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            PagedList<KerberosToken> mrsTokens = new PagedList<>();
            if (tokenType.equals(Constants.MRS_TOKEN)) {
                mrsTokens.setObjects(tokenService.listToken(projectId,pattern));
            } else if (token.equals(Constants.MASTER_KEY)) {
                mrsTokens.setObjects(tokenService.getMasterKeys(projectId, pattern));
            } else {
                throw new CatalogServerException(ErrorCode.INVALID_OPERATION, "getToken with TokenType:" + tokenType);
            }
            return ResponseUtil.responseSuccess(mrsTokens);
        });
    }

    @ApiOperation(value = "alter MRS token")
    @PatchMapping(value = "/{tokenType}/{tokenId}", produces = "application/json;charset=UTF-8")
    public CatalogResponse<KerberosToken> alterToken(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "token id", required = true) @PathVariable("tokenId") String tokenId,
        @ApiParam(value = "token Type", required = true) @PathVariable("tokenType") String tokenType,
        @ApiParam(value = "token Type", required = true) @RequestParam(value = "operateType", required = false) String operateType,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            KerberosToken kerberosToken;
            if (tokenType.equals(Constants.MRS_TOKEN)) {
                if (operateType.equals(Constants.RENEW_TOKEN)) {
                    kerberosToken = tokenService.renewToken(projectId, tokenId);
                } else if(operateType.equals(Constants.CANCEL_TOKEN)){
                    kerberosToken = tokenService.cancelDelegationToken(projectId, tokenId);
                } else {
                    throw new CatalogServerException(ErrorCode.INVALID_OPERATION,
                        "listTokens with TokenType:" + operateType);
                }
            } else {
                throw new CatalogServerException(ErrorCode.INVALID_OPERATION,
                    "listTokens with TokenType:" + operateType);
            }

            return ResponseUtil.responseSuccess(kerberosToken);
        });
    }
}
