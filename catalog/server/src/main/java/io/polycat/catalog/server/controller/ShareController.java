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

import java.util.List;

import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.Share;
import io.polycat.catalog.common.plugin.request.input.ShareInput;
import io.polycat.catalog.service.api.ShareService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
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

@Api(tags = "share api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/shares")
public class ShareController extends BaseController {

    @Autowired
    private ShareService shareService;

    /**
     * create share
     *
     * @param projectId projectId
     * @return CatalogResponse
     */
    @ApiOperation(value = "create share")
    @PostMapping(value = "", produces = "application/json;charset=UTF-8")
    public BaseResponse createShare(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "share input body", required = true) @RequestBody ShareInput shareInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            shareService.createShare(projectId, shareInput);
            return BaseResponseUtil.responseSuccess(HttpStatus.CREATED);
        });
    }

    /**
     * drop share by id
     *
     * @param projectId projectId
     * @param shareId   shareId
     * @return CatalogResponse
     */
    @ApiOperation(value = "drop share by id")
    @DeleteMapping(value = "/{shareId}", produces = "application/json;charset=UTF-8")
    public BaseResponse dropShareById(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "share id", required = true) @PathVariable("shareId") String shareId,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            shareService.dropShareById(projectId, shareId);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * drop share By name
     *
     * @param projectId projectId
     * @param shareName shareName
     * @return CatalogResponse
     */
    @ApiOperation(value = "drop share by name")
    @DeleteMapping(value = "", produces = "application/json;charset=UTF-8")
    public BaseResponse dropShareByName(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "share name", required = true) @RequestParam("shareName") String shareName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            shareService.dropShareByName(projectId, shareName);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * alter share
     *
     * @param projectId projectId
     * @param shareName shareName
     * @return CatalogResponse
     */
    @ApiOperation(value = "alter share")
    @PatchMapping(value = "/{shareName}", produces = "application/json;charset=UTF-8")
    public BaseResponse alterShare(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "share name", required = true) @PathVariable("shareName") String shareName,
        @ApiParam(value = "share input body", required = true) @Valid @RequestBody ShareInput shareInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            shareService.alterShare(projectId, shareName, shareInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * add accounts to share
     *
     * @param projectId projectId
     * @param shareName shareName
     * @return CatalogResponse
     */
    @ApiOperation(value = "add accounts to share")
    @PatchMapping(value = "/{shareName}/addAccounts", produces = "application/json;charset=UTF-8")
    public BaseResponse addConsumersToShare(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "share name", required = true) @PathVariable("shareName") String shareName,
        @ApiParam(value = "share input body", required = true) @Valid @RequestBody ShareInput shareInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            shareService.addConsumersToShare(projectId, shareName, shareInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * add users to share
     *
     * @param projectId projectId
     * @param shareName shareName
     * @return CatalogResponse
     */
    @ApiOperation(value = "add users to share")
    @PatchMapping(value = "/{shareName}/addUsers", produces = "application/json;charset=UTF-8")
    public BaseResponse addUsersToShare(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "share name", required = true) @PathVariable("shareName") String shareName,
        @ApiParam(value = "share input body", required = true) @Valid @RequestBody ShareInput shareInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            shareService.addUsersToShareConsumer(shareInput.getProjectId(), shareName, shareInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * remove accounts from share
     *
     * @param projectId projectId
     * @param shareName shareName
     * @return CatalogResponse
     */
    @ApiOperation(value = "remove accounts from share")
    @PatchMapping(value = "/{shareName}/removeAccounts", produces = "application/json;charset=UTF-8")
    public BaseResponse removeAccountsFromShare(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "share name", required = true) @PathVariable("shareName") String shareName,
        @ApiParam(value = "share input body", required = true) @Valid @RequestBody ShareInput shareInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            shareService.removeConsumersFromShare(projectId, shareName, shareInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * remove users from share
     *
     * @param projectId projectId
     * @param shareName shareName
     * @return CatalogResponse
     */
    @ApiOperation(value = "remove users from share")
    @PatchMapping(value = "/{shareName}/removeUsers", produces = "application/json;charset=UTF-8")
    public BaseResponse removeUsersFromShare(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "share name", required = true) @PathVariable("shareName") String shareName,
        @ApiParam(value = "share input body", required = true) @Valid @RequestBody ShareInput shareInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            shareService.removeUsersFromShareConsumer(shareInput.getProjectId(), shareName, shareInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * grant privilege to share
     *
     * @param projectId projectId
     * @param shareName shareName
     * @return CatalogResponse
     */
    @ApiOperation(value = "grant privilege to share")
    @PatchMapping(value = "/{shareName}/grantPrivilege",
        produces = "application/json;charset=UTF-8")
    public BaseResponse grantPrivilegeToShare(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "share name", required = true) @PathVariable("shareName") String shareName,
        @ApiParam(value = "share input body", required = true) @Valid @RequestBody ShareInput shareInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            shareService.addPrivilegeToShare(projectId, shareName, shareInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * revoke privilege from share
     *
     * @param projectId projectId
     * @param shareName shareName
     * @return CatalogResponse
     */
    @ApiOperation(value = "revoke privilege from share")
    @PatchMapping(value = "/{shareName}/revokePrivilege", produces = "application/json;charset=UTF-8")
    public BaseResponse revokePrivilegeFromShare(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "share name", required = true) @PathVariable("shareName") String shareName,
        @ApiParam(value = "share input body", required = true) @Valid @RequestBody ShareInput shareInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            shareService.removePrivilegeFromShare(projectId, shareName, shareInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * get Share by name
     *
     * @param projectId
     * @param shareName
     * @return CatalogResponse
     */
    @ApiOperation(value = "get share by name")
    @GetMapping(value = "", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Share> getShareByName(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "share name", required = true) @RequestParam("shareName") String shareName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            Share share = shareService.getShareByName(projectId, shareName);
            return ResponseUtil.responseSuccess(share);
        });
    }

    /**
     * get share by Id
     *
     * @param projectId
     * @param shareId
     * @return CatalogResponse
     */
    @ApiOperation(value = "get share by id")
    @GetMapping(value = "/{shareId}", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Share> getShareById(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "share id", required = true) @RequestParam("shareId") String shareId,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            Share share = shareService.getShareById(projectId, shareId);
            return ResponseUtil.responseSuccess(share);
        });
    }

    /**
     * list shares
     *
     * @param projectId
     * @param namePattern
     * @return CatalogResponse
     */
    @ApiOperation(value = "show shares")
    @GetMapping(value = "/showShares",
        produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<Share>> showShares(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "account id", required = true) @RequestParam("accountId") String accountId,
        @ApiParam(value = "user id", required = true) @RequestParam("userId") String userId,
        @ApiParam(value = "name pattern") @RequestParam(value = "namePattern", required = false, defaultValue = "")
            String namePattern,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            PagedList<Share> result = new PagedList<>();
            List<Share> shares = shareService.getShareModels(projectId, accountId, userId, namePattern);
            result.setObjects(shares.toArray(new Share[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }
}
