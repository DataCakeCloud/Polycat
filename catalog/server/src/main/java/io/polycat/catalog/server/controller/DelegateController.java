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


import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.DelegateBriefInfo;
import io.polycat.catalog.common.model.DelegateOutput;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.plugin.request.input.DelegateInput;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.server.util.ResponseUtil;

import io.polycat.catalog.service.api.DelegateService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "delegate api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/delegates")
public class DelegateController extends BaseController {

    @Autowired
    private DelegateService delegateService;

    @ApiOperation(value = "create delegate")
    @PostMapping(value = "", produces = "application/json;charset=UTF-8")
    public CatalogResponse<DelegateOutput> createDelegate(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "delegate input body", required = true) @Valid @RequestBody DelegateInput delegateBody,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            DelegateOutput sdOut = delegateService.createDelegate(projectId, delegateBody);
            return ResponseUtil.responseSuccess(sdOut);
        });
    }

    @ApiOperation(value = "get delegate")
    @GetMapping(value = "", produces = "application/json;charset=UTF-8")
    public CatalogResponse<DelegateOutput> getDelegate(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "delegate name", required = true) @RequestParam("delegate") String delegateName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            DelegateOutput delegate = delegateService.getDelegate(projectId, delegateName);
            return ResponseUtil.responseSuccess(delegate);
        });
    }

    @ApiOperation(value = "drop delegate")
    @DeleteMapping(value = "", produces = "application/json;charset=UTF-8")
    public BaseResponse dropDelegate(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "delegate name", required = true) @RequestParam("delegate") String delegateName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            delegateService.dropDelegate(projectId, delegateName);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "list delegates")
    @GetMapping(value = "/list", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<DelegateBriefInfo>> listDelegates(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "pattern") @RequestParam(value = "pattern", required = false) String pattern,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            PagedList<DelegateBriefInfo> result = new PagedList<>();
            result.setObjects(delegateService.listDelegates(projectId, pattern).toArray(new DelegateBriefInfo[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }
}
