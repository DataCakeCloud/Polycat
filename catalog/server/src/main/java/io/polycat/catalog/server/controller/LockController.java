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

import io.polycat.catalog.audit.api.UserLog;
import io.polycat.catalog.audit.impl.UserLogAop;
import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.lock.LockInfo;
import io.polycat.catalog.common.plugin.request.input.LockInput;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.service.api.LockService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author liangyouze
 * @date 2024/2/19
 */

@Api(tags = "lock api")
@ApiResponses(value = {
        @ApiResponse(code = 400, message = "Bad Request"),
        @ApiResponse(code = 408, message = "Request Timeout"),
        @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/lock")
@Validated
public class LockController extends BaseController{

    @Autowired
    private LockService lockService;

    @UserLog(operation = "create lock", objectType = ObjectType.LOCK)
    @PostMapping(value = "createLock", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "create lock")
    public CatalogResponse<LockInfo> createLock(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "lock input body", required = true) @RequestBody LockInput lockInput,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            UserLogAop.setAuditLogObjectName(lockInput.getObjectName());

            LockInfo lockInfo = lockService.createLock(projectId, lockInput);
            //record auditlog
            UserLogAop.setAuditLogDetail("lock " + lockInfo.getLockId() + " create success");
            return ResponseUtil.responseSuccess(lockInfo, HttpStatus.CREATED);
        });
    }

    @GetMapping(value = "checkLock", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "check lock")
    public CatalogResponse<LockInfo> checkLock(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "lock id", required = true) @RequestParam("lockId") Long lockId,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            LockInfo lockInfo = lockService.checklock(projectId, lockId);
            return ResponseUtil.responseSuccess(lockInfo);
        });
    }

    @GetMapping(value = "heartbeat", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "heartbeat")
    public BaseResponse heartbeat(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "lock id", required = true) @RequestParam("lockId") Long lockId,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            lockService.heartbeat(projectId, lockId);
            //record auditlog
            return BaseResponseUtil.responseSuccess();
        });
    }

    @UserLog(operation = "unlock", objectType = ObjectType.LOCK)
    @DeleteMapping(value = "unlock", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "unlock")
    public BaseResponse unlock(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "lock id", required = true) @RequestParam("lockId") Long lockId,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            UserLogAop.setAuditLogObjectName(lockId.toString());
            lockService.unlock(projectId, lockId);
            //record auditlog
            UserLogAop.setAuditLogDetail("lock " + lockId + " unlock success");
            return BaseResponseUtil.responseSuccess();
        });
    }

    @GetMapping(value = "showLocks", produces = "application/json;charset=UTF-8")
    @ApiOperation(value = "showLocks")
    public CatalogResponse<LockInfo[]> showLocks(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            final List<LockInfo> lockInfos = lockService.showLocks(projectId);
            //record auditlog
            return ResponseUtil.responseSuccess(lockInfos.toArray(new LockInfo[0]));
        });
    }

}
