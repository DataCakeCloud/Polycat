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

import io.polycat.catalog.audit.api.UserLog;
import io.polycat.catalog.audit.impl.UserLogAop;
import io.polycat.catalog.server.util.ResponseUtil;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.PrivilegeRoles;
import io.polycat.catalog.common.model.Role;
import io.polycat.catalog.common.model.TraverseCursorResult;
import io.polycat.catalog.common.plugin.request.input.RoleInput;
import io.polycat.catalog.common.plugin.request.input.ShowRolePrivilegesInput;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.service.api.RoleService;

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

@Api(tags = "role api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/roles")
public class RoleController extends BaseController {

    @Autowired
    private RoleService roleService;

    /**
     * create role
     *
     * @param projectId projectId
     * @return CatalogResponse
     */
    @ApiOperation(value = "create role")
    @PostMapping(value = "", produces = "application/json;charset=UTF-8")
    public BaseResponse createRole(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "role input body", required = true) @RequestBody RoleInput roleInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            roleService.createRole(projectId, roleInput);
            return BaseResponseUtil.responseSuccess(HttpStatus.CREATED);
        });
    }

    /**
     * drop role by id
     *
     * @param projectId projectId
     * @param roleId    roleId
     * @return CatalogResponse
     */
    @ApiOperation(value = "drop role by id")
    @DeleteMapping(value = "/{roleId}", produces = "application/json;charset=UTF-8")
    public BaseResponse dropRoleById(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "role id", required = true) @PathVariable("roleId") String roleId,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            roleService.dropRoleById(projectId, roleId);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * drop role by name
     *
     * @param projectId projectId
     * @param roleName  roleName
     * @return CatalogResponse
     */
    @ApiOperation(value = "drop role by name")
    @DeleteMapping(value = "", produces = "application/json;charset=UTF-8")
    public BaseResponse dropRoleByName(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "role name", required = true) @RequestParam("roleName") String roleName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            roleService.dropRoleByName(projectId, roleName);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * alter role
     *
     * @param projectId projectId
     * @param roleName  roleName
     * @return CatalogResponse
     */
    @ApiOperation(value = "alter role")
    @PatchMapping(value = "/{roleName}", produces = "application/json;charset=UTF-8")
    public BaseResponse alterRole(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "role name", required = true) @PathVariable("roleName") String roleName,
        @ApiParam(value = "role input body", required = true) @Valid @RequestBody RoleInput roleInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            roleService.alterRole(projectId, roleName, roleInput);
            //newRoleService.alterRoleName(projectId,roleName,roleInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * grant privilege to role
     *
     * @param projectId projectId
     * @param roleName  roleName
     * @return CatalogResponse
     */
    @ApiOperation(value = "grant privilege to role")
    @PatchMapping(value = "/{roleName}/grantPrivilege", produces = "application/json;charset=UTF-8")
    public BaseResponse grantPrivilegeToRole(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "role name", required = true) @PathVariable("roleName") String roleName,
        @ApiParam(value = "role input body", required = true) @Valid @RequestBody RoleInput roleInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            roleService.addPrivilegeToRole(projectId, roleName, roleInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * grant all privilege on object to role
     *
     * @param projectId projectId
     * @param roleName  roleName
     * @return CatalogResponse
     */
    @ApiOperation(value = "grant all privilege to role")
    @PatchMapping(value = "/{roleName}/grantAllPrivilege", produces = "application/json;charset=UTF-8")
    public BaseResponse grantAllPrivilegeOnObjectToRole(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "role name", required = true) @PathVariable("roleName") String roleName,
        @ApiParam(value = "role input body", required = true) @Valid @RequestBody RoleInput roleInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {

        return createBaseResponse(token, () -> {
            roleService.addAllPrivilegeOnObjectToRole(projectId, roleName, roleInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * revoke privilege from role
     *
     * @param projectId projectId
     * @param roleName  roleName
     * @return CatalogResponse
     */
    @UserLog(operation = "revoke privilege", objectType = ObjectType.ROLE)
    @ApiOperation(value = "revoke privilege from role")
    @PatchMapping(value = "/{roleName}/revokePrivilege", produces = "application/json;charset=UTF-8")
    public BaseResponse revokePrivilegeFromRole(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "role name", required = true) @PathVariable("roleName") String roleName,
        @ApiParam(value = "role input body", required = true) @Valid @RequestBody RoleInput roleInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            String logObjName = roleName + "." + roleInput.getOperation() + "." + roleInput.getObjectName();
            UserLogAop.setAuditLogObjectName(logObjName);
            roleService.removePrivilegeFromRole(projectId, roleName, roleInput);
            UserLogAop.setAuditLogDetail(logObjName + " revoke privilege success");
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * revoke all privilege on object from role
     *
     * @param projectId projectId
     * @param roleName  roleName
     * @return CatalogResponse
     */
    @ApiOperation(value = "revoke all privilege from role")
    @PatchMapping(value = "/{roleName}/revokeAllPrivilege", produces = "application/json;charset=UTF-8")
    public BaseResponse revokeAllPrivilegeOnObjectFromRole(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "role name", required = true) @PathVariable("roleName") String roleName,
        @ApiParam(value = "role input", required = true) @Valid @RequestBody RoleInput roleInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            roleService.removeAllPrivilegeOnObjectFromRole(projectId, roleName, roleInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * add users to role
     *
     * @param projectId projectId
     * @param roleName  roleName
     * @return CatalogResponse
     */
    @ApiOperation(value = "add users to role")
    @PatchMapping(value = "/{roleName}/addUsers", produces = "application/json;charset=UTF-8")
    public BaseResponse addUsersToRole(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "role name", required = true) @PathVariable("roleName") String roleName,
        @ApiParam(value = "role input body", required = true) @Valid @RequestBody RoleInput roleInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            roleService.addUserToRole(projectId, roleName, roleInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * remove users from role
     *
     * @param projectId projectId
     * @param roleName  roleName
     * @return CatalogResponse
     */
    @ApiOperation(value = "remove users from role")
    @PatchMapping(value = "/{roleName}/removeUsers", produces = "application/json;charset=UTF-8")
    public BaseResponse removeUsersFromRole(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "role name", required = true) @PathVariable("roleName") String roleName,
        @ApiParam(value = "role input body", required = true) @Valid @RequestBody RoleInput roleInput,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            roleService.removeUserFromRole(projectId, roleName, roleInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    /**
     * get Role by name
     *
     * @param projectId
     * @param roleName
     * @return CatalogResponse
     */
    @ApiOperation(value = "get role by name")
    @GetMapping(value = "", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Role> getRoleByName(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "role name", required = true) @RequestParam("roleName") String roleName,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            Role role = roleService.getRoleByName(projectId, roleName);
            return ResponseUtil.responseSuccess(role);
        });
    }

    /**
     * get role by Id
     *
     * @param projectId
     * @param roleId
     * @return CatalogResponse
     */
    @ApiOperation(value = "get role by id")
    @GetMapping(value = "/{roleId}", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Role> getRoleById(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "role id", required = true) @RequestParam("roleId") String roleId,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            Role role = roleService.getRoleById(projectId, roleId);
            return ResponseUtil.responseSuccess(role);
        });
    }

    /**
     * show roles
     *
     * @param projectId
     * @param pattern
     * @return CatalogResponse
     */
    @ApiOperation(value = "show roles")
    @GetMapping(value = "/showRoles",
        produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<Role>> showRoles(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "user id", required = false) @RequestParam(value = "userId", required = false) String userId,
        @ApiParam(value = "contain owner", required = false) @RequestParam(value = "containOwner", required = false, defaultValue = "true") boolean containOwner,
        @ApiParam(value = "name pattern") @RequestParam(value = "pattern", required = false)
            String pattern,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            PagedList<Role> result = new PagedList<>();
            List<Role> roles = roleService.getRoleModels(projectId, userId, pattern, containOwner);
            result.setObjects(roles.toArray(new Role[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }

    /**
     * show roles
     *
     * @param projectId
     * @return CatalogResponse
     */
    @ApiOperation(value = "show all roleName")
    @GetMapping(value = "/showAllRoleName",
            produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<Role>> showAllRoleName(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "name keyword") @RequestParam(value = "keyword", required = false, defaultValue = "")
                    String keyword,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            PagedList<Role> result = new PagedList<>();
            List<Role> roles = roleService.getRoleNames(projectId, keyword);
            result.setObjects(roles.toArray(new Role[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }

    /**
     * show all privileges by userId
     *
     * @param projectId
     * @return CatalogResponse
     */
    @ApiOperation(value = "Display permission object list via user")
    @GetMapping(value = "/showPermObjectsByUser",
            produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<String>> showPermObjectsByUser(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "userId") @RequestParam(value = "userId", required = true) String userId,
            @ApiParam(value = "objectType", example = "TABLE、DATABASE、CATALOG") @RequestParam(value = "objectType", required = true) String objectType,
            @ApiParam(value = "filter") @RequestParam(value = "filter", required = false) String filter,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            PagedList<String> result = new PagedList<>();
            List<String> roles = roleService.showPermObjectsByUser(projectId, userId, objectType, filter);
            result.setObjects(roles.toArray(new String[0]));
            return ResponseUtil.responseSuccess(result);
        });
    }

    @ApiOperation(value = "show role privileges")
    @PostMapping(value = "showRolePrivileges", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<Role>> showRolePrivileges(
            @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
            @ApiParam(value = "searchInput", required = false) @RequestBody ShowRolePrivilegesInput input,
            @ApiParam(value = "maxResults") @RequestParam(value = "maxResults", required = false, defaultValue = "10") Integer maxResults,
            @ApiParam(value = "pageToken") @RequestParam(value = "pageToken", required = false) String pageToken,
            @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, projectId, () -> {
            TraverseCursorResult<List<Role>> searchResult = roleService.showRolePrivileges(projectId,
                input, maxResults, pageToken);
            return ResponseUtil.responseSuccess(new PagedList<Role>().setResult(searchResult));
        });
    }

    @ApiOperation(value = "show Privilege roles")
    @PostMapping(value = "showPrivilegeRoles", produces = "application/json;charset=UTF-8")
    public CatalogResponse<PagedList<PrivilegeRoles>> showPrivilegeRoles(
        @ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "searchInput", required = false) @RequestBody ShowRolePrivilegesInput input,
        @ApiParam(value = "maxResults") @RequestParam(value = "maxResults", required = false, defaultValue = "10") Integer maxResults,
        @ApiParam(value = "pageToken") @RequestParam(value = "pageToken", required = false) String pageToken,
        @ApiParam(value = "authorization", required = true) @RequestHeader("Authorization") String token) {
        return createResponse(token, projectId, () -> {
            TraverseCursorResult<List<PrivilegeRoles>> searchResult = roleService.showPrivilegeRoles(projectId,
                input, maxResults, pageToken);
            return ResponseUtil.responseSuccess(new PagedList<PrivilegeRoles>().setResult(searchResult));
        });
    }
}