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
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.model.glossary.Category;
import io.polycat.catalog.common.model.glossary.Glossary;
import io.polycat.catalog.common.plugin.request.input.CategoryInput;
import io.polycat.catalog.common.plugin.request.input.GlossaryInput;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.service.api.GlossaryService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
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

import java.util.List;

/**
 * @author liangyouze
 * @date 2023/12/1
 */

@Api(tags = "glossary api")
@ApiResponses(value = {
    @ApiResponse(code = 400, message = "Bad Request"),
    @ApiResponse(code = 408, message = "Request Timeout"),
    @ApiResponse(code = 500, message = "Internal Server Error")
})
@RestController
@RequestMapping("/v1/{project-id}/glossary")
@Validated
public class GlossaryController extends BaseController {

    @Autowired
    GlossaryService glossaryService;

    @ApiOperation(value = "create glossary")
    @PostMapping(value = "createGlossary", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Glossary> createGlossary(@ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "glossary input", required = true) @RequestBody(required = true) GlossaryInput glossaryInput,
        @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createResponse(token, () ->
                ResponseUtil.responseSuccess(glossaryService.createGlossary(projectId, glossaryInput)));
    }

    @ApiOperation(value = "delete glossary")
    @DeleteMapping(value = "deleteGlossary", produces = "application/json;charset=UTF-8")
    public BaseResponse deleteGlossary(@ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "glossary id", required = true) @RequestParam(value = "id", required = true) Integer id,
        @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            glossaryService.deleteGlossary(projectId, id);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "create category")
    @PostMapping(value = "createCategory", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Category> createCategory(@ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "category input", required = true) @RequestBody(required = true) CategoryInput categoryInput,
        @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createResponse(token, () ->
            ResponseUtil.responseSuccess(glossaryService.createCategory(projectId, categoryInput)));
    }

    @ApiOperation(value = "alter category")
    @PostMapping(value = "alterCategory", produces = "application/json;charset=UTF-8")
    public BaseResponse alterCategory(@ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "category id", required = true) @RequestParam(value = "id", required = true) Integer id,
        @ApiParam(value = "categoryInput", required = true) @RequestBody(required = true) CategoryInput categoryInput,
        @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            glossaryService.alterCategory(projectId, id, categoryInput);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "delete category")
    @DeleteMapping(value = "deleteCategory", produces = "application/json;charset=UTF-8")
    public BaseResponse deleteCategory(@ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "category id", required = true) @RequestParam(value = "id", required = true) Integer id,
        @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            glossaryService.deleteCategory(projectId, id);
            return BaseResponseUtil.responseSuccess();
        });
    }

    @ApiOperation(value = "get category")
    @GetMapping(value = "getCategory", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Category> getCategory(@ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "category id", required = true) @RequestParam(value = "id", required = false) Integer id,
        @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {

            return ResponseUtil.responseSuccess(glossaryService.getCategory(projectId, id));
        });
    }


    @ApiOperation(value = "list glossary without category")
    @GetMapping(value = "listGlossaryWithoutCategory", produces = "application/json;charset=UTF-8")
    public CatalogResponse<List<Glossary>> listGlossaryWithoutCategory(@ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> {
            return ResponseUtil.responseSuccess(glossaryService.listGlossaryWithoutCategory(projectId));
        });
    }

    @ApiOperation(value = "get glossary")
    @GetMapping(value = "getGlossary", produces = "application/json;charset=UTF-8")
    public CatalogResponse<Glossary> getGlossary(@ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "glossary id", required = false) @RequestParam(value = "id", required = false) Integer id,
        @ApiParam(value = "glossary name", required = false) @RequestParam(value = "name", required = false) String name,
        @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createResponse(token, () -> ResponseUtil.responseSuccess(glossaryService.getGlossary(projectId, id, name)));
    }

    @ApiOperation(value = "alter glossary")
    @PatchMapping(value = "alterGlossary", produces = "application/json;charset=UTF-8")
    public BaseResponse alterGlossary(@ApiParam(value = "project id", required = true) @PathVariable("project-id") String projectId,
        @ApiParam(value = "id", required = true) @RequestParam(value = "id", required = true) Integer id,
        @ApiParam(value = "glossary input", required = true) @RequestBody(required = true) GlossaryInput glossaryInput,
        @ApiParam(value = "authorization") @RequestHeader("Authorization") String token) {
        return createBaseResponse(token, () -> {
            glossaryService.alterGlossary(projectId, id, glossaryInput);
            return BaseResponseUtil.responseSuccess();
        });
    }
}
