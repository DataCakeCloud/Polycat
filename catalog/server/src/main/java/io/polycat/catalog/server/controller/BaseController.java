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

import java.util.function.Supplier;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.BaseResponse;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.server.util.BaseResponseUtil;
import io.polycat.catalog.server.util.ResponseUtil;

import io.polycat.catalog.service.api.PrivilegeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.servlet.handler.SimpleMappingExceptionResolver;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public abstract class BaseController<T> extends SimpleMappingExceptionResolver {

    private static final Logger logger = Logger.getLogger(BaseController.class);

    //private static final Marker FATAL_MARKER = MarkerFactory.getMarker("FATAL");

    @Autowired
    private PrivilegeService privilegeService;

    protected CatalogResponse<T> createResponse(String token, Supplier<CatalogResponse<T>> supplier) {
        try {
            boolean valid = privilegeService.authenticationToken(token);
            if (!valid) {
                return ResponseUtil.responseWithException(
                        new CatalogServerException(ErrorCode.AUTHORIZATION_TYPE_ERROR));
            }

            return supplier.get();
        } catch (CatalogServerException e) {
            return exceptionResponse(e);
        } catch (MetaStoreException e) {
            return exceptionResponse(e);
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

    private ServletRequestAttributes getRequestAttributes() {
        return (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
    }

    protected CatalogResponse exceptionResponse(MetaStoreException e) {
        if (e.getErrorCode() == ErrorCode.INNER_SERVER_ERROR) {
            return errorResponse(e);
        } else {
            logger.warn(e);
            //applyStatusCodeFromCustom(e);
            return ResponseUtil.responseWithException(e);
        }
    }

    private void applyStatusCodeFromCustom(MetaStoreException e) {
        ServletRequestAttributes servletRequestAttributes = getRequestAttributes();
        applyStatusCodeIfPossible(servletRequestAttributes.getRequest(),
                servletRequestAttributes.getResponse(), e.getErrorCode().getStatusCode().value());
    }

    protected CatalogResponse errorResponse(Exception e) {
        ServletRequestAttributes servletRequestAttributes = getRequestAttributes();
        applyStatusCodeIfPossible(servletRequestAttributes.getRequest(),
                servletRequestAttributes.getResponse(),
                ErrorCode.INNER_SERVER_ERROR.getStatusCode().value());
        logger.error(e);
        return ResponseUtil.responseWithError(e);
    }

    protected BaseResponse createBaseResponse(String token, Supplier<BaseResponse> supplier) {
        try {
            boolean valid = privilegeService.authenticationToken(token);
            if (!valid) {
                return BaseResponseUtil.responseWithException(
                        new CatalogServerException(ErrorCode.AUTHORIZATION_TYPE_ERROR));
            }

            return supplier.get();
        } catch (CatalogServerException e) {
            return exceptionResponse(e);
        } catch (MetaStoreException e) {
            return exceptionResponse(e);
        } catch (Exception e) {
            return errorResponse(e);
        }
    }

}
