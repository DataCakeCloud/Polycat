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
package io.polycat.catalog.server.util;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.server.common.ErrorResponse;

import org.springframework.http.HttpStatus;

@SuppressWarnings("unchecked")
public class ResponseUtil {

    public static CatalogResponse responseWithException(CatalogServerException ex) {
        ErrorCode errorCode = ex.getErrorCode();
        return new CatalogResponse(errorCode.getStatusCode().value(), errorCode.getErrorCode(), ex.getMessage());
    }

    public static CatalogResponse responseWithException(MetaStoreException ex) {
        ErrorCode errorCode = ex.getErrorCode();
        return new CatalogResponse(errorCode.getStatusCode().value(), errorCode.getErrorCode(), ex.getMessage());
    }

    public static CatalogResponse responseWithError(Exception ex) {
        return new CatalogResponse(ErrorCode.INNER_SERVER_ERROR.getStatusCode().value(), ex.getMessage());
    }

    /**
     * Response from error code response entity.
     *
     * @param code the code
     * @return the response entity
     */
    public static CatalogResponse responseFromErrorCode(final ErrorCode code) {
        final ErrorResponse rsp = new ErrorResponse(String.valueOf(code.getErrorCode()),
                code.buildErrorMessage());
        return new CatalogResponse(rsp.getErrorMap(), code.getStatusCode().value(),
            code.getStatusCode().getReasonPhrase());
    }

    public static CatalogResponse responseFromErrorCode(final ErrorCode code, String errorMessage) {
        final ErrorResponse rsp = new ErrorResponse(String.valueOf(code.getErrorCode()),
            errorMessage);
        return new CatalogResponse(code.getStatusCode().value(), errorMessage);
    }

    public static CatalogResponse responseSuccess(final Object body) {
        return new CatalogResponse(body, HttpStatus.OK.value(), HttpStatus.OK.getReasonPhrase());
    }

    public static CatalogResponse responseSuccess(final Object body, HttpStatus statusCode) {
        return new CatalogResponse(body, statusCode.value(), statusCode.getReasonPhrase());
    }

}
