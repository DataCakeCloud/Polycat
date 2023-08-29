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
import io.polycat.catalog.common.model.BaseResponse;

import org.springframework.http.HttpStatus;

@SuppressWarnings("unchecked")
public class BaseResponseUtil {

    public static BaseResponse responseWithException(CatalogServerException ex) {
        ErrorCode errorCode = ex.getErrorCode();
        return new BaseResponse(errorCode.getStatusCode().value(), errorCode.getErrorCode(), ex.getMessage());
    }

    public static BaseResponse responseWithException(MetaStoreException ex) {
        ErrorCode errorCode = ex.getErrorCode();
        return new BaseResponse(errorCode.getStatusCode().value(), errorCode.getErrorCode(), ex.getMessage());
    }

    public static BaseResponse responseWithError(Exception ex) {
        return new BaseResponse(ErrorCode.INNER_SERVER_ERROR.getStatusCode().value(), ex.getMessage());
    }

    public static BaseResponse responseFromErrorCode(final ErrorCode code) {
        return new BaseResponse(code.getStatusCode().value(), code.getErrorCode(),
            code.getStatusCode().getReasonPhrase());
    }

    public static BaseResponse responseSuccess() {
        return new BaseResponse(HttpStatus.OK.value(), HttpStatus.OK.getReasonPhrase());
    }

    public static BaseResponse responseSuccess(HttpStatus httpStatus) {
        return new BaseResponse(httpStatus.value(), httpStatus.getReasonPhrase());
    }

}
