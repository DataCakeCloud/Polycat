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
package io.polycat.catalog.common;


import io.polycat.catalog.common.ErrorCode;

public class MetaStoreException extends RuntimeException {

    private ErrorCode errorCode;

    /**
     * constructed function
     *
     * @param errorCode errorCode
     */
    public MetaStoreException(ErrorCode errorCode) {
        this(errorCode.buildErrorMessage());
        this.errorCode = errorCode;
    }

    /**
     * constructed function
     */
    public MetaStoreException() {
        super();
    }

    /**
     * constructed function
     *
     * @param message message
     */
    public MetaStoreException(String message) {
        super(message);
    }

    public MetaStoreException(String message, ErrorCode errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * @param message message
     * @param cause   cause
     */
    public MetaStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * constructed function
     *
     * @param cause cause
     */
    public MetaStoreException(Throwable cause) {
        super(cause);
    }

    /**
     * constructed function
     *
     * @param message            message
     * @param cause              cause
     * @param enableSuppression  enableSuppression
     * @param writableStackTrace writableStackTrace
     */
    protected MetaStoreException(String message, Throwable cause, boolean enableSuppression,
        boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public MetaStoreException(ErrorCode errorCode, Object... args) {
        super(errorCode.buildErrorMessage(args));
        this.errorCode = errorCode;
    }


    /**
     * constructed function
     *
     * @param errorCode          errorCode
     * @param cause              cause
     */
    protected MetaStoreException(ErrorCode errorCode, Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
