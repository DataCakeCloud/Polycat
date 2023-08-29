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
package io.polycat.catalog.server.common;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import static io.polycat.catalog.common.Constants.KEY_ERRORS;
import static io.polycat.catalog.common.Constants.KEY_ERROR_CODE;
import static io.polycat.catalog.common.Constants.KEY_MESSAGE;

public class ErrorResponse {

    private String errorCode;
    private Object errorMsg;

    private ErrorResponse() {
    }

    /**
     * Instantiates a new Error response.
     *
     * @param errorCode the error code
     * @param errorMsg  the error msg
     */
    public ErrorResponse(final String errorCode, final Object errorMsg) {
        this.errorCode = errorCode;
        this.errorMsg = errorMsg;
    }

    /**
     * Gets error code.
     *
     * @return the error code
     */
    public String getErrorCode() {
        return this.errorCode;
    }

    /**
     * Sets error code.
     *
     * @param errorCode the error code
     */
    private void setErrorCode(final String errorCode) {
        this.errorCode = errorCode;
    }

    /**
     * Gets error msg.
     *
     * @return the error msg
     */
    public Object getErrorMsg() {
        return this.errorMsg;
    }

    /**
     * Sets error msg.
     *
     * @param message the message
     */
    public void setErrorMsg(final Object message) {
        this.errorMsg = message;
    }

    /**
     * Gets code.
     *
     * @return the code
     */
    public int getCode() {
        return Integer.parseInt(this.errorCode);
    }

    /**
     * Gets error map.
     *
     * @return the error map
     */
    public Map<String, Object> getErrorMap() {
        final Map<String, Object> returnMap = new LinkedHashMap<>();
        final Collection<Map<String, Object>> errorList = new LinkedList<>();
        final Map<String, Object> errorMap = new LinkedHashMap<>();
        errorMap.put(KEY_ERROR_CODE, this.errorCode);
        errorMap.put(KEY_MESSAGE, this.errorMsg);
        errorList.add(errorMap);
        returnMap.put(KEY_ERRORS, errorList);
        return returnMap;
    }
}
