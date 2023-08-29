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
package io.polycat.catalog.client.Exception;

import io.polycat.catalog.common.ErrorCode;

import org.slf4j.helpers.MessageFormatter;

public enum ClientErrorCode {
    INNER_ERROR("dash.10001", "inner error", 0),
    RESOURCE_FORMAT_ERROR("dash.10002", "resource {} name is illegal", 1),
    RESOURCE_LENGTH_ERROR("dash.10003", "resource {} length is illegal", 1),
    PARAMETER_FORMAT_ERROR("dash.10004", "parameter {} name is illegal", 1),
    PARAMETER_LENGTH_ERROR("dash.10005", "parameter {} length is illegal", 1);
    private final String errorCode;

    private final String messageFormat;

    // number of arguments in message, for validation
    private final int argsCount;

    ClientErrorCode(String errorCode, String messageFormat, int argsCount) {
        this.errorCode = errorCode;
        this.messageFormat = messageFormat;
        this.argsCount = argsCount;
    }

    public String getErrorCode() {
        return errorCode;
    }


    public String buildErrorMessage() {
        if (argsCount == 0) {
            return messageFormat;
        } else {
            return INNER_ERROR.messageFormat;
        }
    }

    public String buildErrorMessage(Object[] args) {
        if (args.length != argsCount) {
            // 参数个数不一致，则不做格式化，只返回描述信息
            return messageFormat;
        }
        return MessageFormatter.format(messageFormat, args).getMessage();
    }

}
