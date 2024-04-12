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
package io.polycat.catalog.common.exception;

import io.polycat.catalog.common.ErrorCode;

/**
 * @singe 2020/12/25
 */
public class ExceptionUtil {
    public static final int MAX_ERROR_MESSAGE_COUNT = 1024;

    private ExceptionUtil() {
    }

    // 获取排除掉异常名的异常信息
    public static String getMessageExcludeException(Throwable e) {
        Throwable rootException = e;
        while (rootException.getCause() != null) {
            rootException = rootException.getCause();
        }
        String message = rootException.getMessage();
        if (message == null) {
            message = e.toString();
        } else if (message.length() > MAX_ERROR_MESSAGE_COUNT) {
            message = message.substring(0, MAX_ERROR_MESSAGE_COUNT) + "...";
        }
        return message;
    }

    public static void throwInvalidOperationException(CatalogException e) throws InvalidOperationException {
        if (e.getMessage() == null) {
            return;
        }
        if (e.getMessage().contains(ErrorCode.DATABASE_TABLE_EXISTS.getErrorCode())) {
            throw new InvalidOperationException(e);
        }
    }

    public static void throwNoSuchObjectException(CatalogException e) throws NoSuchObjectException {
        if (e.getMessage() == null) {
            return;
        }
        if (e.getMessage().contains(ErrorCode.TABLE_NOT_FOUND.getErrorCode())) {
            throw new NoSuchObjectException(e);
        }
        if (e.getMessage().contains(ErrorCode.DATABASE_NOT_FOUND.getErrorCode())) {
            throw new NoSuchObjectException(e);
        }
        if (e.getMessage().contains(ErrorCode.CATALOG_NOT_FOUND.getErrorCode())) {
            throw new NoSuchObjectException(e);
        }
    }

    public static void throwAlreadyExistsException(CatalogException e) throws AlreadyExistsException {
        if (e.getMessage() == null) {
            return;
        }
        if (e.getMessage().contains(ErrorCode.TABLE_ALREADY_EXIST.getErrorCode())) {
            throw new AlreadyExistsException(e);
        }
        if (e.getMessage().contains(ErrorCode.DATABASE_ALREADY_EXIST.getErrorCode())) {
            throw new AlreadyExistsException(e);
        }
        if (e.getMessage().contains(ErrorCode.CATALOG_ALREADY_EXIST.getErrorCode())) {
            throw new AlreadyExistsException(e);
        }
    }
}
