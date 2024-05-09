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

/**
 * @author liangyouze
 * @date 2024/2/2
 */
public class NoSuchObjectException extends CatalogException{

    public NoSuchObjectException(Throwable cause) {
        super(cause);
    }

    public NoSuchObjectException(String message) {
        super(String.format("Object: %s not found.", message));
    }

    public NoSuchObjectException(Throwable cause, int statusCode) {
        super(cause, statusCode);
    }

    public NoSuchObjectException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoSuchObjectException(String message, Throwable cause, int statusCode) {
        super(message, cause, statusCode);
    }
}
