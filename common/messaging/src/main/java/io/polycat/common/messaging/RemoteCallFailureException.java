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
package io.polycat.common.messaging;

/**
 * 当{@link MessageClient#invoke}调用时，Server处理消息发生异常时，Client会抛出此异常
 */
public class RemoteCallFailureException extends RuntimeException {

    private Failure failure;

    public RemoteCallFailureException(Failure failure) {
        this.failure = failure;
    }

    @Override
    public String getMessage() {
        return String.format("%s in %s: %s", failure.getExceptionClass(), failure.getServiceObjectClass(),
                failure.getMessage());
    }
}
