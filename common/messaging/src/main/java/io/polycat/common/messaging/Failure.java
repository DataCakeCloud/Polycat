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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Failure extends Message {
    private String serviceObjectClass;

    private String message;

    private String exceptionClass;

    public Failure() {
    }

    public Failure(MessageService messageService, Throwable t) {
        this.serviceObjectClass = messageService.getClass().getSimpleName();
        this.exceptionClass = t.getClass().getSimpleName();
        this.message = t.getMessage();
    }

    public String getExceptionClass() {
        return exceptionClass;
    }

    public String getMessage() {
        return message;
    }

    public String getServiceObjectClass() {
        return serviceObjectClass;
    }

    @Override
    public void writeMessage(DataOutput out) throws IOException {
        out.writeBoolean(serviceObjectClass != null);
        if (serviceObjectClass != null) {
            out.writeUTF(serviceObjectClass);
        }
        out.writeBoolean(message != null);
        if (message != null) {
            out.writeUTF(message);
        }
        out.writeBoolean(exceptionClass != null);
        if (exceptionClass != null) {
            out.writeUTF(exceptionClass);
        }
    }

    @Override
    public void readMessage(DataInput in) throws IOException {
        if (in.readBoolean()) {
            serviceObjectClass = in.readUTF();
        }
        if (in.readBoolean()) {
            message = in.readUTF();
        }
        if (in.readBoolean()) {
            exceptionClass = in.readUTF();
        }
    }
}
