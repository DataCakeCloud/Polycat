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

import java.io.IOException;
import java.util.Iterator;

/**
 * 消息服务Server端接口，处理外部请求，返回响应消息。
 * 使用{@link MessagingFactory#setupMessageService(String, Address, MessageService)}创建服务端
 * 使用{@link MessagingFactory#createMessageClient(String, Address)}来创建对应的客户端
 */
public interface MessageService {
    Message invoke(Message message) throws IOException;

    Iterator<Message> invokeStream(Message message) throws IOException;

    default void notify(Message message) throws IOException {
    }

    String name();
}
