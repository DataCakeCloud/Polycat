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
 * 消息服务Client端接口。
 * 使用{@link MeF#setupMessageService(String, Address, MessageService)}创建服务端
 * 使用{@link MessagingFactory#createMessageClient(String, Address)}来创建对应的客户端
 */
public interface MessageClient {

    /**
     * 返回服务名称，用于打印日志
     *
     * @return 服务名称
     */
    String getName();

    /**
     * 发送请求消息，返回响应消息。超时时间见{@link GlobalConfig#RPC_TIMEOUT}
     *
     * @param in 请求消息
     * @return 响应消息
     * @throws IOException                网络失败
     * @throws RemoteCallFailureException 对方处理请求消息时发生异常
     */
    Message invoke(Message in) throws IOException, RemoteCallFailureException;

    /**
     * 发送请消息，返回消息消息，指定超时时间。
     *
     * @param in             请求消息
     * @param timeoutSeconds 超时时间，单位为秒
     * @return 响应消息
     * @throws IOException                网络失败
     * @throws RemoteCallFailureException 对方处理请求消息时发生异常
     * @throws RemoteCallTimeoutException 等待响应消息超时
     */
    Message invoke(Message in, int timeoutSeconds)
            throws IOException, RemoteCallFailureException, RemoteCallTimeoutException;

    Iterator<Message> invokeStream(Message in) throws IOException, RemoteCallFailureException;

    Iterator<Message> invokeStream(Message in, int timeoutSeconds)
            throws IOException, RemoteCallFailureException, RemoteCallTimeoutException;

    void notify(Message in) throws IOException;

    void close();
}
