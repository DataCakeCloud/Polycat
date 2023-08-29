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

import io.polycat.catalog.common.Address;

import java.io.IOException;

/**
 * 消息系统
 */
public interface MessagingFactory {
    /**
     * 创建消息服务的客户端
     *
     * @param serviceName 服务名，用于日志打印
     * @param address     服务所在地址和端口
     * @return 客户端
     */
    MessageClient createMessageClient(String serviceName, Address address);

    /**
     * 创建消息服务的服务端
     *
     * @param serviceName    服务名，用于日志打印
     * @param address        服务地址和端口
     * @param messageService 消息处理实体
     * @return 服务端
     * @throws IOException 网络失败
     */
    Service setupMessageService(String serviceName, Address address, MessageService messageService) throws IOException;
}
