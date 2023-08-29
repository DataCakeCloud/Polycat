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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.polycat.catalog.common.Address;
import io.polycat.catalog.common.Logger;
import io.polycat.common.messaging.grpc.MessageClientGrpcImpl;
import io.polycat.common.messaging.grpc.MessageServerGrpcImpl;
import io.polycat.common.messaging.grpc.generated.ServerGrpc;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 一个基于GRPC实现的消息系统
 */
public class GrpcMessagingFactory implements MessagingFactory {

    private static final Logger logger = Logger.getLogger(GrpcMessagingFactory.class.getName());

    /**
     * 创建消息服务的客户端
     *
     * @param serviceName 服务名，用于日志打印
     * @param address     服务所在地址和端口
     * @return 客户端
     */
    public MessageClient createMessageClient(String serviceName, Address address) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(address.getHostname(), address.getPort())
                .keepAliveTime(15, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .enableRetry()
                .maxRetryAttempts(3)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build();
        ServerGrpc.ServerBlockingStub stub = ServerGrpc.newBlockingStub(channel);
        return new MessageClientGrpcImpl(serviceName, stub, channel);
    }

    /**
     * 创建消息服务的服务端
     *
     * @param serviceName    服务名，用于日志打印
     * @param address        服务地址和端口
     * @param messageService 消息处理实体
     * @return 服务端
     * @throws IOException 网络失败
     */
    public Service setupMessageService(String serviceName, Address address, MessageService messageService)
            throws IOException {
        Server server = NettyServerBuilder.forPort(address.getPort())
                .keepAliveTime(15, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .permitKeepAliveWithoutCalls(true)
                .permitKeepAliveTime(15, TimeUnit.SECONDS)
                .addService(new MessageServerGrpcImpl(messageService))
                .build()
                .start();
        logger.info(serviceName + " service started, listening on " + server.getPort());
        return new Service(serviceName, server);
    }

}
