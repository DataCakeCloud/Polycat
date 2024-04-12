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
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.polycat.common.messaging.grpc.MessageClientGrpcImpl;
import io.polycat.common.messaging.grpc.MessageServerGrpcImpl;

import io.polycat.catalog.common.Address;
import io.polycat.catalog.common.GlobalConfig;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.exception.CarbonSqlException;
import io.polycat.common.messaging.grpc.generated.ServerGrpc;

import javax.net.ssl.SSLException;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class SSLGrpcMessagingFactory implements MessagingFactory {
    private static final Logger logger = Logger.getLogger(SSLGrpcMessagingFactory.class.getName());

    // 从环境变量获取ssl证书相关的路径
    private String cacertPath = System.getProperty("cacertPath"); //ca证书

    private String crtPath = System.getProperty("crtPath"); //客户端/服务器端证书

    private String keyPath = System.getProperty("keyPath"); //私钥路径

    private boolean clientAuth = Boolean.parseBoolean(System.getProperty("clientAuth", "false")); //是否双向加密

    public MessageClient createMessageClient(String serviceName, Address address) {
        ManagedChannel channel = null;
        try {
            channel = NettyChannelBuilder.forAddress(address.getHostname(), address.getPort())
                    .negotiationType(NegotiationType.TLS)
                    .overrideAuthority("tics")
                    .maxInboundMessageSize(GlobalConfig.getInt(GlobalConfig.RPC_MESSAGE_MAX_SIZE))
                    .sslContext(GrpcSslContexts.forClient()
                            .trustManager(new File(cacertPath))
                            .keyManager(new File(crtPath), new File(keyPath))
                            .sslProvider(SslProvider.OPENSSL)
                            .build())
                    .enableRetry()
                    .maxRetryAttempts(3)
                    .build();
            
            ServerGrpc.ServerBlockingStub stub = ServerGrpc.newBlockingStub(channel);
            logger.info("SSLGrpcMessagingFactory:" + serviceName + " client started, address:" + address.getHostname() + "|"
                    + address.getPort());
            return new MessageClientGrpcImpl(serviceName, stub, channel);
        } catch (SSLException e) {
            logger.error("create client failed ..", e);
            throw new CarbonSqlException("create client failed ..", e);
        }
    }

    public Service setupMessageService(String serviceName, Address address, MessageService messageService)
            throws IOException {
        Server server = NettyServerBuilder.forAddress(new InetSocketAddress(address.getPort()))
                .addService(new MessageServerGrpcImpl(messageService))
                .maxInboundMessageSize(GlobalConfig.getInt(GlobalConfig.RPC_MESSAGE_MAX_SIZE))
                .sslContext(GrpcSslContexts.forServer(new File(crtPath), new File(keyPath))
                        .trustManager(new File(cacertPath))
                        .clientAuth(clientAuth ? ClientAuth.REQUIRE : ClientAuth.NONE)
                        .sslProvider(SslProvider.OPENSSL)
                        .build())
                .keepAliveTime(15, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .permitKeepAliveWithoutCalls(true)
                .permitKeepAliveTime(15, TimeUnit.SECONDS)
                .build()
                .start();
        logger.info("SSLGrpcMessagingFactory:" + serviceName + " service started, listening on " + server.getPort());
        return new Service(serviceName, server);
    }

}
