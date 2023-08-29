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

import java.util.concurrent.TimeUnit;

import io.polycat.catalog.common.Logger;

import io.grpc.Server;

/**
 * 对外提供服务的RPC服务
 */
public class Service {
    private static final Logger logger = Logger.getLogger(Service.class.getName());

    private final String name;

    private final io.grpc.Server server;

    public Service(String name, Server server) {
        this.name = name;
        this.server = server;
        addShutdownHook(server);
    }

    private static void addShutdownHook(Server server) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            try {
                server.awaitTermination();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    public void close() {
        try {
            logger.info(String.format("shutting down service %s", name));
            boolean down = server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            if (!down) {
                throw new RuntimeException("shutdown return false");
            }
            logger.info(String.format("service %s shutted down", name));
        } catch (InterruptedException e) {
            logger.warn("Interrupted exception during gRPC channel close", e);
            Thread.currentThread().interrupt();
        }
    }

    public String getName() {
        return name;
    }
}
