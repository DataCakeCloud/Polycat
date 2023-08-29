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
package io.polycat.common.messaging.grpc;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import io.polycat.catalog.common.GlobalConfig;
import io.polycat.catalog.common.serialization.Writable;
import io.polycat.common.messaging.Failure;
import io.polycat.common.messaging.Message;
import io.polycat.common.messaging.MessageClient;
import io.polycat.common.messaging.RemoteCallFailureException;
import io.polycat.common.messaging.RemoteCallTimeoutException;
import io.polycat.common.messaging.grpc.generated.Request;
import io.polycat.common.messaging.grpc.generated.Response;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.polycat.common.messaging.grpc.generated.ServerGrpc;

/**
 * GRPC客户端实现
 */
public class MessageClientGrpcImpl implements MessageClient {

    private final String serviceName;

    private final ServerGrpc.ServerBlockingStub stub;

    private final ManagedChannel channel;

    public MessageClientGrpcImpl(String serviceName, ServerGrpc.ServerBlockingStub stub, ManagedChannel channel) {
        this.serviceName = serviceName;
        this.stub = stub;
        this.channel = channel;
    }

    @Override
    public String getName() {
        return serviceName;
    }

    @Override
    public Message invoke(Message in) {
        return invoke(in, GlobalConfig.getInt(GlobalConfig.RPC_CONTROL_FLOW_TIMEOUT));
    }

    @Override
    public Message invoke(Message in, int timeoutSeconds) {
        Request request = Request.newBuilder()
                .setData(ByteString.copyFrom(Writable.serializeWithClassName(in)))
                .build();
        Response rsp;
        try {
            rsp = stub.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS).invoke(request);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Status.DEADLINE_EXCEEDED.getCode())) {
                throw new RemoteCallTimeoutException("timeout for " + in.toString(), e);
            } else {
                throw e;
            }
        }
        Message response = Writable.deserializeWithClassName(rsp.getData().toByteArray());
        if (response instanceof Failure) {
            throw new RemoteCallFailureException((Failure) response);
        } else {
            return response;
        }
    }

    @Override
    public Iterator<Message> invokeStream(Message in) throws IOException, RemoteCallFailureException {
        return invokeStream(in, GlobalConfig.getInt(GlobalConfig.RPC_CONTROL_FLOW_TIMEOUT));
    }

    @Override
    public Iterator<Message> invokeStream(Message in, int timeoutSeconds)
            throws IOException, RemoteCallFailureException, RemoteCallTimeoutException {
        return new Iterator<Message>() {
            Iterator<Response> rsp;
            @Override
            public boolean hasNext() {
                if (rsp == null) {
                    fetchResult();
                }
                return rsp.hasNext();
            }

            private void fetchResult() {
                Request request = Request.newBuilder()
                        .setData(ByteString.copyFrom(Writable.serializeWithClassName(in)))
                        .build();
                try {
                    rsp = stub.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS).invokeStream(request);
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode().equals(Status.DEADLINE_EXCEEDED.getCode())) {
                        throw new RemoteCallTimeoutException("timeout for " + in.toString(), e);
                    } else {
                        throw e;
                    }
                }
            }

            @Override
            public Message next() {
                Message response = Writable.deserializeWithClassName(rsp.next().getData().toByteArray());
                if (response instanceof Failure) {
                    throw new RemoteCallFailureException((Failure) response);
                } else {
                    return response;
                }
            }
        };
    }

    @Override
    public void notify(Message message) {
        Request request = Request.newBuilder()
                .setData(ByteString.copyFrom(Writable.serializeWithClassName(message)))
                .build();
        stub.notify(request);
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            channel.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
