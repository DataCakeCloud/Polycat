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

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.serialization.Writable;
import io.polycat.common.messaging.Failure;
import io.polycat.common.messaging.Message;
import io.polycat.common.messaging.MessageService;
import io.polycat.common.messaging.grpc.generated.NotifyResponse;
import io.polycat.common.messaging.grpc.generated.Request;
import io.polycat.common.messaging.grpc.generated.Response;
import io.polycat.common.messaging.grpc.generated.ServerGrpc;

import java.util.Iterator;

/**
 * GRPC服务端实现
 */
public class MessageServerGrpcImpl extends ServerGrpc.ServerImplBase {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    private final MessageService service;

    public MessageServerGrpcImpl(MessageService service) {
        this.service = service;
    }

    @Override
    public void invoke(Request request, StreamObserver<Response> responseObserver) {
        Response response = null;
        try {
            Message in = Writable.deserializeWithClassName(request.getData().toByteArray());
            logger.debug("got " + in.getClass().getSimpleName() + " for " + service.toString());
            Message out = service.invoke(in);
            if (out == null) {
                logger.info(service.getClass().getSimpleName() + " returning null result, ignoring it");
            } else {
                response = Response.newBuilder()
                        .setData(ByteString.copyFrom(Writable.serializeWithClassName(out)))
                        .build();
                responseObserver.onNext(response);
            }
        } catch (Exception e) {
            logger.error(String.format("exception %s in %s service: %s", e.getClass().getSimpleName(),
                    service.getClass().getSimpleName(), e.getMessage()));
            logger.error(e);
            Failure failure = new Failure(service, e);
            response = Response.newBuilder()
                    .setData(ByteString.copyFrom(Writable.serializeWithClassName(failure)))
                    .build();
            responseObserver.onNext(response);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void invokeStream(Request request, StreamObserver<Response> responseObserver) {
        Response response = null;
        try {
            Message in = Writable.deserializeWithClassName(request.getData().toByteArray());
            logger.debug("got " + in.getClass().getSimpleName() + " for " + service.toString());
            Iterator<Message> out = service.invokeStream(in);
            while (out.hasNext()) {
                Message next = out.next();
                if (next == null) {
                    logger.info(service.getClass().getSimpleName() + " returning null result, ignoring it");
                    break;
                } else {
                    response = Response.newBuilder()
                            .setData(ByteString.copyFrom(Writable.serializeWithClassName(next)))
                            .build();
                    responseObserver.onNext(response);
                }
            }
        } catch (Exception e) {
            logger.error(String.format("exception %s in %s service: %s", e.getClass().getSimpleName(),
                    service.getClass().getSimpleName(), e.getMessage()));
            logger.error(e);
            Failure failure = new Failure(service, e);
            response = Response.newBuilder()
                    .setData(ByteString.copyFrom(Writable.serializeWithClassName(failure)))
                    .build();
            responseObserver.onNext(response);
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void notify(Request request, StreamObserver<NotifyResponse> responseObserver) {
        try {
            Message in = Writable.deserializeWithClassName(request.getData().toByteArray());
            logger.debug("got " + in.getClass().getSimpleName() + " for " + service.toString());
            service.notify(in);
        } catch (Exception e) {
            logger.error(String.format("exception %s in %s service: %s", e.getClass().getSimpleName(),
                    service.getClass().getSimpleName(), e.getMessage()));
            logger.error(e);
        }
        responseObserver.onNext(NotifyResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
