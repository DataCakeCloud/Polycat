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
package io.polycat.catalog.server.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;

@Slf4j
@Configuration
public class AsyncStoreTPConfig {

    private static final String THREAD_POOL_NAME_PREFIX = "Async-handler-tp";
    private static final Integer THREAD_POOL_CORE_THREAD_SIZE = 1;
    private static final Integer THREAD_POOL_MAX_THREAD_SIZE = 4;
    private static final Integer THREAD_POOL_KEEP_ALIVE_TIME = 5;
    private static final Integer THREAD_POOL_QUEUE_CAPACITY = 100000;


    @Bean("asyncStoreTP")
    public ExecutorService createThreadPool() {
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat(THREAD_POOL_NAME_PREFIX + "-%d")
                .setDaemon(true).build();
        return new ThreadPoolExecutor(
                THREAD_POOL_CORE_THREAD_SIZE,
                THREAD_POOL_MAX_THREAD_SIZE,
                THREAD_POOL_KEEP_ALIVE_TIME,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(THREAD_POOL_QUEUE_CAPACITY),
                factory,
                new LogPrintAbortPolicy());
    }


    private static class LogPrintAbortPolicy implements RejectedExecutionHandler {

        LogPrintAbortPolicy() {
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (r instanceof MsgRunnable) {
                log.error("RejectedExecutionHandler LogPrintAbortPolicy handle message: {}", ((MsgRunnable) r).getMsgObj());
            }
        }
    }

    public static abstract class MsgRunnable implements Runnable {

        public Object msgObj;

        public Object getMsgObj() {
            return msgObj;
        }

        public void setMsgObj(Object msgObj) {
            this.msgObj = msgObj;
        }
    }
}
