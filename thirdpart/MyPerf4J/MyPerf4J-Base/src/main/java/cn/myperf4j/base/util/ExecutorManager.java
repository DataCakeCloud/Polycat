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
package cn.myperf4j.base.util;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by LinShunkang on 2018/8/23
 * <p>
 * 该类用于统一管理ExecutorService，便于统一关闭线程池
 */
public final class ExecutorManager {

    private static final Set<ExecutorService> executors = new HashSet<>();

    private ExecutorManager() {
        //empty
    }

    public static void addExecutorService(ExecutorService executor) {
        executors.add(executor);
    }

    public static void stopAll(long timeout, TimeUnit unit) {
        for (ExecutorService executorService : executors) {
            try {
                executorService.shutdown();
                executorService.awaitTermination(timeout, unit);
            } catch (InterruptedException e) {
                Logger.error("ExecutorManager.stopAll()", e);
            }
        }
    }
}
