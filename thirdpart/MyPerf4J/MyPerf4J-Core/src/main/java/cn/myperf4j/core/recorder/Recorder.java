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
package cn.myperf4j.core.recorder;

import cn.myperf4j.base.buffer.IntBuf;

/**
 * Created by LinShunkang on 2018/3/13
 */
public abstract class Recorder {

    private final int methodTagId;

    public Recorder(int methodTagId) {
        this.methodTagId = methodTagId;
    }

    public int getMethodTagId() {
        return methodTagId;
    }

    public abstract boolean hasRecord();

    public abstract void recordTime(long startNanoTime, long endNanoTime);

    /**
     * 为了节省内存的使用，利用 IntBuf 作为返回结果
     *
     * @param intBuf : intBuf.capacity 为 effectiveRecordCount 的两倍!!!
     *               其中：
     *               第 0 位存 timeCost，第 1 位存 count，
     *               第 2 位存 timeCost，第 3 位存 count，
     *               以此类推
     * @return 总请求数
     */
    public abstract long fillSortedRecords(IntBuf intBuf);

    /**
     * 获取有效的记录的个数
     */
    public abstract int getDiffCount();

    public abstract void resetRecord();

}
