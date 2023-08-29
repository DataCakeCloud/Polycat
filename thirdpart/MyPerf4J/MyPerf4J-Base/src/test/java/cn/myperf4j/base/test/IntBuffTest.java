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
package cn.myperf4j.base.test;

import cn.myperf4j.base.buffer.IntBuf;
import cn.myperf4j.base.buffer.IntBufPool;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by LinShunkang on 2019/06/16
 */
public class IntBuffTest {

    @Test
    public void testIntBuf() {
        IntBuf intBuf = new IntBuf(100);
        Assert.assertEquals(intBuf.capacity(), 100);

        intBuf.write(1);
        intBuf.write(2);
        Assert.assertEquals(intBuf.writerIndex(), 2);

        intBuf.write(3);
        Assert.assertEquals(intBuf.writerIndex(), 3);

        Assert.assertEquals(intBuf.getInt(0), 1);
        Assert.assertEquals(intBuf.getInt(1), 2);
        Assert.assertEquals(intBuf.getInt(2), 3);

        intBuf.setInt(0, 0);
        Assert.assertEquals(intBuf.getInt(0), 0);
        Assert.assertEquals(intBuf.writerIndex(), 3);
    }

    @Test
    public void testIntBufPool() {
        IntBufPool pool = IntBufPool.getInstance();
        IntBuf intBuf = pool.acquire(1024);
        Assert.assertEquals(intBuf.intBufPool(), pool);

        intBuf.write(1);
        Assert.assertEquals(intBuf.writerIndex(), 1);

        pool.release(intBuf);
        Assert.assertEquals(intBuf.writerIndex(), 0);

        IntBuf intBuf2 = pool.acquire(100 * 1024 + 1);
        Assert.assertNotEquals(intBuf2.intBufPool(), pool);
        Assert.assertNull(intBuf2.intBufPool());
    }

}
