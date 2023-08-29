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
package cn.myperf4j.base.buffer;

import java.util.Arrays;

/**
 * Created by LinShunkang on 2019/06/12
 */
public class IntBuf {

    private final int[] buf;

    private int writerIndex;

    private final IntBufPool pool;

    public IntBuf(int capacity, IntBufPool pool) {
        this.buf = new int[capacity];
        this.writerIndex = 0;
        this.pool = pool;
    }

    public IntBuf(int capacity) {
        this.buf = new int[capacity];
        this.writerIndex = 0;
        this.pool = null;
    }

    public void write(int value) {
        ensureWritable(1);
        this.buf[writerIndex++] = value;
    }

    public void write(int v1, int v2) {
        ensureWritable(2);
        this.buf[writerIndex++] = v1;
        this.buf[writerIndex++] = v2;
    }

    private void ensureWritable(int minWritableSize) {
        if (minWritableSize > buf.length - writerIndex) {
            throw new IndexOutOfBoundsException("IntBuf minWritableSize(" + minWritableSize +
                    ") + writerIndex(" + writerIndex + ") exceed buf.length(" + buf.length + ")");
        }
    }

    public void setInt(int index, int value) {
        checkBounds(index);
        this.buf[index] = value;
    }

    private void checkBounds(int index) {
        if (index >= buf.length) {
            throw new IndexOutOfBoundsException("IntBuf index(" + index + ") exceed buf.length(" + buf.length + ")");
        }
    }

    public int capacity() {
        return buf.length;
    }

    public int writerIndex() {
        return writerIndex;
    }

    public void writerIndex(int writerIndex) {
        this.writerIndex = writerIndex;
    }

    public int getInt(int index) {
        checkBounds(index);
        return buf[index];
    }

    public int _getInt(int index) {
        return buf[index];
    }

    public int[] _buf() {
        return buf;
    }

    public void reset() {
        this.writerIndex = 0;
    }

    public IntBufPool intBufPool() {
        return pool;
    }

    @Override
    public String toString() {
        return "IntBuf{" +
                "buf=" + Arrays.toString(buf) +
                ", writerIndex=" + writerIndex +
                ", pool=" + pool +
                '}';
    }
}
