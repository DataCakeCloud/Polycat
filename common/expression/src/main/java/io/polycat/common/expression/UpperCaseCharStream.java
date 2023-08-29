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
package io.polycat.common.expression;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.misc.Interval;

public class UpperCaseCharStream implements CharStream {

    private CodePointCharStream wrapped;

    public UpperCaseCharStream(CodePointCharStream wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public String getText(Interval interval) {
        return size() > 0 && (interval.b - interval.a >= 0) ? wrapped.getText(interval) : "";
    }

    @Override
    public void consume() {
        wrapped.consume();
    }

    @Override
    public int LA(int i) {
        int la = wrapped.LA(i);
        return la == 0 || la == IntStream.EOF ? la : Character.toUpperCase(la);
    }

    @Override
    public int mark() {
        return wrapped.mark();
    }

    @Override
    public void release(int i) {
        wrapped.release(i);
    }

    @Override
    public int index() {
        return wrapped.index();
    }

    @Override
    public void seek(int i) {
        wrapped.seek(i);
    }

    @Override
    public int size() {
        return wrapped.size();
    }

    @Override
    public String getSourceName() {
        return wrapped.getSourceName();
    }
}
