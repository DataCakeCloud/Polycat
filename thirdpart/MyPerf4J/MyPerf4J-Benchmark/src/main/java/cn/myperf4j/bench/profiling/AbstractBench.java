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
package cn.myperf4j.bench.profiling;

import cn.myperf4j.asm.ASMRecorderMaintainer;
import cn.myperf4j.asm.aop.ProfilingAspect;
import cn.myperf4j.base.config.ProfilingParams;
import cn.myperf4j.bench.EnvUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Created by LinShunkang on 2019/08/31
 */
@State(Scope.Thread)
public class AbstractBench {

    @Setup
    public void setup() {
        boolean initASMBootstrap = EnvUtils.initASMBootstrap(60 * 1000);
        System.out.println("initASMBootstrap=" + initASMBootstrap);

        ASMRecorderMaintainer recorderMaintainer = ASMRecorderMaintainer.getInstance();
        recorderMaintainer.addRecorder(0, ProfilingParams.of(1000, 16));

        ProfilingAspect.setRunning(true);
        ProfilingAspect.setRecorderMaintainer(recorderMaintainer);
    }

    @Benchmark
    public int baseline() {
        return emptyMethod();
    }

    @Benchmark
    public int profiling() {
        long start = System.nanoTime();
        int result = emptyMethod();
        ProfilingAspect.profiling(start, 0);
        return result;
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public int emptyMethod() {
        return 1000;
    }
}
