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
package MyPerf4J.dynamic;

import cn.myperf4j.asm.aop.ProfilingAspect;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;

/**
 * Created by LinShunkang on 2018/4/15
 */
public class DynamicMethodVisitor extends AdviceAdapter {

    private static final String PROFILING_ASPECT_INNER_NAME = Type.getInternalName(ProfilingAspect.class);

    private int startTimeIdentifier;

    private String name;

    private String desc;

    public DynamicMethodVisitor(int access,
                                String name,
                                String desc,
                                MethodVisitor mv) {
        super(ASM5, mv, access, name, desc);
        this.name = name;
        this.desc = desc;
    }

    @Override
    protected void onMethodEnter() {
        if (profiling()) {
            mv.visitMethodInsn(INVOKESTATIC, "java/lang/System", "nanoTime", "()J", false);
            startTimeIdentifier = newLocal(Type.LONG_TYPE);
            mv.visitVarInsn(LSTORE, startTimeIdentifier);
        }
    }

    @Override
    protected void onMethodExit(int opcode) {
        if (profiling() && ((IRETURN <= opcode && opcode <= RETURN) || opcode == ATHROW)) {
            mv.visitVarInsn(LLOAD, startTimeIdentifier);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitMethodInsn(INVOKESTATIC, PROFILING_ASPECT_INNER_NAME,
                    "profiling", "(JLjava/lang/reflect/Method;)V", false);
        }
    }

    private boolean profiling() {
        return true;
    }
}
