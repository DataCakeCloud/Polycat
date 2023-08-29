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
package MyPerf4J.test1;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Created by LinShunkang on 2018/4/23
 */
public class MyClassAdapter extends ClassVisitor implements Opcodes {

    private Class<?> analysisClass;

    private boolean isInterface;

    public MyClassAdapter(final ClassVisitor cv, Class<?> clazz) {
        super(ASM5, cv);
        this.analysisClass = clazz;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        super.visit(version, access, name, signature, superName, interfaces);
        this.isInterface = (access & ACC_INTERFACE) != 0;
    }

    @Override
    public MethodVisitor visitMethod(int access,
                                     String name,
                                     String desc,
                                     String signature,
                                     String[] exceptions) {
        MethodVisitor mv = cv.visitMethod(access, name, desc, signature, exceptions); //先得到原始的方法
        if (isInterface || mv == null || "<init>".equals(name) || "<clinit>".equals(name)) {
            return mv;
        }

//        return new EnteringAdapter(mv, access, name, desc); //访问需要修改的方法
//        return new ExitingAdapter(mv, access, name, desc); //访问需要修改的方法

        MethodVisitor enteringMV = new EnteringAdapter(mv, access, name, desc); //访问需要修改的方法
        return new FinallyAdapter(enteringMV, access, name, desc); //访问需要修改的方法
    }
}
