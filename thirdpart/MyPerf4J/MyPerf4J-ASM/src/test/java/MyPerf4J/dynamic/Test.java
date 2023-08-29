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

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;


/**
 * Created by LinShunkang on 2018/4/23
 */
public final class Test {

    private Test() {
        //empty
    }

    public static void main(String[] args) throws Throwable {
        rewriteClass();
//        runNewHandler();
    }

    private static void rewriteClass() throws IOException {
        ClassReader cr = new ClassReader(DaoInvocationHandler.class.getName());
        ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_FRAMES);
        ClassVisitor cv = new DynamicClassAdapter(cw);
        cr.accept(cv, ClassReader.EXPAND_FRAMES);

        byte[] bytes = cw.toByteArray();
        rewriteClass(bytes);
    }

    private static void rewriteClass(byte[] toByte) throws IOException {
        File tofile = new File("/Users/linshunkang/WorkSpace/personal/MyPerf4J/MyPerf4J-ASM/target" +
                "/test-classes/MyPerf4J/dynamic/DaoInvocationHandler.class");
        FileOutputStream fout = new FileOutputStream(tofile);
        fout.write(toByte);
        fout.close();
    }

    private static void runNewHandler() {
        DaoInvocationHandler handler = new DaoInvocationHandler();
        handler.invoke(null, null, new Object[0]);
    }
}
