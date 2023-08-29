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
package DynamicPackage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.commons.io.FileUtils;
import org.junit.platform.commons.util.StringUtils;

public class CreateJarUtil {

    public static void createJarFile(String classPath, String jarFilePath, String jarFileName) throws IOException {
        if (!new File(classPath).exists()) {
            throw new IOException(String.format("rootPath [%s] does not exists", classPath));
        }
        if (StringUtils.isBlank(jarFileName)) {
            throw new NullPointerException("jarFileName is null");
        }

        // generate META-INF file
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
        manifest.getMainAttributes().putValue("Main-Class", "HelloUDF");

        // generate jar file
        final File jarFile = File.createTempFile("HelloUDF-", ".jar",
            new File(System.getProperty("java.io.tmpdir")));

        JarOutputStream out = new JarOutputStream(new FileOutputStream(jarFile), manifest);
        createTempJarInner(out, new File(classPath), "");
        out.flush();
        out.close();

        // generate target jar file
        File targetPath = new File(jarFilePath);
        if (!targetPath.exists()) {
            targetPath.mkdirs();
        }
        File targetJarFile = new File(jarFilePath + File.separator + jarFileName + ".jar");
        if (targetJarFile.exists() && targetJarFile.isFile()) {
            targetJarFile.delete();
        }
        FileUtils.moveFile(jarFile, targetJarFile);
        System.out.println("打包JAR成功");
    }

    private static void createTempJarInner(JarOutputStream out, File f, String base) throws IOException {
        if (f.isDirectory()) {
            File[] fl = f.listFiles();
            if (base.length() > 0) {
                base = base + "/";
            }
            for (int i = 0; i < fl.length; i++) {
                createTempJarInner(out, fl[i], base + fl[i].getName());
            }
        } else {
            out.putNextEntry(new JarEntry(base));
            FileInputStream in = new FileInputStream(f);
            byte[] buffer = new byte[1024];
            int n = in.read(buffer);
            while (n != -1) {
                out.write(buffer, 0, n);
                n = in.read(buffer);
            }
            in.close();
        }
    }
}
