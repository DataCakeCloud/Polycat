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
package MyPerf4J;

import cn.myperf4j.base.util.Logger;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

/**
 * Created by LinShunkang on 2018/10/17
 */
public final class ClassFileUtils {

    private ClassFileUtils() {
        //empty
    }

    public static byte[] getClassFileContent(String fullClassName) throws IOException {
        int idx = fullClassName.lastIndexOf(".");
        String simpleClassName = fullClassName.substring(idx + 1);
        File targetClassFile = getClasses(fullClassName.substring(0, idx), true, simpleClassName + ".class");
        if (targetClassFile == null) {
            return null;
        }

        return toByteArray(new FileInputStream(targetClassFile));
    }

    private static File getClasses(String basePackage, boolean recursive, String targetClassName) {
        String packageName = basePackage;
        if (packageName.endsWith(".")) {
            packageName = packageName.substring(0, packageName.lastIndexOf('.'));
        }

        String package2Path = packageName.replace('.', '/');
        try {
            Enumeration<URL> dirs = Thread.currentThread().getContextClassLoader().getResources(package2Path);
            while (dirs.hasMoreElements()) {
                URL url = dirs.nextElement();
                String protocol = url.getProtocol();
                if ("file".equals(protocol)) {
                    Logger.debug("ClassScanner scanning file type class....");
                    String filePath = URLDecoder.decode(url.getFile(), "UTF-8");
                    File file = scanByFile(packageName, filePath, recursive, targetClassName);
                    if (file != null) {
                        return file;
                    }
                }
            }
        } catch (IOException e) {
            Logger.error("ClassScanner.getClasses(" + basePackage + ", " + recursive + ")", e);
        }
        return null;
    }

    private static File scanByFile(String packageName,
                                   String packagePath,
                                   final boolean recursive,
                                   String targetClassName) {
        File dir = new File(packagePath);
        if (!dir.exists() || !dir.isDirectory()) {
            return null;
        }

        File[] dirFiles = dir.listFiles(new FileFilter() {
            // 自定义文件过滤规则
            public boolean accept(File file) {
                if (file.isDirectory()) {
                    return recursive;
                }
                return filterClassName(file.getName());
            }
        });

        if (dirFiles == null) {
            return null;
        }

        for (File file : dirFiles) {
            if (file.isDirectory()) {
                scanByFile(packageName + "." + file.getName(), file.getAbsolutePath(), recursive, targetClassName);
                continue;
            }

            if (file.getName().equals(targetClassName)) {
                return file;
            }
        }
        return null;
    }

    private static boolean filterClassName(String className) {
        return className.endsWith(".class");
    }

    private static byte[] toByteArray(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024 * 4];
        int n;
        while ((n = in.read(buffer)) != -1) {
            out.write(buffer, 0, n);
        }
        return out.toByteArray();
    }
}
