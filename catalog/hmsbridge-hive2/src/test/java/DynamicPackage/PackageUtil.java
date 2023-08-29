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
import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.UDF;

public class PackageUtil {
    static String encoding = "utf-8";
    static String curModulePath;
    static String separator = File.separator;

    static String sourceRootPath;
    static String targetClassPath;
    static String refSourcePath;
    static String refJarPath;
    static String targetJarPath;
    static String targetJarName;

    public static void runPackage() throws Exception {
        preparePackage();
        CompilerUtils.compiler(sourceRootPath, targetClassPath, refSourcePath, encoding, refJarPath);
        CreateJarUtil.createJarFile(targetClassPath, targetJarPath, targetJarName);
    }

    private static void preparePackage() throws IOException {
        refJarPath = UDF.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        File f = new File(".");
        curModulePath = f.getCanonicalPath();

        // make source paths
        sourceRootPath = curModulePath + "/src/test/java/io";
        f = new File(sourceRootPath);
        if (f.exists()) {
            refSourcePath = sourceRootPath;
        } else {
            throw new IOException("source root directory not found");
        }

        // make target paths
        f = new File(curModulePath + "/target");
        if (!f.exists()) {
            f.mkdirs();
        }
        String targetDir = f.getCanonicalPath() + separator + "udf-jar-package";
        targetClassPath = targetDir + "/class";
        targetJarPath = targetDir + "/jar";
        targetJarName = "HelloUDF";
    }
}
