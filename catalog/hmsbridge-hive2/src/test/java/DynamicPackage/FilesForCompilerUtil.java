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
import java.util.ArrayList;
import java.util.List;


public class FilesForCompilerUtil {

    public static List<File> getSourceFiles(String sourceFilePath) {
        List<File> sourceFileList = new ArrayList<>();
        try {
            getSourceFiles(new File(sourceFilePath), sourceFileList);
        } catch (Exception e) {
            e.printStackTrace();
            sourceFileList = null;
        }
        return sourceFileList;
    }

    private static void getSourceFiles(File sourceFile, List<File> sourceFileList) throws Exception {
        if (!sourceFile.exists()) {
            throw new IOException(String.format("source path [%s] does not exist", sourceFile.getPath()));
        }
        if (null == sourceFileList) {
            throw new NullPointerException("sourceFileList cannot be null");
        }

        if (sourceFile.isDirectory()) {
            for (File file : sourceFile.listFiles()) {
                if (file.isDirectory()) {
                    getSourceFiles(file, sourceFileList);
                } else {
                    sourceFileList.add(file);
                }
            }
        } else {
            sourceFileList.add(sourceFile);
        }
    }

    public static String getJarFiles(String sourceFilePath) {
        String jars = ".:";
        try {
            jars = getJarFiles(new File(sourceFilePath), jars);
        } catch (Exception e) {
            e.printStackTrace();
            jars = "";
        }
        return jars;
    }

    private static String getJarFiles(File sourceFile, String jars) throws Exception {
        if (!sourceFile.exists()) {
            throw new IOException("jar path does not exist");
        }

        if (sourceFile.isDirectory()) {
            for (File file : sourceFile.listFiles()) {
                if (file.isDirectory()) {
                    jars = getJarFiles(file, jars);
                } else {
                    jars = jars + file.getPath() + ":";
                }
            }
        } else {
            jars += sourceFile.getPath() + ":";
        }
        return jars;
    }

}
