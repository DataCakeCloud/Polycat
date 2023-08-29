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
package io.polycat.catalog.server.security;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.polycat.catalog.common.Logger;

public class ApiToOperation {

    private static final Logger logger = Logger.getLogger(ApiToOperation.class.getName());
    private static Map<String, ArrayList<String>> apiToOperation = new HashMap<>();

    public ApiToOperation() {
        initialization();
    }

    private void initialization() {
        String propertyPath = getPropertyPath();
        File propertyFile = new File(propertyPath);
        FileInputStream fis = null;
        Properties apiToOperationProperties = new Properties();

        try {
            if (propertyFile.exists()) {
                fis = new FileInputStream(propertyFile);
                apiToOperationProperties.load(fis);
                int numIdentifyInfor = apiToOperationProperties.size();
                if (numIdentifyInfor > 0) {
                    for (Map.Entry<Object, Object> entry : apiToOperationProperties.entrySet()) {
                        ArrayList<String> operationStrs = new ArrayList<String>();
                        String str = (String) entry.getValue();
                        for (String operStr : str.split(";")) {
                            operationStrs.add(operStr);
                        }
                        if (!operationStrs.isEmpty())
                            apiToOperation.put(entry.getKey().toString(), operationStrs);
                    }

                } else {
                    return;
                }
            }
        } catch (FileNotFoundException var17) {
            logger.error("The file: " + propertyFile.getAbsolutePath() + " does not exist");
        } catch (IOException var18) {
        } finally {
            if (null != fis) {
                try {
                    fis.close();
                } catch (IOException var15) {
                    logger.error("Error while closing the file stream for file: " + propertyFile.getAbsolutePath());
                }
            }
        }
    }

    public String getPropertyPath() {
        String rootPath = System.getProperty("user.dir");
        String propertyPath = rootPath + "/../conf/apiToOperation.properties";
        return propertyPath;
    }

    public ArrayList<String> getOperationByApi(String api) {
        if (!apiToOperation.isEmpty()) {
            ArrayList<String> strOperationList = apiToOperation.get(api);
            return strOperationList;
        }
        return null;
    }
}