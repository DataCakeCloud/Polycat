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

import java.util.List;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ApiToOperationTest {

    public static ApiToOperation apiToOperation;

    @BeforeAll
    public static void setup() {
       new MockUp<ApiToOperation>() {
           @Mock
            public String getPropertyPath() {
                String rootPath = System.getProperty("user.dir");
                String propertyPath = rootPath + "/../../conf/apiToOperation.properties";
                return propertyPath;
            }
        };
        System.out.println("LocalAuthenticatorTest set up");
        apiToOperation = new ApiToOperation() ;
    }

    @Test
    public  void  getOperationByApiTest(){
        String apiTest = "alterDatabase";
        List<String> operationTest;
        operationTest = apiToOperation.getOperationByApi(apiTest);
        if(!operationTest.isEmpty())
        {
            operationTest.stream().forEach(System.out::println);
        }
          return;
    }
}
