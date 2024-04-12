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
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.polycat.catalog.common.plugin.request.CreateFunctionRequest;
import io.polycat.catalog.common.plugin.request.GetFunctionRequest;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import io.polycat.catalog.common.plugin.request.input.FunctionResourceUri;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class UDFTest extends HMSBridgeTestEnv {
    String dir = "target/";
    String udfJar = "udf-jar-package/jar/HelloUDF.jar";
    String udfClassName = "HelloUDF";
    String udfJarLocation;

    private void createAndUseDbInPolyCat(String dbName) {
        assertDoesNotThrow(() -> sparkSession.sql("create database if not exists " + dbName
            + " with dbproperties (lms_name='lms')").collect());
        assertDoesNotThrow(() -> sparkSession.sql("use " + dbName).collect());
    }

    private GetFunctionRequest makeGetFunctionRequest(String dbName, String funcName) {
        return new GetFunctionRequest(catalogClient.getProjectId(), defaultCatalogName, dbName, funcName);
    }

    private void runCreateFunction(String syntax, String funcName, String dbName) {
        switch (syntax) {
            case "add_jar":
                sparkSession.sql("add jar " + udfJarLocation).collect();
                assertDoesNotThrow(() ->
                    sparkSession.sql("create function " + funcName + " as '" + udfClassName + "'").collect());
                break;
            case "using":
                assertDoesNotThrow(() -> sparkSession.sql("create function " + funcName + " AS '" + udfClassName
                    + "' using jar '" + udfJarLocation + "'").collect());
                break;
            case "ddl_server":
                CreateFunctionRequest createFunctionRequest = makeCreateFunctionRequest(dbName, funcName);
                assertDoesNotThrow(() -> catalogClient.createFunction(createFunctionRequest));
                break;
            default:
                throw new RuntimeException("Unexpected syntax");
        }
    }

    private CreateFunctionRequest makeCreateFunctionRequest(String dbName, String funcName) {
        FunctionInput fIn = new FunctionInput();
        fIn.setFunctionName(funcName);
        fIn.setClassName(udfClassName);
        fIn.setOwner(catalogClient.getUserName());
        fIn.setOwnerType("USER");
        fIn.setFuncType("JAVA");
        fIn.setDatabaseName(dbName);
        List<FunctionResourceUri> resourceUris = new ArrayList<>();
        FunctionResourceUri uri = new FunctionResourceUri("JAR", udfJarLocation);
        resourceUris.add(uri);
        fIn.setResourceUris(resourceUris);
        return new CreateFunctionRequest(catalogClient.getProjectId(), defaultCatalogName, fIn);
    }

    @BeforeEach
    public void beforeTests() throws IOException {
        File jarFile = new File(dir + udfJar);
        if (jarFile.exists()) {
            Path path = new Path(jarFile.getCanonicalPath());
            udfJarLocation = path.toUri().toString();
        } else {
            throw new IOException("udf jar not found");
        }
    }

    @Test
    public void show_default_udf_from_empty_db_should_be_same() {
        Row[] defaultFuncs1 = (Row[]) sparkSession.sql("SHOW FUNCTIONS").collect();
        assertNotNull(defaultFuncs1);

        sparkSession.sql("create database udf_empty_db").collect();
        sparkSession.sql("use udf_empty_db").collect();
        Row[] defaultFuncs2 = (Row[]) sparkSession.sql("SHOW FUNCTIONS").collect();
        assertEquals(Arrays.stream(defaultFuncs1).collect(Collectors.toList()),
            Arrays.stream(defaultFuncs2).collect(Collectors.toList()));

        sparkSession.sql("create database lms_udf_empty_db with dbproperties (lms_name='lms')").collect();
        sparkSession.sql("use lms_udf_empty_db").collect();
        Row[] defaultFuncs3 = (Row[]) sparkSession.sql("SHOW FUNCTIONS").collect();
        assertEquals(Arrays.stream(defaultFuncs2).collect(Collectors.toList()),
            Arrays.stream(defaultFuncs3).collect(Collectors.toList()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"add_jar", "using", "ddl_server"})
    public void create_udf_in_diff_ways_should_success(String createFuncSyntax) {
        String udfDb = "udf_db_from_bridge";
        createAndUseDbInPolyCat(udfDb);
        String funcName = "hello";
        String udfFullName = udfDb + "." + funcName;

        // create udf by 'add jar' and 'create function using ..'
        runCreateFunction(createFuncSyntax, funcName, udfDb);

        // check udf by 'show' and 'get' sql
        Row[] res = (Row[]) sparkSession.sql("show functions like 'hello'").collect();
        assertEquals(1, res.length);
        assertEquals(udfFullName, res[0].getString(0));

        res = (Row[]) sparkSession.sql("desc function " + funcName).collect();
        assertEquals(3, res.length);
        assertEquals("Function: " + udfFullName, res[0].getString(0));
        assertEquals("Class: " + udfClassName, res[1].getString(0));
        assertEquals("Usage: N/A.", res[2].getString(0));

        // check udf by select
        res = (Row[]) sparkSession.sql("select " + funcName + "('World')").collect();
        assertEquals("Hello PolyCat World", res[0].getString(0));

        // check udf by polycat client
        GetFunctionRequest getFunctionRequest = makeGetFunctionRequest(udfDb, funcName);
        FunctionInput function = catalogClient.getFunction(getFunctionRequest);
        assertEquals(udfDb, function.getDatabaseName());
        assertEquals(funcName, function.getFunctionName());
        assertEquals(udfClassName, function.getClassName());

        // drop function
        assertDoesNotThrow(() -> sparkSession.sql("drop function if exists " + funcName).collect());
        res = (Row[]) sparkSession.sql("show functions like 'hello'").collect();
        assertEquals(0, res.length);
    }
}
