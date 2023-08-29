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
package io.polycat.catalog.spark;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.polycat.catalog.common.model.record.Record;
import io.polycat.common.RowBatch;

import org.apache.arrow.flatbuf.Bool;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.polycat.catalog.spark.SparkCatalogTestEnv.batch;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.checkAnswer;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.getUUIDName;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.row;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.sql;

@ExtendWith({SparkCatalogTestEnv.class})
public class CopyIntoTableTest {

    static private String sourceRoot = "target/copy_into_source_dir";
    static private String sourceAbsolutePath;

    static private RowBatch header = batch(row("c1","c2"));
    static private RowBatch copy_into_test = batch(row(1, "obs"), row(2, "s3"), row(3, "azure"));
    static private RowBatch copy_into_test2 = batch(row(4, "OSS"), row(5, "GCS"));
    static private RowBatch copy_into_test3 = batch(row(6, "cos"));

    @BeforeAll
    public static void before() throws IOException {
        initAbsolutePath();
        sql("CREATE CATALOG copy_into_catalog");
        sql("USE CATALOG copy_into_catalog");
        sql("CREATE DATABASE db1");
        sql("USE db1");
    }

    private static void initAbsolutePath() {
        String curDir = CopyIntoTableTest.class.getResource("").getPath();
        sourceAbsolutePath = curDir.substring(0, curDir.indexOf("target") + 7);
        sourceAbsolutePath += "copy_into_source_dir/";
    }

    private static void deleteSourceFiles(String rootStr) throws IOException {
        File root = new File(rootStr);
        FileUtils.forceDelete(root);
    }

    private static void makeSourceFiles(Boolean with_header, String delimiter) throws IOException {
        new File(sourceRoot).mkdir();
        new File(sourceRoot + "/sub_dir1").mkdir();
        new File(sourceRoot + "/sub_dir2").mkdir();


        FileWriter fw = new FileWriter(sourceRoot + "/sub_dir1/copy_into_test.csv");
        if (with_header) {
            writeBatch(fw, header, delimiter);
        }
        writeBatch(fw, copy_into_test, delimiter);
        fw.close();

        fw = new FileWriter(sourceRoot + "/sub_dir2/copy_into_test2.csv");
        if (with_header) {
            writeBatch(fw, header, delimiter);
        }
        writeBatch(fw, copy_into_test2, delimiter);
        fw.close();

        fw = new FileWriter(sourceRoot + "/sub_dir2/copy_into_test3.csv");
        if (with_header) {
            writeBatch(fw, header, delimiter);
        }
        writeBatch(fw, copy_into_test3, delimiter);
        fw.close();
    }

    private static void writeBatch(FileWriter fw, RowBatch batchRecords, String delimiter) throws IOException {
        for (Record record : batchRecords.getRecords()) {
            String line = record.fields.stream().map(Object::toString)
                .collect(Collectors.joining(delimiter));
            fw.write(line + "\n");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc", "csv"})
    void copy_into_table_test_with_header(String source) throws IOException {
        char[] delimiters = {',', '|', ' '};
        for (char delimiter : delimiters) {
            makeSourceFiles(true, String.valueOf(delimiter));

            // copy single csv file
            String table1Name = "t1_" + source + getUUIDName();
            sql("CREATE TABLE " + table1Name + " (c1 int, c2 string) using " + source);
            sql("COPY INTO copy_into_catalog.db1." + table1Name
                + " FROM '" + sourceAbsolutePath + "sub_dir1/copy_into_test.csv'"
                + " FILE_FORMAT=csv WITH_HEADER=true DELIMITER='" + delimiter + "'");
            checkAnswer(sql("SELECT c1,c2 FROM " + table1Name), copy_into_test);

            // copy multiple csv files
            String table2Name = "t2_" + source + getUUIDName();
            sql("CREATE TABLE " + table2Name + " (c1 int, c2 string) USING " + source);
            sql("COPY INTO copy_into_catalog.db1." + table2Name
                + " FROM '" + sourceAbsolutePath + "sub_dir2'"
                + " FILE_FORMAT=csv WITH_HEADER=true DELIMITER='" + delimiter + "'");
            List<Record> r2 = new ArrayList<>(copy_into_test2.getRecords());
            r2.addAll(copy_into_test3.getRecords());
            RowBatch expectedBatch2 = new RowBatch(r2);
            checkAnswer(sql("SELECT c1,c2 FROM " + table2Name + " ORDER BY c1"), expectedBatch2);

            //sql("drop table t1_" + source);
            //sql("drop table t2_" + source);
            deleteSourceFiles(sourceRoot);
        }
    }

    @Test
    void copy_into_table_test_without_header() throws IOException {
        char[] delimiters = {',', '|', ' '};
        for (char delimiter : delimiters) {
            makeSourceFiles(false, String.valueOf(delimiter));

            // copy single csv file
            String table1Name = "t1_" + getUUIDName();
            sql("CREATE TABLE " + table1Name + " (c1 int, c2 string) using csv");
            sql("COPY INTO copy_into_catalog.db1." + table1Name
                + " FROM '" + sourceAbsolutePath + "sub_dir1/copy_into_test.csv'"
                + " FILE_FORMAT=csv WITH_HEADER=false"
                + " DELIMITER='" + delimiter + "'");
            checkAnswer(sql("SELECT c1,c2 FROM " + table1Name), copy_into_test);

            // copy multiple csv files
            String table2Name = "t2_"  + getUUIDName();
            sql("CREATE TABLE " + table2Name + " (c1 int, c2 string) USING csv");
            sql("COPY INTO copy_into_catalog.db1." + table2Name
                + " FROM '" + sourceAbsolutePath + "sub_dir2'"
                + " FILE_FORMAT=csv WITH_HEADER=false"
                + " DELIMITER='" + delimiter + "'");
            List<Record> r2 = new ArrayList<>(copy_into_test2.getRecords());
            r2.addAll(copy_into_test3.getRecords());
            RowBatch expectedBatch2 = new RowBatch(r2);
            checkAnswer(sql("SELECT c1,c2 FROM " + table2Name), expectedBatch2);



                //sql("drop table t1_" + source);
            //sql("drop table t2_" + source);
            deleteSourceFiles(sourceRoot);
        }
    }
}
