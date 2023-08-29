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

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.polycat.catalog.spark.SparkCatalogTestEnv.batch;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.checkAnswer;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.row;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.shouldExistTableInLMS;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.sql;

@ExtendWith({SparkCatalogTestEnv.class})
public class SparkCatalogTest {

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc", "carbon"})
    void smoke(String format) {
        sql("create catalog spark_" + format);
        sql("use catalog spark_" + format);
        sql("create namespace db1");
        sql("use namespace db1");
        sql("create table t1 (c1 string, c2 int) using " + format);
        sql("insert into t1 values('a', 1)");
        sql("insert into t1 values('b', 2)");

        checkAnswer(sql("select * from t1 order by c1"),
            batch(row("a", 1), row("b", 2)));

        shouldExistTableInLMS("spark_" + format, "db1", "t1");
    }

}
