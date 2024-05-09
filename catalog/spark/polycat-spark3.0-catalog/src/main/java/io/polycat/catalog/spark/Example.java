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

import io.polycat.catalog.common.Logger;

import org.apache.spark.sql.SparkSession;

public class Example {
    static private final Logger logger = Logger.getLogger(Example.class);

    public static void main(String[] args) {
        logger.info("Example starts");
        SparkSession session = SparkSession.builder()
                .appName("test")
                .master("local")
                .config("spark.sql.catalog.dash", SparkCatalog.class.getCanonicalName())
                .config("spark.sql.catalog.dash.projectid", "123")
                .config("spark.sql.catalog.dash.tenant", "456")
                .config("spark.sql.catalog.dash.username", "789")
                .getOrCreate();

        session.sql("create table dash.t1 (name string, age int)");
        session.sql("insert into dash.t1 values ('amy', 32), ('bob', 31)");
        session.sql("select * from dash.t1").show();

        session.close();
    }
}
