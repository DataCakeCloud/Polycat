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

import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsCatalogOptions;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

@Slf4j
public class PolyCatSupportsCatalogOptions implements SupportsCatalogOptions, DataSourceRegister {
    /**
     * Return a {@link Identifier} instance that can identify a table for a DataSource given
     * DataFrame[Reader|Writer] options.
     *
     * @param options the user-specified options that can identify a table, e.g. file path, Kafka
     *                topic name, etc. It's an immutable case-insensitive string-to-string map.
     */
    @Override
    public Identifier extractIdentifier(CaseInsensitiveStringMap options) {
        return null;
    }

    /**
     * Infer the schema of the table identified by the given options.
     *
     * @param options an immutable case-insensitive string-to-string map that can identify a table,
     *                e.g. file path, Kafka topic name, etc.
     */
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return null;
    }

    /**
     * Return a {@link Table} instance with the specified table schema, partitioning and properties
     * to do read/write. The returned table should report the same schema and partitioning with the
     * specified ones, or Spark may fail the operation.
     *
     * @param schema       The specified table schema.
     * @param partitioning The specified table partitioning.
     * @param properties   The specified table properties. It's case preserving (contains exactly what
     *                     users specified) and implementations are free to use it case sensitively or
     *                     insensitively. It should be able to identify a table, e.g. file path, Kafka
     */
    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        log.info("getTable properties: {}", properties);
        log.info("getTable schema: {}", schema);
        return null;
    }

    /*def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
        val paths = getPaths(options)
        val tableName = getTableName(options, paths)
        val optionsWithoutPaths = getOptionsWithoutPaths(options)
        ParquetTable(
                tableName, sparkSession, optionsWithoutPaths, paths, Some(schema), fallbackFileFormat)
    }*/

    @Override
    public String shortName() {
        return "polycat";
    }
}
