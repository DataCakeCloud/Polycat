/**
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
package org.apache.spark.sql.connector.catalog

import io.polycat.catalog.client.Client
import io.polycat.catalog.common.model.TableName
import io.polycat.catalog.spark.PolyCatClientSparkHelper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * @author liangyouze
 * @date 2024/2/26
 */
class PolyCatStagedTable(client: Client, tableName: TableName, schema: StructType, partitionKeysSchema: StructType, sparkSession: SparkSession, fileFormat: String, paths: Seq[String], options: CaseInsensitiveStringMap, userSpecifiedSchema: Option[StructType])
  extends PolyCatPartitionTable(client = client, tableName = tableName,
    schema = schema,
    partitionKeysSchema = partitionKeysSchema,
    sparkSession = sparkSession, fileFormat = fileFormat, paths = paths, options = options, userSpecifiedSchema = userSpecifiedSchema) with StagedTable{

  override def commitStagedChanges(): Unit = {
    //TODO support replace table
    PolyCatClientSparkHelper.createTable(client, tableName, schema, partitioning(), options)
  }

  override def abortStagedChanges(): Unit = {

  }
}
