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
package org.apache.spark.sql.execution.datasources.v2.carbon

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.carbondata.execution.datasources.SparkCarbonFileFormat
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class CarbonFileTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {
  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    // convert key of options to lower case
    val mutableOptions = scala.collection.mutable.Map[String, String]();
    options.forEach((key, value) => { mutableOptions.put(key.toLowerCase(), value)})
    new SparkCarbonFileFormat().inferSchema(sparkSession, mutableOptions.toMap, files)
  }

  override def formatName: String = "CARBON"

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    CarbonFileScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new CarbonFileWriteBuilder(paths, formatName, supportsDataType, info)
}
