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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.carbondata.execution.datasources.SparkCarbonFileFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

case class CarbonFilePartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter]) extends FilePartitionReaderFactory {

  private val resultSchema = StructType(readDataSchema.fields ++ partitionSchema.fields)
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis

  private val fileReader = new SparkCarbonFileFormat().buildReaderWithPartitionValues(
    SparkSession.getActiveSession.get,
    dataSchema,
    partitionSchema,
    readDataSchema,
    filters,
    Map.empty,
    broadcastedConf.value.value)

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    val iter = fileReader.apply(partitionedFile)
    new PartitionReader[InternalRow]() {
      override def next(): Boolean = {
        iter.hasNext
      }

      override def get(): InternalRow = iter.next()

      override def close(): Unit = {

      }
    }
  }
}
