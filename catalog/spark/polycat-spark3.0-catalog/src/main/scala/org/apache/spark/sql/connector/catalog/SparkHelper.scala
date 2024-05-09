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

import io.polycat.catalog.client.PolyCatClient
import io.polycat.catalog.common.model.Column
import io.polycat.catalog.common.plugin.request.ListFileRequest
import io.polycat.catalog.common.types.{DataTypes => LmsDataTypes}
import io.polycat.catalog.common.utils.PartitionUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.connector.expressions.{LogicalExpressions, Transform}
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.execution.datasources.{InMemoryFileIndex, PartitionPath, PartitionSpec}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

object SparkHelper {
  def createIdentifier(namespace: Array[String], name: String) = new IdentifierImpl(namespace, name)

  def convertDataType(dataType: String): DataType = {
    var result = dataType.toUpperCase.trim
    if (result.equals(LmsDataTypes.BOOLEAN.getName)) {
      return DataTypes.BooleanType
    } else if (result.equals(LmsDataTypes.TINYINT.getName)) {
      return DataTypes.ByteType
    } else if (result.equals(LmsDataTypes.BYTE.getName)) {
      return DataTypes.ByteType
    } else if (result.equals(LmsDataTypes.SMALLINT.getName)) {
      return DataTypes.ShortType
    } else if (result.equals(LmsDataTypes.SHORT.getName)) {
      return DataTypes.ShortType
    } else if (result.equals(LmsDataTypes.INTEGER.getName)) {
      return DataTypes.IntegerType
    } else if (result.equals(LmsDataTypes.INT.getName)) {
      return DataTypes.IntegerType
    } else if (result.equals(LmsDataTypes.BIGINT.getName)) {
      return DataTypes.LongType
    } else if (result.equals(LmsDataTypes.LONG.getName)) {
      return DataTypes.LongType
    } else if (result.equals(LmsDataTypes.FLOAT.getName)) {
      return DataTypes.FloatType
    } else if (result.equals(LmsDataTypes.DOUBLE.getName)) {
      return DataTypes.DoubleType
    } else if (result.equals(LmsDataTypes.STRING.getName)) {
      return DataTypes.StringType
    } else if (result.equals(LmsDataTypes.VARCHAR.getName)) {
      return DataTypes.StringType
    } else if (result.equals(LmsDataTypes.TIMESTAMP.getName)) {
      return DataTypes.TimestampType
    } else if (result.equals(LmsDataTypes.DATE.getName)) {
      return DataTypes.DateType
    } else if (result.equals(LmsDataTypes.BLOB.getName)) {
      return DataTypes.BinaryType
    } else if (result.equals(LmsDataTypes.BINARY.getName)) {
      return DataTypes.BinaryType
    } else if (result.startsWith("DECIMAL(") && result.endsWith(")")) {
      return parseDecimal(result)
    } else if (result.startsWith("ARRAY<")) {
      return DataType.fromDDL(result)
    }
    throw new UnsupportedOperationException("failed to convert " + dataType)
  }

  def parseDecimal(dataType: String): DataType = {
    val i = dataType.indexOf("(")
    val j = dataType.indexOf(",")
    val k = dataType.indexOf(")")
    if (k != dataType.length - 1) {
      throw new UnsupportedOperationException("failed to convert " + dataType)
    }

    val str1 = dataType.substring(i + 1, j)
    val str2 = dataType.substring(j + 1, k)

    val precision = str1.toInt
    val scale = str2.toInt
    return DataTypes.createDecimalType(precision, scale)
  }


  def createTransform(columnName: String): Transform = {
    LogicalExpressions.identity(LogicalExpressions.reference(Seq(columnName)))
  }

  def setPartitionInfo(client: PolyCatClient, projectId: String, table: io.polycat.catalog.common.model.Table, sparkTable: Table, shareName: String): Unit = {
    if (table.getPartitionKeys != null && !table.getPartitionKeys.isEmpty) {
      sparkTable match {
        case fileTable: FileTable =>
          fileTable.fileIndex match {
            case inMemoryFileIndex: InMemoryFileIndex =>
              if (!table.isLmsMvcc && inMemoryFileIndex.partitionSpec() != PartitionSpec.emptySpec) {
                return
              }
              val partitionColumns = table.getPartitionKeys
                .asScala
                .map(x => convertField(x))
              val partitionPaths = if (table.isLmsMvcc) {
                val request: ListFileRequest = new ListFileRequest(projectId, table.getCatalogName, table.getDatabaseName, table.getTableName, shareName)
                val partitions = client.listPartitions(request)
                partitions.asScala.map(p => {
                  val fields = table.getPartitionKeys.asScala
                  // initialCommitPartition means that table does not have a commit_partition like 'lms_commit=UUID', instead
                  // the table folder is being considered as a commit partition to support time travel. This is valid in
                  // case of tables migrated through metabot.
                  // Subsequent loads to such table would be considered as normal partition but the first commit would
                  // always be a special partition.
                  var initialCommitPartition = false
                  val rawPartitionValues = p.getPartitionValues
                  // Below condition is true when user migrates a partition table using metabot, the lms_commit
                  // partition directory is not available thus spark will throw ArrayIndexOutOfBounds exception when filling the vector/batch.
                  // To avoid this the available partition name would be added to the result indicating that the
                  // initalCommitPartition points to the actual partition(the column provided by user) directory.
                  val updatedPartitions = if (rawPartitionValues.size() == partitionColumns.length - 1) {
                    initialCommitPartition = true
                    rawPartitionValues.add(table.getTableName)
                    rawPartitionValues.asScala
                  } else {
                    rawPartitionValues.asScala
                  }
                  val values = updatedPartitions.map(x => PartitionUtil.unescapePathName(x))
                    .zipWithIndex
                    .map(x => Cast(Literal(x._1), convertDataType(fields(x._2).getColType)).eval())
                  val loc = if (initialCommitPartition) {
                    table.getStorageDescriptor.getLocation
                  } else {
                    p.getStorageDescriptor.getLocation
                  }
                  PartitionPath(InternalRow.fromSeq(values), loc)
                })
              } else {
                PartitionSpec.emptySpec.partitions
              }
              val partitionSpec = new PartitionSpec(StructType(partitionColumns), partitionPaths)
              val field = classOf[InMemoryFileIndex].getDeclaredField("cachedPartitionSpec")
              field.setAccessible(true)
              field.set(inMemoryFileIndex, partitionSpec)
            case _ =>
          }
        case _ =>
      }
    }
  }

  def convertField(x: Column) = {
    StructField(x.getColumnName, convertDataType(x.getColType), true, Metadata.empty)
  }
}
