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
package org.apache.spark.sql.hive

import io.polycat.catalog.common.model.Column
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.SerDeUtils
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.hive.execution.HiveTableScanExec

import java.util.Properties

import scala.collection.JavaConversions._

/**
 * @author liangyouze
 * @date 2024/3/21
 */

object SparkHiveHelper {



  def createHiveTableScanExec(requestedAttributes: Seq[Attribute],
                              relation: HiveTableRelation,
                              partitionPruningPred: Seq[Expression])
                             (sparkSession: SparkSession): HiveTableScanExec = {
    HiveTableScanExec(requestedAttributes, relation, partitionPruningPred)(sparkSession)
  }

  def createFileSinkDesc(tmpLocation: String, tableDesc: TableDesc): FileSinkDesc = {
    new FileSinkDesc(tmpLocation, tableDesc, false)
  }

  def getMetadata(polycatTable: io.polycat.catalog.common.model.Table): Properties = {
    val properties = new Properties()
    val sd = polycatTable.getStorageDescriptor
    properties.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT,
      sd.getInputFormat)
    properties.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_OUTPUT_FORMAT,
      sd.getOutputFormat)
    properties.setProperty(
      org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME,
      polycatTable.getDatabaseName + "." + polycatTable.getTableName)
    properties.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION,
      sd.getLocation)
    properties.setProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB, sd
      .getSerdeInfo.getSerializationLibrary)
    properties.putAll(sd.getSerdeInfo.getParameters)
    properties.putAll(polycatTable.getParameters)
    val partitionKeys = polycatTable.getPartitionKeys
    var partString = ""
    var partStringSep = ""
    var partTypesString = ""
    var partTypesStringSep = ""
    for (partKey <- partitionKeys) {
      partString = partString.concat(partStringSep)
      partString = partString.concat(partKey.getColumnName)
      partTypesString = partTypesString.concat(partTypesStringSep)
      partTypesString = partTypesString.concat(partKey.getColType)
      if (partStringSep.isEmpty) {
        partStringSep = "/"
        partTypesStringSep = ":"
      }
    }
    if (partString.nonEmpty) {
      properties.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, partString)
      properties.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES, partTypesString)
    }

    val cols = sd.getColumns

    val colNameBuf = new StringBuilder
    val colTypeBuf = new StringBuilder
    val colComment = new StringBuilder

    var first = true
    val columnNameDelimiter = getColumnNameDelimiter(cols)

    for (col <- cols) {
      if (!first) {
        colNameBuf.append(columnNameDelimiter)
        colTypeBuf.append(":")
        colComment.append('\0')
      }
      colNameBuf.append(col.getColumnName)
      colTypeBuf.append(col.getColType.toLowerCase)
      colComment.append(if (null != col.getComment) col.getComment
      else "")
      first = false
    }
    properties.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS, colNameBuf.toString)
    properties.setProperty(serdeConstants.COLUMN_NAME_DELIMITER, columnNameDelimiter)
    val colTypes = colTypeBuf.toString
    properties.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES, colTypes)
    properties.setProperty("columns.comments", colComment.toString)

    properties
  }

  def getColumnNameDelimiter(columns: java.util.List[Column]): String = {
    // we first take a look if any fieldSchemas contain COMMA
    for (col <- columns) {
      if (col.getColumnName.contains(","))
        return String.valueOf(SerDeUtils.COLUMN_COMMENTS_DELIMITER)
    }
    String.valueOf(SerDeUtils.COMMA)
  }
}
