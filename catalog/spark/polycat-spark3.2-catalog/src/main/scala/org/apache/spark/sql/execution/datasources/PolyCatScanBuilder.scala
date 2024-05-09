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
package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.SupportFileFormat
import org.apache.spark.sql.connector.catalog.SupportFileFormat.{CSV, JSON, ORC, PARQUET, TEXT, TEXTFILE}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.csv.{CSVScan, CSVScanBuilder}
import org.apache.spark.sql.execution.datasources.v2.json.{JsonScan, JsonScanBuilder}
import org.apache.spark.sql.execution.datasources.v2.orc.{OrcScan, OrcScanBuilder}
import org.apache.spark.sql.execution.datasources.v2.parquet.{ParquetScan, ParquetScanBuilder}
import org.apache.spark.sql.execution.datasources.v2.text.{TextScan, TextScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class PolyCatScanBuilder(sparkSession: SparkSession,
                              fileIndex: PartitioningAwareFileIndex,
                              schema: StructType,
                              dataSchema: StructType,
                              options: CaseInsensitiveStringMap) {

  private def applyWithFilters(partitionFilters: Seq[Expression], fileIndex: PartitioningAwareFileIndex): Unit = {
    fileIndex match {
      case fileIndex: PolyCatFileIndex =>
        fileIndex.refresh1(partitionFilters)
      case _ =>
    }
  }

  def apply(fileFormat: String): ScanBuilder = SupportFileFormat.apply(fileFormat) match {
    case PARQUET =>
      new ParquetScanBuilder(sparkSession, fileIndex, schema, dataSchema, options) {
        override def build(): Scan = {
          new ParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(),
            readPartitionSchema(), pushedParquetFilters, options) {
            override def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan = {
              applyWithFilters(partitionFilters, fileIndex)
              super.withFilters(partitionFilters, dataFilters)
            }
          }
        }
      }
    case ORC =>
      new OrcScanBuilder(sparkSession, fileIndex, schema, dataSchema, options) {
        override def build(): Scan = {
          new OrcScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(),
            readPartitionSchema(), options, pushedFilters()) {
            override def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan = {
              applyWithFilters(partitionFilters, fileIndex)
              super.withFilters(partitionFilters, dataFilters)
            }
          }
        }
      }
    case TEXT | TEXTFILE =>
      new TextScanBuilder(sparkSession, fileIndex, schema, dataSchema, options) {
        override def build(): Scan = {
          new TextScan(sparkSession, fileIndex, readDataSchema(),
            readPartitionSchema(), options) {
            override def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan = {
              applyWithFilters(partitionFilters, fileIndex)
              super.withFilters(partitionFilters, dataFilters)
            }
          }
        }
      }
    case JSON =>
      new JsonScanBuilder(sparkSession, fileIndex, schema, dataSchema, options) {
        override def build(): Scan = {
          new JsonScan(
            sparkSession,
            fileIndex,
            dataSchema,
            readDataSchema(),
            readPartitionSchema(),
            options,
            pushedFilters()) {
            override def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan = {
              applyWithFilters(partitionFilters, fileIndex)
              super.withFilters(partitionFilters, dataFilters)
            }
          }
        }
      }
    case CSV =>
      new CSVScanBuilder(sparkSession, fileIndex, schema, dataSchema, options) {
        override def build(): Scan = {
          new CSVScan(
            sparkSession,
            fileIndex,
            dataSchema,
            readDataSchema(),
            readPartitionSchema(),
            options,
            pushedFilters()) {
            override def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan = {
              applyWithFilters(partitionFilters, fileIndex)
              super.withFilters(partitionFilters, dataFilters)
            }
          }
        }
      }
    /*case AVRO =>
      new AvroScanBuilder(sparkSession, fileIndex, columnSchema, dataSchema, options)*/
    case _ =>
      throw QueryCompilationErrors.unsupportedDataSourceTypeForDirectQueryOnFilesError(fileFormat)
  }
}
