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

import io.polycat.catalog.client.{Client, PolyCatClientHelper}
import io.polycat.catalog.common.model.{Partition, TableName}
import io.polycat.catalog.common.options.PolyCatOptions
import io.polycat.catalog.spark.PolyCatClientSparkHelper
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, PartitionAlreadyExistsException}
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.connector.catalog.SupportFileFormat._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.csv.CSVDataSource
import org.apache.spark.sql.execution.datasources.json.JsonDataSource
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.execution.streaming.{FileStreamSink, MetadataLogFileIndex}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import java.util
import scala.collection.JavaConverters.setAsJavaSetConverter
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.jdk.CollectionConverters.{mapAsScalaMapConverter, seqAsJavaListConverter}

class PolyCatPartitionTable(client: Client,
                            tableName: TableName,
                            schema: StructType,
                            partitionKeysSchema: StructType,
                            sparkSession: SparkSession,
                            fileFormat: String,
                            paths: Seq[String],
                            options: CaseInsensitiveStringMap,
                            userSpecifiedSchema: Option[StructType])
  extends SupportsAtomicPartitionManagement with SupportsRead with SupportsWrite with Logging {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  def getPartitionValues(ident: InternalRow): util.List[String] = {
    ident.toSeq(schema).map(_.toString).asJava
  }

  override def createPartition(ident: InternalRow, properties: util.Map[String, String]): Unit = {
    if (partitionExists(ident)) {
      if (PolyCatOptions.getValue(properties, PolyCatOptions.PARTITION_OVERWRITE)) {
        dropPartition(ident)
      } else throw new PartitionAlreadyExistsException(name(), ident, partitionKeysSchema)
    }
    PolyCatClientSparkHelper.createPartition(client, tableName, partitionKeysSchema, getPartitionValues(ident), properties, paths.head)
  }

  override def dropPartition(ident: InternalRow): Boolean = {
    if(partitionExists(ident)) {
      PolyCatClientSparkHelper.dropPartition(client, tableName, partitionKeysSchema, getPartitionValues(ident))
    } else false
  }

  override def replacePartitionMetadata(ident: InternalRow, properties: util.Map[String, String]): Unit = {
    if(partitionExists(ident)) {
      PolyCatClientSparkHelper.alterPartitionMetadata(client, tableName, partitionKeysSchema, getPartitionValues(ident), properties)
    } else {
      throw new NoSuchPartitionException(name(), ident, partitionKeysSchema)
    }
  }

  override def loadPartitionMetadata(ident: InternalRow): util.Map[String, String] = {
    PolyCatClientSparkHelper.printLog("loadPartitionMetadata", ident)
    val partitionSpec = PolyCatClientSparkHelper.getPartition(client, tableName, partitionKeysSchema, getPartitionValues(ident))
    if(partitionSpec != null) {
      partitionSpec.getParameters
    } else {
      throw new NoSuchPartitionException(name(), ident, partitionKeysSchema)
    }
  }

  def listPartitionsByValues(values: List[String]): List[Partition] = {
    PolyCatClientHelper.listPartitionsWithValues(client, tableName, values.asJava).asScala.toList
  }

  def listPartitionsPathByNames(idents: Array[InternalRow]): List[PartitionPath] = {
    val partitions = PolyCatClientHelper.getPartitionsByNames(client, tableName, idents.map(getPartitionValues).map(partValues => PolyCatClientHelper.makePartitionName(PolyCatClientSparkHelper.getPartitionKeys(partitionKeysSchema), partValues))).asScala.toList
    partitions.map(partition => {
      PartitionPath(InternalRow.fromSeq(partition.getPartitionValues.map(value => UTF8String.fromString(value)).toSeq), partition.getStorageDescriptor.getLocation)
    })
  }

  override def listPartitionIdentifiers(names: Array[String], ident: InternalRow): Array[InternalRow] = {
    assert(names.length == ident.numFields,
      s"Number of partition names (${names.length}) must be equal to " +
        s"the number of partition values (${ident.numFields}).")
    val schema: StructType = partitionKeysSchema
    assert(names.forall(fieldName => schema.fieldNames.contains(fieldName)),
      s"Some partition names ${names.mkString("[", ", ", "]")} don't belong to " +
        s"the partition schema '${schema.sql}'.")
    val valueSet = getPartitionValues(ident)
    val partValuesFilter = new util.ArrayList[String]()
    schema.foreach(pk => {
      if (names.contains(pk.name)) {
        partValuesFilter.add(valueSet.get(names.indexOf(pk.name)))
      } else {
        partValuesFilter.add("")
      }
    })
    val partitionNames = PolyCatClientSparkHelper.listPartitionNames(client, tableName, partValuesFilter)
    val listPartitions = partitionNames.map(PartitioningUtils.parsePathFragmentAsSeq)

    listPartitions.map(partVals => {
      InternalRow.fromSeq(partVals.map(value => UTF8String.fromString(value._2)))
    }).toArray
  }

  override def name(): String = tableName.getQualifiedName

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.STREAMING_WRITE,
    TableCapability.OVERWRITE_BY_FILTER,
    TableCapability.OVERWRITE_DYNAMIC,
    TableCapability.TRUNCATE).asJava

  override def createPartitions(idents: Array[InternalRow], properties: Array[util.Map[String, String]]): Unit = {
    val tuples = idents.map(getPartitionValues).zip(properties)
    PolyCatClientSparkHelper.createPartitions(client, tableName, partitionKeysSchema, tuples, paths.head);
  }

  override def dropPartitions(idents: Array[InternalRow]): Boolean = {
    PolyCatClientSparkHelper.dropPartitions(client, tableName, partitionKeysSchema, idents.map(getPartitionValues).toList.asJava)
  }

  override def partitionExists(ident: InternalRow): Boolean = {
    val partitionNames = partitionSchema().names
    if (ident.numFields == partitionNames.length) {
      PolyCatClientSparkHelper.doesPartitionExist(client, tableName, getPartitionValues(ident))
    }
    else throw new IllegalArgumentException("The number of fields (" + ident.numFields + ") in the partition identifier is not equal to the partition schema length (" + partitionNames.length + "). The identifier might not refer to one partition.")
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new SparkWriteBuilder(paths, fileFormat, supportsDataType, this, info)
  }

  def supportsDataType(dataType: DataType): Boolean = WriteHelper.supportsDataType(fileFormat, dataType)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    PolyCatScanBuilder(sparkSession, fileIndex, schema, dataSchema, options).apply(fileFormat)

  lazy val fileIndex: PartitioningAwareFileIndex = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    if (FileStreamSink.hasMetadata(paths, hadoopConf, sparkSession.sessionState.conf)) {
      // We are reading from the results of a streaming query. We will load files from
      // the metadata log instead of listing them using HDFS APIs.
      new MetadataLogFileIndex(sparkSession, new Path(paths.head),
        options.asScala.toMap, userSpecifiedSchema)
    } else {
      // This is a non-streaming file based datasource.
      val rootPathsSpecified = DataSource.checkAndGlobPathIfNecessary(paths, hadoopConf,
        checkEmptyGlobPath = true, checkFilesExist = false, enableGlobbing = globPaths)
      val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
      new PolyCatFileIndex(
         this, sparkSession, rootPathsSpecified, caseSensitiveMap, userSpecifiedSchema, fileStatusCache, Option(PartitionSpec(partitionKeysSchema, Seq())))
//        this, sparkSession, rootPathsSpecified, caseSensitiveMap, fileStatusCache)
    }
  }

  private def globPaths: Boolean = {
    val entry = options.get(DataSource.GLOB_PATHS_KEY)
    Option(entry).forall(_ == "true")
  }

  lazy val dataSchema: StructType = {
    val schema = userSpecifiedSchema.map { schema =>
      val partitionSchema = partitionKeysSchema
      val resolver = sparkSession.sessionState.conf.resolver
      StructType(schema.filterNot(f => partitionSchema.exists(p => resolver(p.name, f.name))))
    }.orElse {
      inferSchema(fileIndex.allFiles())
    }.getOrElse {
      throw QueryCompilationErrors.dataSchemaNotSpecifiedError(fileFormat)
    }

    fileIndex match {
      case _: MetadataLogFileIndex => schema
      case _ => schema.asNullable
    }
  }
  override def partitioning(): Array[Transform] = {
    partitionKeysSchema.names.toSeq.asTransforms
  }


  def inferSchema(files: Seq[FileStatus]): Option[StructType] = SupportFileFormat.apply(fileFormat) match {
    case PARQUET =>
      ParquetUtils.inferSchema(sparkSession, options.asScala.toMap, files)
    case ORC =>
      OrcUtils.inferSchema(sparkSession, files, options.asScala.toMap)
    case TEXT | TEXTFILE =>
      Some(StructType(Seq(StructField("value", StringType))))
    case JSON =>
      val parsedOptions = new JSONOptionsInRead(
        options.asScala.toMap,
        sparkSession.sessionState.conf.sessionLocalTimeZone,
        sparkSession.sessionState.conf.columnNameOfCorruptRecord)
      JsonDataSource(parsedOptions).inferSchema(
        sparkSession, files, parsedOptions)
    case CSV =>
      val parsedOptions = new CSVOptions(
        options.asScala.toMap,
        columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
        sparkSession.sessionState.conf.sessionLocalTimeZone)
      CSVDataSource(parsedOptions).inferSchema(sparkSession, files, parsedOptions)
/*    case AVRO =>
      AvroUtils.inferSchema(sparkSession, options.asScala.toMap, files)*/
    case _ =>
      throw QueryCompilationErrors.unsupportedDataSourceTypeForDirectQueryOnFilesError(fileFormat)
  }

  override def schema(): StructType = schema

  override def properties(): util.Map[String, String] = options

  override def partitionSchema(): StructType = partitionKeysSchema

  def getClient: Client = client
  def getTableName: TableName = tableName
  def getSparkSession: SparkSession = sparkSession

  def getFileFormat(): SupportFileFormat = SupportFileFormat.apply(fileFormat)
}
