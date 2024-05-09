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

import io.polycat.catalog.client.PolyCatClientHelper
import io.polycat.catalog.common.model.Partition
import io.polycat.catalog.spark.PolyCatClientSparkHelper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.orc.{OrcUtils => _}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.connector.catalog.SupportFileFormat._
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{NullOrdering, SortDirection, SortOrder, SortValue}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, RequiresDistributionAndOrdering}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.csv.CSVWrite
import org.apache.spark.sql.execution.datasources.v2.json.JsonWrite
import org.apache.spark.sql.execution.datasources.v2.orc.OrcWrite
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetWrite
import org.apache.spark.sql.execution.datasources.v2.text.TextWrite
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, DataSource, OutputWriterFactory, WriteJobDescription}
import org.apache.spark.sql.hive.HiveExternalCatalog.DATASOURCE_PROVIDER
import org.apache.spark.sql.hive.SparkHiveHelper
import org.apache.spark.sql.hive.execution.HiveFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.SerializableConfiguration

import java.util.{Properties, UUID}


abstract class SparkWrite(paths: Seq[String],
                          formatName: String,
                          supportsDataType: DataType => Boolean,
                          table: PolyCatPartitionTable,
                          info: LogicalWriteInfo) extends RequiresDistributionAndOrdering {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._


  val isPartitionTable = table.partitioning().nonEmpty
  var overwriteByFilter = false
  var staticPartitions: Map[String, String] = Map.empty

  var isDynamicMode = isPartitionTable && !table.properties().containsKey(DATASOURCE_PROVIDER)

  var dynamicPartitionOverwrite = false

  var matchingPartitions: List[Partition] = List.empty

  protected val schema = table.dataSchema
  protected val queryId = info.queryId()
  protected val options = info.options()

  def prepareWrite(sqlConf: SQLConf, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    SupportFileFormat.apply(formatName) match {
      case PARQUET => ParquetWrite(paths, formatName, supportsDataType, info).prepareWrite(sqlConf, job, options, dataSchema)
      case CSV => CSVWrite(paths, formatName, supportsDataType, info).prepareWrite(sqlConf, job, options, dataSchema)
      case JSON => JsonWrite(paths, formatName, supportsDataType, info).prepareWrite(sqlConf, job, options, dataSchema)
      case TEXT | TEXTFILE =>
        val polycatTable = PolyCatClientHelper.getTable(table.getClient, table.getTableName)
        val sd = polycatTable.getStorageDescriptor
        val tableDesc = new TableDesc(Class.forName(sd.getInputFormat).asInstanceOf[Class[_ <: InputFormat[_, _]]],
          Class.forName(sd.getOutputFormat), SparkHiveHelper.getMetadata(polycatTable))
        val fileSinkConf = SparkHiveHelper.createFileSinkDesc(job.getWorkingDirectory.toString, tableDesc)
        new HiveFileFormat(fileSinkConf).prepareWrite(table.getSparkSession, job, options, dataSchema)
      case ORC => OrcWrite(paths, formatName, supportsDataType, info).prepareWrite(sqlConf, job, options, dataSchema)
      // case AVRO => AvroWrite(paths, formatName, supportsDataType, info).prepareWrite(sqlConf, job, options, dataSchema)
    }

  }

  def getPartitionColumns: Seq[Attribute] = {
    val partitionColumnNames = table.partitioning().toSeq.asPartitionColumns
    new StructType(schema.filter(p => partitionColumnNames.contains(p.name)).toArray).toAttributes
  }

  def adapterHadoopConf(sparkConf: SQLConf): Unit = {
    val configuration = SparkHadoopUtil.newConfiguration(new SparkConf(false))
    val confIterator = configuration.iterator()
    while (confIterator.hasNext) {
      val value = confIterator.next()
      PolyCatClientSparkHelper.printLog("confIterator", value.getKey + "=" + value.getValue)
      sparkConf.setConfString(value.getKey, value.getValue)
    }
  }

  protected def createWriteJobDescription(
                                         sparkSession: SparkSession,
                                         hadoopConf: Configuration,
                                         job: Job,
                                         pathName: String,
                                         customPartitionLocations: Map[Map[String, String], String],
                                         options: Map[String, String]) = {
    val caseInsensitiveOptions = CaseInsensitiveMap(options)
    val sparkConf = sparkSession.sessionState.conf
    // adapterHadoopConf(sparkConf)
    // Note: prepareWrite has side effect. It sets "job".
    val outputWriterFactory =
      prepareWrite(sparkSession.sessionState.conf, job, caseInsensitiveOptions, schema)
    val partitionSchema = table.partitionSchema()
    val allColumns = table.schema().toAttributes
    val metrics = BasicWriteJobStatsTracker.metrics
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val statsTracker = new BasicWriteJobStatsTracker(serializableHadoopConf, metrics)

    val jobDescription = new WriteJobDescription(
      uuid = UUID.randomUUID().toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = allColumns,
      dataColumns = allColumns.filter(attribute => !partitionSchema.exists(partition => partition.name == attribute.name)),
      partitionColumns = allColumns.filter(attribute => partitionSchema.exists(partition => partition.name == attribute.name)),
      // dataColumns = allColumns,
      // partitionColumns = allColumns.filter(c => table.partitioning().toSeq.asPartitionColumns.contains(c.name)),
      bucketIdExpression = None,
      path = pathName,
      customPartitionLocations = customPartitionLocations,
      maxRecordsPerFile = caseInsensitiveOptions.get("maxRecordsPerFile").map(_.toLong)
        .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone),
      statsTrackers = Seq(statsTracker)
    )
    jobDescription
  }

  protected def validateInputs(caseSensitiveAnalysis: Boolean): Unit = {
    assert(schema != null, "Missing input data schema")
    assert(queryId != null, "Missing query ID")

    if (paths.length != 1) throw new IllegalArgumentException("Expected exactly one path to be specified, but " +
      s"got: ${paths.mkString(", ")}")
    val pathName = paths.head
    SchemaUtils.checkColumnNameDuplication(schema.fields.map(_.name),
      s"when inserting into $pathName", caseSensitiveAnalysis)
    DataSource.validateSchema(schema)

    // TODO: [SPARK-36340] Unify check schema filed of DataSource V2 Insert.
    val format = SupportFileFormat.apply(formatName)
    schema.foreach { field =>
      if (format != TEXTFILE && format != TEXT && !supportsDataType(field.dataType)) throw QueryCompilationErrors.dataTypeUnsupportedByDataSourceError(formatName, field)
    }
  }

  protected def getJobInstance(hadoopConf: Configuration, path: Path) = {
    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, path)
    job
  }

  override def requiredDistribution(): Distribution = {
    Distributions.unspecified()
  }
  override def requiredOrdering(): Array[SortOrder] = {
    table.partitioning().map(a => SortValue(a, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)).toArray
  }
}
