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
import io.polycat.catalog.common.Constants
import io.polycat.catalog.common.model.Partition
import io.polycat.catalog.common.options.PolyCatOptions
import io.polycat.catalog.common.utils.PartitionUtil
import io.polycat.catalog.spark.PolyCatClientSparkHelper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.v2.FileBatchWrite
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, PartitioningUtils, WriteJobDescription, WriteTaskResult}
import org.apache.spark.sql.hive.HiveExternalCatalog.DATASOURCE_PROVIDER
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{SerializableConfiguration, Utils}

import java.util
import java.util.{Collections, UUID}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

class SparkWriteBuilder(paths: Seq[String],
                        formatName: String,
                        supportsDataType: DataType => Boolean,
                        table: PolyCatPartitionTable,
                        info: LogicalWriteInfo) extends WriteBuilder with SupportsDynamicOverwrite with SupportsOverwrite with Logging {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  def deleteSubDirDataFile(committer: FileCommitProtocol, fs: FileSystem, deletePath: Path, pathFilter: PathFilter, recursive: Boolean = false): Unit = {
    if (fs.exists(deletePath) && fs.getFileStatus(deletePath).isDirectory) {
      val files: Array[FileStatus] = fs.listStatus(deletePath, pathFilter)
      for (file <- files) {
        committer.deleteWithJob(fs, file.getPath, recursive = recursive)
      }
    }
  }

  private val sparkWrite: SparkWrite = new SparkWrite(paths, formatName, supportsDataType, table, info) {
    val caseSensitiveMap: Map[String, String] = options.asCaseSensitiveMap.asScala.toMap

    private def getCustomPartitionLocations(
                                             fs: FileSystem,
                                             qualifiedOutputPath: Path,
                                             partitions: List[Partition]): Map[Map[String, String], String] = {
      partitions.flatMap { p =>
        val spec = table.partitionSchema().map(_.name).zip(p.getPartitionValues).toMap
        val defaultLocation = qualifiedOutputPath.suffix(
          "/" + PartitioningUtils.getPathFragment(spec, table.partitionSchema())).toString
        val catalogLocation = new Path(p.getStorageDescriptor.getLocation).makeQualified(
          fs.getUri, fs.getWorkingDirectory).toString
        if (catalogLocation != defaultLocation) {
          Some(spec -> catalogLocation)
        } else {
          None
        }
      }.toMap
    }

    private def deleteMatchingPartitions(
                                          fs: FileSystem,
                                          qualifiedOutputPath: Path,
                                          customPartitionLocations: Map[TablePartitionSpec, String],
                                          committer: FileCommitProtocol): Unit = {
      val staticPartitionPrefix = if (staticPartitions.nonEmpty) {
        "/" + table.partitionSchema().flatMap { p =>
          staticPartitions.get(p.name).map(ExternalCatalogUtils.getPartitionPathString(p.name, _))
        }.mkString("/")
      } else {
        ""
      }
      // first clear the path determined by the static partition keys (e.g. /table/foo=1)
      val staticPrefixPath = qualifiedOutputPath.suffix(staticPartitionPrefix)
      if (fs.exists(staticPrefixPath) && !committer.deleteWithJob(fs, staticPrefixPath, true)) {
        throw QueryExecutionErrors.cannotClearOutputDirectoryError(staticPrefixPath)
      }
      // now clear all custom partition locations (e.g. /custom/dir/where/foo=2/bar=4)
      for ((spec, customLoc) <- customPartitionLocations) {
        assert(
          (staticPartitions.toSet -- spec).isEmpty,
          "Custom partition location did not match static partitioning keys")
        val path = new Path(customLoc)
        if (fs.exists(path) && !committer.deleteWithJob(fs, path, true)) {
          throw QueryExecutionErrors.cannotClearPartitionDirectoryError(path)
        }
      }
    }

    override def toBatch: BatchWrite = {
      val sparkSession = SparkSession.active
      validateInputs(sparkSession.sessionState.conf.caseSensitiveAnalysis)
      val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)

      val outputPath = new Path(paths.head)
      val fs = outputPath.getFileSystem(hadoopConf)
      val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
      val jobId = java.util.UUID.randomUUID().toString
      dynamicPartitionOverwrite = overwriteByFilter && (
        (isDynamicMode && staticPartitions.size < table.partitionSchema().size) ||
          (staticPartitions.isEmpty && isPartitionTable)
        )
      val job = getJobInstance(hadoopConf, if (dynamicPartitionOverwrite) {
        FileCommitProtocol.getStagingDir(outputPath.toString, jobId)
          .makeQualified(fs.getUri, fs.getWorkingDirectory)
      } else {
        qualifiedOutputPath
      })

      val committer = FileCommitProtocol.instantiate(
        sparkSession.sessionState.conf.fileCommitProtocolClass,
        jobId = jobId,
        outputPath = paths.head,
        dynamicPartitionOverwrite)

      matchingPartitions = table.listPartitionsByValues(staticPartitions.values.toList)
      val customPartitionLocations =
        getCustomPartitionLocations(qualifiedOutputPath.getFileSystem(hadoopConf), qualifiedOutputPath, matchingPartitions)

      lazy val description =
        createWriteJobDescription(sparkSession, hadoopConf, job, paths.head, customPartitionLocations, options.asScala.toMap)

      if (overwriteByFilter && !dynamicPartitionOverwrite) {
        deleteMatchingPartitions(fs, qualifiedOutputPath, customPartitionLocations, committer)
      }

      committer.setupJob(job)



      /*val pathFilter = SparkHelper.getPathFilter(hadoopConf)
      if (overwriteByFilter) {
        var deletePath = new Path(locs.head)
        if (isPartitionTable) {
          if (!dynamicPartitionOverwrite) {
            if (staticPartitions.nonEmpty) {
              deletePath = new Path(paths.head + Path.SEPARATOR + staticPartitions.map(v => ExternalCatalogUtils.getPartitionPathString(v._1, v._2.toString)).mkString(Path.SEPARATOR))
            }
            deleteSubDirDataFile(committer, fs, deletePath, pathFilter, recursive = true)
          }
        } else {
          deleteSubDirDataFile(committer, fs, deletePath, pathFilter)
        }
      }*/

      new FileBatchWrite(job, description, committer) {

        def getExistsPartitions: Set[InternalRow] = {
          val existsPartitions: Array[InternalRow] = table.listPartitionIdentifiers(staticPartitions.keys.toArray[String], InternalRow.fromSeq(staticPartitions.values.map(UTF8String.fromString).toArray[UTF8String]))
          logInfo(s"Get exists partitions: ${existsPartitions.mkString("Array(", ", ", ")")}")
          existsPartitions.toSet
        }

        def parsePathFragmentAsIdent(pathFragment: String): InternalRow = {
          InternalRow.fromSeq(PartitioningUtils.parsePathFragmentAsSeq(pathFragment).map(_._2).map(UTF8String.fromString))
        }

        def addPartitions(specs: Set[InternalRow]): Unit = {
          if (specs.size <= 0) return
          logInfo(s"Start commit add partitions: $specs")
          table.createPartitions(specs.toArray, Array.tabulate(specs.size)(_ => {
            val properties = new util.HashMap[String, String]()
            PolyCatClientHelper.adaptCommonParameters(properties)
          }))
          logInfo(s"End commit add partitions: $specs, success.")
        }

        def dropPartitions(specs: Set[InternalRow]): Boolean = {
          if (specs.size <= 0) return true
          logInfo(s"Start commit drop partitions: $specs")
          val b = table.dropPartitions(specs.toArray)
          logInfo(s"End commit drop partitions: $specs, success.")
          b
        }

        def refreshUpdatedPartitions(updatedPartitionPaths: Set[String]): Unit = {
          if (isPartitionTable) {
            val partitions = PolyCatClientHelper.getPartitionsByNames(table.getClient, table.getTableName, updatedPartitionPaths.toArray)
            val partitionNames = partitions.map(p => {
              PolyCatClientHelper.makePartitionName(PolyCatClientSparkHelper.getPartitionKeys(table.partitionSchema()), p.getPartitionValues)
            }).toSeq
            val newPartitions = updatedPartitionPaths.filter(p => !partitionNames.contains(p))
              .map(name => (InternalRow.fromSeq(PartitionUtil.convertNameToVals(name).toSeq), Collections.emptyMap[String, String]()))
              .toMap
            table.createPartitions(newPartitions.keys.toArray, newPartitions.values.toArray)
            // For dynamic partition overwrite, we never remove partitions but only update existing
            // ones.
            if (overwriteByFilter && !dynamicPartitionOverwrite) {
              val deletedPartitions = matchingPartitions.map(p => {
                  PolyCatClientHelper.makePartitionName(PolyCatClientSparkHelper.getPartitionKeys(table.partitionSchema()), p.getPartitionValues)
                }).filter(!partitionNames.contains(_))
                .map(name => InternalRow.fromSeq(PartitionUtil.convertNameToVals(name).toSeq))
                .toArray
              table.dropPartitions(deletedPartitions)
            }
          }
        }

        override def commit(messages: Array[WriterCommitMessage]): Unit = {
          super.commit(messages)

          val results = messages.map(_.asInstanceOf[WriteTaskResult])
          val updatedPartitionNames = results.map(r => r.summary.updatedPartitions).reduceOption(_ ++ _).getOrElse(Set.empty)
          if (updatedPartitionNames.isEmpty && staticPartitions.nonEmpty
            && table.partitionSchema().length == staticPartitions.size) {
            // Avoid empty static partition can't loaded to datasource table.
            val staticPathFragment =
              PartitioningUtils.getPathFragment(staticPartitions, table.partitionSchema())
            refreshUpdatedPartitions(Set(staticPathFragment))
          } else {
            refreshUpdatedPartitions(updatedPartitionNames)
          }

          /*if (isPartitionTable) {
            if (overwriteByFilter) {
              if (partSpecIdents.size() == 1) {
                val partProperties = new util.HashMap[String, String]()
                partProperties.put(Constants.LOCATION, locs.head)
                partProperties.put(PolyCatOptions.PARTITION_OVERWRITE.key(), "true")
                table.createPartition(partSpecIdents.head, partProperties)
              } else {
                val partitionPaths: Set[String] = messages.map(_.asInstanceOf[WriteTaskResult]).map(_.summary.updatedPartitions).reduceOption(_ ++ _).getOrElse(Set.empty)
                logInfo(s"partitionPaths= ${partitionPaths}")
                val updatedPartitions: Set[InternalRow] = partitionPaths.map(parsePathFragmentAsIdent)
                val existsPartitions = getExistsPartitions
                addPartitions(updatedPartitions -- existsPartitions)
                if (!dynamicPartitionOverwrite) {
                  dropPartitions(existsPartitions -- updatedPartitions)
                }
              }
            }
          }*/

          /*if (isPartitionTable) {
            if (!dynamicPartitionOverwrite) {

            }
          }*/


        }
      }
    }
  }
  override def build(): Write = {
    sparkWrite
  }

  override def overwriteDynamicPartitions(): WriteBuilder = {
    sparkWrite.isDynamicMode = sparkWrite.isPartitionTable
    sparkWrite.overwriteByFilter = true
    this
  }


  def setStaticPartitionSpec(filters: Array[Filter], partNames: Seq[String]): Unit = {
    filters.map {
      case EqualNullSafe(attribute, value) =>
        if (value != null && attribute != null && partNames.contains(attribute)) {
          sparkWrite.staticPartitions += (attribute -> value.toString)
        } else {
          throw new IllegalArgumentException(s"Unknown filter attribute: $attribute")
        }
      case EqualTo(attribute, value) =>
        if (partNames.contains(attribute)) {
          sparkWrite.staticPartitions += (attribute -> value.toString)
        } else {
          throw new IllegalArgumentException(s"Unknown filter attribute: $attribute")
        }
      case _ => None
    }
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    logInfo(s"overwrite filters: ${filters.mkString("Array(", ", ", ")")}")
    sparkWrite.overwriteByFilter = true
    val partNames: Seq[String] = table.partitioning().toSeq.asPartitionColumns
    setStaticPartitionSpec(filters, partNames)
    /*if (filters.length == 1 && filters(0).isInstanceOf[AlwaysTrue] || partNames.isEmpty) {
      if (isPartitionTable &&
        PartitionOverwriteMode.DYNAMIC != SparkSession.active.sqlContext.conf.partitionOverwriteMode) {
        logInfo(s"${SQLConf.PARTITION_OVERWRITE_MODE}=${SparkSession.active.sqlContext.conf.partitionOverwriteMode}.")
        // throw new SparkException(s"Dynamic partition is disabled. Either enable it by setting ${PARTITION_OVERWRITE_MODE.doc}")
      }
      return this
    }*/
    /*if (filters.length != partNames.length) {
      return this
    }*/
  /*  val partVals = new util.ArrayList[Any]()
    partNames.zipWithIndex.foreach {
      case (name, index) =>
        filters(index) match {
          case EqualNullSafe(attribute, value) =>
            if (value != null && attribute != null && name.equals(attribute)) {
              partVals.add(value)
            } else {
              throw new IllegalArgumentException(s"Unknown filter attribute: $attribute")
            }
          case EqualTo(attribute, value) =>
            if (name.equals(attribute)) {
              partVals.add(value)
            } else {
              throw new IllegalArgumentException(s"Unknown filter attribute: $attribute")
            }
          case _ => None
        }
    }

    partSpecIdents.add(InternalRow.fromSeq(partVals.map(value => UTF8String.fromString(value.toString)).toSeq))
    val partSpecPath = paths.head + Path.SEPARATOR + partNames.zip(partVals).map {
      case (k, v) => ExternalCatalogUtils.getPartitionPathString(k, v.toString)
    }.mkString(Path.SEPARATOR)

    locs = Seq(partSpecPath)*/
    this
  }
}
