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
package io.polycat.catalog.spark.execution

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{PolyCatPartitionTable, SparkHelper, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SerializableConfiguration

import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.immutable.ParVector
import scala.collection.{GenMap, GenSeq}
import scala.concurrent.duration.MILLISECONDS
import scala.jdk.CollectionConverters.{collectionAsScalaIterableConverter, mapAsJavaMapConverter}

/**
 * @author liangyouze
 * @date 2024/3/6
 */

case class RecoverPartitionsExec(
                                  table: PolyCatPartitionTable,
                                  sparkSession: SparkSession,
                                  enableAddPartitions: Boolean,
                                  enableDropPartitions: Boolean,
                                  cmd: String = "MSCK REPAIR TABLE") extends LeafV2CommandExec{

  val NUM_FILES = "numFiles"
  val TOTAL_SIZE = "totalSize"
  val DDL_TIME = "transient_lastDdlTime"

  override protected def run(): Seq[InternalRow] = {
    if (table.partitionSchema().isEmpty) {
      SparkHelper.throwCmdOnlyWorksOnPartitionedTablesError(cmd, table.name())
    }
    val location = table.properties().get(TableCatalog.PROP_LOCATION)
    if (StringUtils.isEmpty(location)) {
      SparkHelper.throwCmdOnlyWorksOnTableWithLocationError(cmd, table.name())
    }
    val root = new Path(location)
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val fs = root.getFileSystem(hadoopConf)
    val droppedAmount = if (enableDropPartitions) {
      dropPartitions(fs)
    } else 0
    val addedAmount = if (enableAddPartitions) {
      val threshold = sparkSession.sparkContext.getConf.get("spark.rdd.parallelListingThreshold", "10").toInt
      val pathFilter = SparkHelper.getPathFilter(hadoopConf)

      val evalPool = SparkHelper.newForkJoinPool("RepairTableCommand", 8)
      val partitionSpecsAndLocs: GenSeq[(Map[String, String], Path)] =
        try {
          val partitionNames = table.partitionSchema().map(field => field.name)
          scanPartitions(fs, pathFilter, root, Map(), partitionNames, threshold,
            sparkSession.sessionState.conf.resolver, new ForkJoinTaskSupport(evalPool)).seq
        } finally {
          evalPool.shutdown()
        }
      val total = partitionSpecsAndLocs.length
      logInfo(s"Found $total partitions in $root")

      val partitionStats = if (sparkSession.sessionState.conf.gatherFastStats) {
        gatherPartitionStats(partitionSpecsAndLocs, fs, pathFilter, threshold)
      } else {
        GenMap.empty[String, PartitionStatistics]
      }
      logInfo(s"Finished to gather the fast stats for all $total partitions.")

      addPartitions(partitionSpecsAndLocs, partitionStats)
      total
    } else 0
    Seq.empty
  }

  private def addPartitions(
      partitionSpecsAndLocs: GenSeq[(Map[String, String], Path)],
      partitionStats: GenMap[String, PartitionStatistics]): Unit = {
    val total = partitionSpecsAndLocs.length
    var done = 0L
    // Hive metastore may not have enough memory to handle millions of partitions in single RPC,
    // we should split them into smaller batches. Since Hive client is not thread safe, we cannot
    // do this in parallel.
    val batchSize = sparkSession.conf.get(SQLConf.ADD_PARTITION_BATCH_SIZE.key).toInt
    partitionSpecsAndLocs.toIterator.grouped(batchSize).foreach { batch =>
      val now = MILLISECONDS.toSeconds(System.currentTimeMillis())
      val parts = batch.map { case (spec, location) =>
        val params = partitionStats.get(location.toString).map {
          case PartitionStatistics(numFiles, totalSize) =>
            // This two fast stat could prevent Hive metastore to list the files again.
            Map(NUM_FILES -> numFiles.toString,
              TOTAL_SIZE -> totalSize.toString,
              // Workaround a bug in HiveMetastore that try to mutate a read-only parameters.
              // see metastore/src/java/org/apache/hadoop/hive/metastore/HiveMetaStore.java
              DDL_TIME -> now.toString)
        }.getOrElse(Map.empty)
        // inherit table storage format (possibly except for location)
        (InternalRow.fromSeq(spec.values.toSeq), params)
      }
      val partsTuple = parts.filter(part => !table.partitionExists(part._1)).unzip

      table.createPartitions(partsTuple._1.toArray, partsTuple._2.map(_.asJava).toArray)
      done += parts.length
      logDebug(s"Recovered ${parts.length} partitions ($done/$total so far)")
    }
  }

  private def gatherPartitionStats(
      partitionSpecsAndLocs: GenSeq[(Map[String, String], Path)],
      fs: FileSystem,
      pathFilter: PathFilter,
      threshold: Int): GenMap[String, PartitionStatistics] = {
    if (partitionSpecsAndLocs.length > threshold) {
      val hadoopConf = sparkSession.sessionState.newHadoopConf()
      val serializableConfiguration = new SerializableConfiguration(hadoopConf)
      val serializedPaths = partitionSpecsAndLocs.map(_._2.toString).toArray

      // Set the number of parallelism to prevent following file listing from generating many tasks
      // in case of large #defaultParallelism.
      val numParallelism = Math.min(serializedPaths.length,
        Math.min(sparkSession.sparkContext.defaultParallelism, 10000))
      // gather the fast stats for all the partitions otherwise Hive metastore will list all the
      // files for all the new partitions in sequential way, which is super slow.
      logInfo(s"Gather the fast stats in parallel using $numParallelism tasks.")
      sparkSession.sparkContext.parallelize(serializedPaths, numParallelism)
        .mapPartitions { paths =>
          val pathFilter = SparkHelper.getPathFilter(serializableConfiguration.value)
          paths.map(new Path(_)).map { path =>
            val fs = path.getFileSystem(serializableConfiguration.value)
            val statuses = fs.listStatus(path, pathFilter)
            (path.toString, PartitionStatistics(statuses.length, statuses.map(_.getLen).sum))
          }
        }.collectAsMap()
    } else {
      partitionSpecsAndLocs.map { case (_, location) =>
        val statuses = fs.listStatus(location, pathFilter)
        (location.toString, PartitionStatistics(statuses.length, statuses.map(_.getLen).sum))
      }.toMap
    }
  }

  private def scanPartitions(
      fs: FileSystem,
      filter: PathFilter,
      path: Path,
      spec: Map[String, String],
      partitionNames: Seq[String],
      threshold: Int,
      resolver: Resolver,
      evalTaskSupport: ForkJoinTaskSupport): GenSeq[(Map[String, String], Path)] = {
    if (partitionNames.isEmpty) {
      return Seq(spec -> path)
    }

    val statuses = fs.listStatus(path, filter)
    val statusPar: GenSeq[FileStatus] =
      if (partitionNames.length > 1 && statuses.length > threshold || partitionNames.length > 2) {
        // parallelize the list of partitions here, then we can have better parallelism later.
        val parArray = new ParVector(statuses.toVector)
        parArray.tasksupport = evalTaskSupport
        parArray.seq
      } else {
        statuses
      }
    statusPar.flatMap { st =>
      val name = st.getPath.getName
      if (st.isDirectory && name.contains("=")) {
        val ps = name.split("=", 2)
        val columnName = ExternalCatalogUtils.unescapePathName(ps(0))
        // TODO: Validate the value
        val value = ExternalCatalogUtils.unescapePathName(ps(1))
        if (resolver(columnName, partitionNames.head)) {
          scanPartitions(fs, filter, st.getPath, spec ++ Map(partitionNames.head -> value),
            partitionNames.drop(1), threshold, resolver, evalTaskSupport)
        } else {
          logWarning(
            s"expected partition column ${partitionNames.head}, but got ${ps(0)}, ignoring it")
          Seq.empty
        }
      } else {
        logWarning(s"ignore ${new Path(path, name)}")
        Seq.empty
      }
    }
  }

  private def dropPartitions(fs: FileSystem): Unit = {
    val dropPartitions = SparkHelper.parmap(
      table.listPartitionsByValues(List.empty),
      "RepairTableCommand: non-existing partitions",
      maxThreads = 8) { partition =>
      if (fs.exists(new Path(partition.getStorageDescriptor.getLocation))) {
        None
      } else {
        Some(InternalRow.fromSeq(partition.getPartitionValues.asScala.toSeq))
      }
    }.flatten
    table.dropPartitions(dropPartitions.toArray)
  }

  override def output: Seq[Attribute] = {
    Seq.empty
  }
}

case class PartitionStatistics(numFiles: Int, totalSize: Long)
