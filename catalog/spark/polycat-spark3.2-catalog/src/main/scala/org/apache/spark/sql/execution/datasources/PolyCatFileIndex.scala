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

import io.polycat.catalog.client.PolyCatClientHelper
import io.polycat.catalog.common.model.Partition
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BoundReference, Cast, Expression, Literal, Predicate}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.connector.catalog.{PolyCatPartitionTable, SparkHelper}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.streaming.FileStreamSink
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.HadoopFSUtils

import java.util
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

class PolyCatFileIndex(
                        table: PolyCatPartitionTable,
                        sparkSession: SparkSession,
                        rootPathsSpecified: Seq[Path],
                        parameters: Map[String, String],
                        userSpecifiedSchema: Option[StructType],
                        fileStatusCache: FileStatusCache = NoopCache,
                        userSpecifiedPartitionSpec: Option[PartitionSpec] = None,
                        override val metadataOpsTimeNs: Option[Long] = None)
  extends PartitioningAwareFileIndex(sparkSession, parameters, userSpecifiedSchema, fileStatusCache) {
  // Filter out streaming metadata dirs or files such as "/.../_spark_metadata" (the metadata dir)
  // or "/.../_spark_metadata/0" (a file in the metadata dir). `rootPathsSpecified` might contain
  // such streaming metadata dir or files, e.g. when after globbing "basePath/*" where "basePath"
  // is the output of a streaming query.
  override val rootPaths =
  rootPathsSpecified.filterNot(FileStreamSink.ancestorIsMetadataDirectory(_, hadoopConf))

  @volatile private var cachedLeafFiles: mutable.LinkedHashMap[Path, FileStatus] = _
  @volatile private var cachedLeafDirToChildrenFiles: Map[Path, Array[FileStatus]] = _
  @volatile private var cachedPartitionSpec: PartitionSpec = _
  @volatile private var cachePartitionPredicates: Seq[Expression] = _
  @volatile private var cachePartitionPaths: Seq[PartitionPath] = _
  @volatile private var dataSize: Long = 0L

  // refresh1()

  override def partitionSpec(): PartitionSpec = {
    if (userSpecifiedPartitionSpec.isDefined) {
      cachedPartitionSpec = userSpecifiedPartitionSpec.get
    } else {
      cachedPartitionSpec = inferPartitioning()
    }
    logTrace(s"Partition spec: $cachedPartitionSpec")
    cachedPartitionSpec
  }

  override def allFiles(): Seq[FileStatus] = {
    super.allFiles()
  }

  override protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus] = {
    cachedLeafFiles
  }

  override def sizeInBytes: Long = {
    dataSize
  }

  override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = {
    cachedLeafDirToChildrenFiles
  }

  override def refresh(): Unit = {
    fileStatusCache.invalidateAll()
    refresh1(Nil)
  }

  private def refresh0(): Unit = {
    val files = listLeafFiles(rootPaths)
    cachedLeafFiles =
      new mutable.LinkedHashMap[Path, FileStatus]() ++= files.map(f => f.getPath -> f)
    cachedLeafDirToChildrenFiles = files.toArray.groupBy(_.getPath.getParent)
    cachedPartitionSpec = null
  }

  def refresh1(predicates: Seq[Expression]): Unit = {
    val files = if (table.partitionSchema().nonEmpty) {
      val partitionsPath: Seq[PartitionPath] = prunePartitions(predicates)
      listLeafFiles(partitionsPath.map(p => p.path))
    } else listLeafFiles(rootPaths)
    cachedLeafFiles =
      new mutable.LinkedHashMap[Path, FileStatus]() ++= files.map(f => f.getPath -> f)
    cachedLeafDirToChildrenFiles = files.toArray.groupBy(_.getPath.getParent)
    cachedPartitionSpec = null
    dataSize = super.sizeInBytes
    if (cachedLeafDirToChildrenFiles != null && cachedLeafDirToChildrenFiles.nonEmpty) {
      logInfo(s"""\n
        Estimate scan table: ${table.getTableName.getQualifiedName} \t
          list paths size: ${cachedLeafDirToChildrenFiles.size} \t
          head path: ${cachedLeafDirToChildrenFiles.keys.head} \t
          data file sizeInBytes: ${dataSize}\n""")
    }
  }

  // SPARK-15895: Metadata files (e.g. Parquet summary files) and temporary files should not be
  // counted as data files, so that they shouldn't participate partition discovery.
  private def isDataPath(path: Path): Boolean = {
    val name = path.getName
    !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
  }

  def getPartitionValues(partitionColumns: StructType, ident: InternalRow): Seq[String] = {
    ident.toSeq(partitionColumns).map(_.toString)
  }

  def getPartitionPathByIdent(partitionColumns: StructType, partitionColumnNames: Set[String], ident: InternalRow): PartitionPath = {
    new PartitionPath(ident,
      new Path(rootPaths.head + Path.SEPARATOR + partitionColumnNames.zip(getPartitionValues(partitionColumns, ident)).map(v => ExternalCatalogUtils.getPartitionPathString(v._1, v._2.toString)).mkString(Path.SEPARATOR)))
  }

  def prunePartitionsByFilter(partitions: util.List[Partition], predicates: Seq[Expression]): Seq[PartitionPath] = {
    val fs = rootPaths.head.getFileSystem(hadoopConf)
    cachePartitionPaths = SparkHelper.prunePartitionsByFilter(table, fs, partitions, predicates)
    cachePartitionPaths
  }

  def getCachePartitionPaths(): Seq[PartitionPath] = {
    cachePartitionPaths
  }

  private def prunePartitions(predicates: Seq[Expression]): Seq[PartitionPath] = {
    val partitionFilters = SparkHelper.convertFilters(predicates)
    if (cachePartitionPredicates != predicates) {
      logInfo(s"Prune partitions predicates: ${predicates}, filter phase: ${partitionFilters}")
      val partitions = PolyCatClientHelper.getPartitionsByFilter(table.getClient, table.getTableName, partitionFilters)
      cachePartitionPaths = prunePartitionsByFilter(partitions, predicates)
      cachePartitionPredicates = predicates
      logInfo {
        val total = partitions.size()
        val selectedSize = cachePartitionPaths.length
        val percentPruned = (1 - selectedSize.toDouble / total.toDouble) * 100
        s"Prune hit partitions $selectedSize partitions out of $total, " +
          s"pruned ${if (total == 0) "0" else s"$percentPruned%"} partitions."
      }
    }
    cachePartitionPaths
  }

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    if (cachedLeafFiles == null) {
      refresh1(partitionFilters)
    }

    def isNonEmptyFile(f: FileStatus): Boolean = {
      isDataPath(f.getPath) && f.getLen > 0
    }
    val selectedPartitions = if (partitionSpec().partitionColumns.isEmpty) {
      PartitionDirectory(InternalRow.empty, allFiles().filter(isNonEmptyFile)) :: Nil
    } else {
      if (recursiveFileLookup) {
        throw new IllegalArgumentException(
          "Datasource with partition do not allow recursive file loading.")
      }

      prunePartitions(partitionFilters).map {
        case PartitionPath(values, path) =>
          val files: Seq[FileStatus] = leafDirToChildrenFiles.get(path) match {
            case Some(existingDir) =>
              // Directory has children files in it, return them
              existingDir.filter(f => matchPathPattern(f) && isNonEmptyFile(f))

            case None =>
              // Directory does not exist, or has no children files
              Nil
          }
          PartitionDirectory(values, files)
      }
    }

    logTrace("Selected files after partition pruning:\n\t" + selectedPartitions.mkString("\n\t"))
    selectedPartitions
  }

  override def equals(other: Any): Boolean = other match {
    case hdfs: PolyCatFileIndex => rootPaths.toSet == hdfs.rootPaths.toSet
    case _ => false
  }

  override def hashCode(): Int = rootPaths.toSet.hashCode()

  /**
   * List leaf files of given paths. This method will submit a Spark job to do parallel
   * listing whenever there is a path having more files than the parallel partition discovery
   * discovery threshold.
   *
   * This is publicly visible for testing.
   */
  def listLeafFiles(paths: Seq[Path]): mutable.LinkedHashSet[FileStatus] = {
    val startTime = System.nanoTime()
    val output = mutable.LinkedHashSet[FileStatus]()
    val pathsToFetch = mutable.ArrayBuffer[Path]()
    for (path <- paths) {
      fileStatusCache.getLeafFiles(path) match {
        case Some(files) =>
          HiveCatalogMetrics.incrementFileCacheHits(files.length)
          output ++= files
        case None =>
          pathsToFetch += path
      }
      () // for some reasons scalac 2.12 needs this; return type doesn't matter
    }
    val filter = FileInputFormat.getInputPathFilter(new JobConf(hadoopConf, this.getClass))
    val discovered: Seq[(Path, Seq[FileStatus])] = PolyCatFileIndex.bulkListLeafFiles(
      pathsToFetch.toSeq, hadoopConf, filter, sparkSession)
    discovered.foreach { case (path, leafFiles) =>
      HiveCatalogMetrics.incrementFilesDiscovered(leafFiles.size)
      fileStatusCache.putLeafFiles(path, leafFiles.toArray)

      output ++= leafFiles
    }
    logInfo(s"It took ${(System.nanoTime() - startTime) / (1000 * 1000)} ms to list leaf files" +
      s" for ${paths.length} paths.")
    output
  }

  def listLeafPathFiles(paths: Seq[Path]): Seq[(Path, Seq[FileStatus])] = {
    val startTime = System.nanoTime()
    val output = mutable.LinkedHashSet[FileStatus]()
    val pathsToFetch = mutable.ArrayBuffer[Path]()
    for (path <- paths) {
      fileStatusCache.getLeafFiles(path) match {
        case Some(files) =>
          HiveCatalogMetrics.incrementFileCacheHits(files.length)
          output ++= files
        case None =>
          pathsToFetch += path
      }
      () // for some reasons scalac 2.12 needs this; return type doesn't matter
    }
    val filter = FileInputFormat.getInputPathFilter(new JobConf(hadoopConf, this.getClass))
    val discovered: Seq[(Path, Seq[FileStatus])] = PolyCatFileIndex.bulkListLeafFiles(
      pathsToFetch.toSeq, hadoopConf, filter, sparkSession)
    discovered.foreach { case (path, leafFiles) =>
      HiveCatalogMetrics.incrementFilesDiscovered(leafFiles.size)
      fileStatusCache.putLeafFiles(path, leafFiles.toArray)

      output ++= leafFiles
    }
    logInfo(s"listLeafPathFiles1: ${cachedLeafFiles.size}")
    logInfo(s"It took ${(System.nanoTime() - startTime) / (1000 * 1000)} ms to list leaf files" +
      s" for ${paths.length} paths.")
    discovered
  }
}

object PolyCatFileIndex extends Logging {

  private[sql] def bulkListLeafFiles(
                                      paths: Seq[Path],
                                      hadoopConf: Configuration,
                                      filter: PathFilter,
                                      sparkSession: SparkSession): Seq[(Path, Seq[FileStatus])] = {
    HadoopFSUtils.parallelListLeafFiles(
      sc = sparkSession.sparkContext,
      paths = paths,
      hadoopConf = hadoopConf,
      filter = new PathFilterWrapper(filter),
      ignoreMissingFiles = sparkSession.sessionState.conf.ignoreMissingFiles,
      ignoreLocality = sparkSession.sessionState.conf.ignoreDataLocality,
      parallelismThreshold = sparkSession.sessionState.conf.parallelPartitionDiscoveryThreshold,
      parallelismMax = sparkSession.sessionState.conf.parallelPartitionDiscoveryParallelism)
  }

}

private class PathFilterWrapper(val filter: PathFilter) extends PathFilter with Serializable {
  override def accept(path: Path): Boolean = {
    (filter == null || filter.accept(path)) && !HadoopFSUtils.shouldFilterOutPathName(path.getName)
  }
}
