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
package io.polycat.probe.spark

import io.polycat.catalog.common.lineage.{EDbType, EJobStatus, LineageConstants, LineageFact}
import io.polycat.catalog.common.plugin.request.input.LineageInfoInput
import io.polycat.probe.{EventProcessor, ProbeConstants}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import java.io.{PrintWriter, StringWriter}
import java.util
import scala.collection.JavaConverters._


class SparkLineageListener extends SparkListener with QueryExecutionListener with Logging {
  val conf: Configuration = SparkSession.getActiveSession.map(ss => ss.sparkContext.hadoopConfiguration).getOrElse(new Configuration())
  val processor = new EventProcessor(conf)

  def pushSqlLineage(qe: QueryExecution, state: EJobStatus, exception: Option[Exception]) = {
    val command = SparkTransferSqlParser.execSQL
    try {
      val lineageInfo = new LineageInfoInput()
      val lineageFact = new LineageFact
      lineageFact.setJobStatus(state)
      lineageFact.setSql(command)
      lineageFact.setStartTime(System.currentTimeMillis)
      exception.foreach(exc => lineageFact.setErrorMsg(stringifyException(exc)))
      lineageFact.setJobType(EDbType.SPARKSQL.name)
      lineageFact.setCluster(conf.get(ProbeConstants.CATALOG_PROBE_CLUSTER_NAME))
      lineageFact.setExecuteUser(conf.get(ProbeConstants.CATALOG_PROBE_USER_ID))
      lineageFact.setJobId(conf.get(ProbeConstants.CATALOG_PROBE_TASK_ID))
      lineageFact.setJobName(conf.get(ProbeConstants.CATALOG_PROBE_TASK_NAME))

      val params = new util.HashMap[String, AnyRef]
      conf.iterator().asScala
        .filter(entry => entry.getKey.startsWith(LineageConstants.LINEAGE_JOB_FACT_PREFIX))
        .foreach(entry => params.put(entry.getKey, entry.getValue))
      params.put(LineageConstants.LINEAGE_JOB_FACT_PREFIX + ProbeConstants.CATALOG_PROBE_SOURCE, conf.get(ProbeConstants.CATALOG_PROBE_SOURCE))
      lineageFact.setParams(params)
      lineageFact.setEndTime(System.currentTimeMillis)
      lineageInfo.setJobFact(lineageFact)
      // parse plan get lineage info
      new SparkLineageParser(conf.get(ProbeConstants.POLYCAT_CATALOG)).parsePlan(qe.logical, lineageInfo)
      if (null != lineageInfo.getNodeMap && !lineageInfo.getNodeMap.isEmpty) {
        processor.pushSqlLineage(lineageInfo)
      } else {
        log.info("lineage info is empty, will skip pushSqlLineage!")
      }
    } catch {
      case e: Exception => log.warn("exec pushSqlLineage error!", e)
    }
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    pushSqlLineage(qe, EJobStatus.SUCCESS, None)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    pushSqlLineage(qe, EJobStatus.FAILED, Some(exception))
  }

  def stringifyException(e: Throwable): String = {
    val stm = new StringWriter
    val wrt = new PrintWriter(stm)
    e.printStackTrace(wrt)
    wrt.close()
    stm.toString
  }
}