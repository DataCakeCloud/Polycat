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

import io.polycat.probe.EventProcessor
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class PolyCatSparkListener extends SparkListener with QueryExecutionListener with Logging {
  val conf: Configuration = SparkSession.getActiveSession.map(ss => ss.sparkContext.hadoopConfiguration).getOrElse(new Configuration())
  val qeHashCode = new java.util.HashSet[Int]()
  val processor = new EventProcessor(conf)

  def pushTableUsageProfile(qe: QueryExecution): Unit = {
    val command = SparkTransferSqlParser.execSQL
    if (qeHashCode.contains(qe.analyzed.hashCode())) {
      log.info("The current statement has been processed and will be skipped directly. Skipped statement:{}", command)
    } else {
      try {
        val tableUsageProfiles = new SparkUsageProfileExtracter(conf, command).extractTableUsageProfile(qe)
        processor.pushTableUsageProfile(tableUsageProfiles)
        qeHashCode.add(qe.analyzed.hashCode())
      } catch {
        case e: Exception => log.warn("push tableUsageProfiles error!", e)
      }
    }
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    pushTableUsageProfile(qe)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    pushTableUsageProfile(qe)
  }

}
