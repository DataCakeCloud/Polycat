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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{CreateDataSourceTableCommand, CreateDatabaseCommand, CreateTableCommand}
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}


class SparkTablePermissionExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { case (_, parser) => new SparkTransferSqlParser(parser) }
    extensions.injectCheckRule((sparkSession: SparkSession) => new SparkTablePermissionCheck(sparkSession))
    extensions.injectOptimizerRule((sparkSession: SparkSession) => new SparkTableOwnerReset(sparkSession))
  }
}

class SparkTableOwnerReset(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private val userName: String = sparkSession.sparkContext.hadoopConfiguration.get("polycat.client.userName")
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case cdc: CreateDatabaseCommand =>
        cdc.copy(props = cdc.props ++ Map("owner" -> userName) )
      case ctc: CreateTableCommand =>
        ctc.copy(table = ctc.table.copy(owner = userName))
      case cdst: CreateDataSourceTableCommand =>
        cdst.copy(table = cdst.table.copy(owner = userName))
      case ct: CreateTable =>
        ct.copy(tableDesc = ct.tableDesc.copy(owner = userName))
      case _ =>
        plan
    }
  }
}

