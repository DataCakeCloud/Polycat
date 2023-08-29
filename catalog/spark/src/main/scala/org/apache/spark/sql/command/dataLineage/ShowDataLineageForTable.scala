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
package org.apache.spark.sql.command.dataLineage

import io.polycat.catalog.common.model.{DataSource, DataSourceType}
import io.polycat.catalog.common.plugin.request.ListDataLineageRequest
import io.polycat.catalog.common.plugin.request.base.ProjectRequestBase
import io.polycat.catalog.spark.PolyCatClientHelper
import io.polycat.common.sql.AuthSubject
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConversions._

case class ShowDataLineageForTable(request: ListDataLineageRequest) extends RunnableCommand with AuthSubject{


  override def run(sparkSession: SparkSession): Seq[Row] = {
    val client = PolyCatClientHelper.getClientFromSession(sparkSession)
    request.initContext(client.getContext)
    var strSource = ""
    client.listDataLineages(request).getObjects.flatMap { dataLineage =>
      dataLineage.getDataInput.map { dataSource =>
        strSource = getDataSource(dataSource)
        if (strSource ne "") {
          val tableTarget = dataLineage.getDataOutput.getTableSource
          Row(dataLineage.getDataOutput.getSourceType, strSource, dataLineage.getOperation,
            tableTarget.getCatalogName + "." + tableTarget.getDatabaseName + "." + tableTarget.getTableName)
        } else {
          Row.empty
        }
      }
    }
  }

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("SourceType", StringType, nullable = false)(),
      AttributeReference("Source", StringType, nullable = false)(),
      AttributeReference("Operation", StringType, nullable = false)(),
      AttributeReference("Target", StringType, nullable = false)()
    )
  }

  private def getDataSource(dataSource: DataSource) = {
    var strSource = ""
    dataSource.getSourceType match {
      case DataSourceType.SOURCE_TABLE =>
        val tableSource = dataSource.getTableSource
        strSource = tableSource.fullTableName

      case DataSourceType.SOURCE_STREAM =>
        strSource = dataSource.getStreamerSource.toString

      case DataSourceType.SOURCE_FILE =>
        strSource = dataSource.getFileSource.getPath

      case DataSourceType.SOURCE_APP =>
        strSource = dataSource.getAppSource.getAppName

      case _ =>

    }
    strSource
  }
  override def getRequest: ProjectRequestBase[_] = request
}
