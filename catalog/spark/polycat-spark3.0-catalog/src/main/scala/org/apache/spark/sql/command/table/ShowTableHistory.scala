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
package org.apache.spark.sql.command

import io.polycat.catalog.common.plugin.request.ListTableCommitsRequest
import io.polycat.catalog.common.plugin.request.base.ProjectRequestBase
import io.polycat.catalog.spark.PolyCatClientHelper
import io.polycat.common.sql.AuthSubject
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession}

case class ShowTableHistory(request: ListTableCommitsRequest) extends RunnableCommand with AuthSubject {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val client = io.polycat.catalog.spark.PolyCatClientHelper.getClientFromSession(sparkSession)
    request.initContext(client.getContext)
    client.listTableCommits(request)
      .getObjects
      .map { x =>
        Row(request.getCatalogName,
          request.getDatabaseName,
          request.getTableName,
          x.getCommitVersion,
          x.getCommitTime,
          x.getTableOperation.toString)
      }
  }

  override def output: Seq[Attribute] =
    Seq(AttributeReference("catalog name", StringType, nullable = false)(),
      AttributeReference("database name", StringType, nullable = false)(),
      AttributeReference("table name", StringType, nullable = false)(),
      AttributeReference("version", StringType, nullable = false)(),
      AttributeReference("commit time", StringType, nullable = false)(),
      AttributeReference("operation", StringType, nullable = false)())

  override def getRequest: ProjectRequestBase[_] = request
}
