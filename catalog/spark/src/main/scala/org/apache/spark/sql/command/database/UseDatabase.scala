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
package org.apache.spark.sql.command.database

import io.polycat.catalog.common.Operation
import io.polycat.catalog.common.model.CatalogInnerObject
import io.polycat.catalog.common.plugin.CatalogContext
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest
import io.polycat.catalog.common.plugin.request.base.ProjectRequestBase
import io.polycat.catalog.spark.PolyCatClientHelper
import io.polycat.common.sql.AuthSubject
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

import java.util.Objects

case class UseDatabase(request: GetDatabaseRequest)  extends RunnableCommand with AuthSubject{
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val client = PolyCatClientHelper.getClientFromSession(sparkSession)
    request.initContext(client.getContext)
    val database = client.getDatabase(request)
    if (Objects.nonNull(database) && Objects.nonNull(database.getDatabaseName)) {
      client.getContext.setCurrentCatalogName(request.getCatalogName)
      client.getContext.setCurrentDatabaseName(request.getDatabaseName)
      sparkSession.sessionState.catalogManager.setCurrentCatalog(request.getCatalogName)
      sparkSession.sessionState.catalogManager.setCurrentNamespace(Array(request.getDatabaseName))
    }
    Seq.empty
  }

  override def initializePlan(context: CatalogContext): Unit = {
    request.initContext(context)
  }

  override def getOperation = Operation.USE_DATABASE

  override def getCatalogObject(context: CatalogContext): CatalogInnerObject = request.getCatalogObject(context)

}
