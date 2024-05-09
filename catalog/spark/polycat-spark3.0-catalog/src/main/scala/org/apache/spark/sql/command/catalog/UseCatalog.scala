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
package org.apache.spark.sql.command.catalog

import io.polycat.catalog.common.plugin.request.GetCatalogRequest
import io.polycat.catalog.common.plugin.request.base.ProjectRequestBase
import io.polycat.common.sql.AuthSubject
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

case class UseCatalog(request: GetCatalogRequest) extends RunnableCommand with AuthSubject {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val client = io.polycat.catalog.spark.PolyCatClientHelper.getClientFromSession(sparkSession)
    request.initContext(client.getContext)
    val catalogName = client.getCatalog(request).getCatalogName
    val icebergEnabled = sparkSession.conf.get("spark.polycat.iceberg.enabled", "true").toBoolean
    if (icebergEnabled) {
      sparkSession.conf.set("spark.sql.catalog.spark_catalog.catalog-impl", "io.polycat.catalog.iceberg.PolyCatIcebergCatalog")
      sparkSession.conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      if (!catalogName.equals("spark_catalog")) {
        sparkSession.conf.set("spark.sql.catalog." + catalogName + ".catalog-impl", "io.polycat.catalog.iceberg.PolyCatIcebergCatalog")
        sparkSession.conf.set("spark.sql.catalog." + catalogName, "org.apache.iceberg.spark.SparkCatalog")
      }
    } else {
      sparkSession.conf.set("spark.sql.catalog." + catalogName, "SparkCatalog")
    }
    sparkSession.sessionState.catalogManager.setCurrentCatalog(catalogName)
    client.getContext.setCurrentCatalogName(catalogName)
    client.getContext.setCurrentDatabaseName("default")
    Seq.empty
  }

  override def getRequest: ProjectRequestBase[_] = request
}
