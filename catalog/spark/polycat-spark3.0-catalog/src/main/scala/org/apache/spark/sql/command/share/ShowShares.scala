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
package org.apache.spark.sql.command.share

import io.polycat.catalog.common.model.Share
import io.polycat.catalog.common.model.record.Record
import io.polycat.catalog.common.plugin.request.ShowSharesRequest
import io.polycat.catalog.common.plugin.request.base.ProjectRequestBase
import io.polycat.common.{Records, RowBatch}
import io.polycat.common.sql.AuthSubject
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.ArrayType

import java.util
import java.util.{ArrayList, List}

case class ShowShares(request: ShowSharesRequest) extends RunnableCommand with AuthSubject{

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val client = io.polycat.catalog.spark.PolyCatClientHelper.getClientFromSession(sparkSession)
    request.initContext(client.getContext)
    val shares = client.showShares(request).getObjects;
    shares.flatMap { share =>
      val user = if (share.getToUsers == null || share.getToUsers.length == 0) {
        ""
      } else {
        share.getToUsers.mkString(",")
      }
      if (share.getShareGrantObjects == null || share.getShareGrantObjects.isEmpty) {
        Seq(Row(share.getCreatedTime, share.getKind, share.getShareName, "",
          share.getToAccounts.mkString(","),
          share.getOwnerAccount+ ":" + share.getOwnerUser,
          share.getComment,
          user))
      } else {
        var catalogName: String = null
        if (share.getKind == "OUTBOUND") catalogName = share.getCatalogName
        else catalogName = share.getProjectId + "." + share.getShareName
        share.getShareGrantObjects.map { shareGrantObject =>
          val objectNames: util.List[String] = new util.ArrayList[String]
          for (objectName <- shareGrantObject.getObjectName) {
            objectNames.add(catalogName + "." + shareGrantObject.getDatabaseName + "." + objectName)
          }
          Row(share.getCreatedTime, share.getKind, share.getShareName,
            objectNames.toArray(new Array[String](objectNames.size)).mkString(","),
            share.getToAccounts.mkString(","),
            share.getOwnerAccount + ":" + share.getOwnerUser,
            share.getComment,
            user)
        }
      }
    }
  }

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("created time", StringType, nullable = false)(),
      AttributeReference("kind", StringType, nullable = false)(),
      AttributeReference("share name", StringType, nullable = false)(),
      AttributeReference("object name", StringType, nullable = true)(),
      AttributeReference("to accounts", StringType, nullable = true)(),
      AttributeReference("owner", StringType, nullable = true)(),
      AttributeReference("comment", StringType, nullable = true)(),
      AttributeReference("assigned users", StringType, nullable = true)()
    )
  }

  override def getRequest: ProjectRequestBase[_] = request
}
