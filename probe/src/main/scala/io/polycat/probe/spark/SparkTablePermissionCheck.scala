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

import io.polycat.catalog.common.Operation
import io.polycat.catalog.common.exception.CatalogException
import io.polycat.catalog.common.model.AuthorizationType
import io.polycat.catalog.common.plugin.CatalogContext
import io.polycat.catalog.common.plugin.request.AuthenticationRequest
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput
import io.polycat.probe.PolyCatClientUtil
import io.polycat.probe.model.CatalogOperationObject
import io.polycat.probe.spark.parser.SparkPlanParserHelper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import java.util
import scala.collection.mutable.ArrayBuffer

class SparkTablePermissionCheck(sparkSession: SparkSession) extends (LogicalPlan => Unit) with Logging {
  val hadoopConf: Configuration = sparkSession.sparkContext.hadoopConfiguration
  val polyCatCatalog = hadoopConf.get("polycat.catalog.name")

  override def apply(plan: LogicalPlan): Unit = {
    if (sparkSession.conf.get("spark.sql.authorization.enabled", "true").toBoolean) {
      val ss: SessionState = SessionState.get
      Option(ss).foreach {
        ss => {
          val paraString = HiveConf.getVar(ss.getConf, HiveConf.ConfVars.NEWTABLEDEFAULTPARA)
          if (paraString != null && paraString.nonEmpty) {
            for (keyValuePair <- paraString.split(",")) {
              val keyValue = keyValuePair.split("=", 2)
              if (keyValue.length == 2) {
                log.info(s"==hiveConf==:${keyValue(0)},${keyValue(1)}")
                hadoopConf.set(keyValue(0), keyValue(1))
              }
            }
          }
        }
      }
      val client = PolyCatClientUtil.buildPolyCatClientFromConfig(hadoopConf)
      val authorizationNotAllowedList = new ArrayBuffer[AuthorizationInput]()
      val parserHelper = new SparkPlanParserHelper(client.getProjectId, polyCatCatalog)
      val catalogOperationObjects = parserHelper.parsePlan(plan)

      if (!catalogOperationObjects.isEmpty) {
        catalogOperationObjects.forEach(catalogOperationObject => {
          val authorization = createAuthorizationInput(client.getContext, catalogOperationObject)
          logInfo(s"table authorization info:$authorization")
          try {
            val response = client.authenticate(new AuthenticationRequest(client.getProjectId, util.Arrays.asList(authorization)))
            if (!response.getAllowed) {
              authorizationNotAllowedList.append((authorization))
            }
          } catch {
            case e: CatalogException if e.getStatusCode == 404 => logDebug("table authenticate not found error! ", e)
            case u: Exception => throw new CatalogException("table authenticate error!", u)
          }
        }
        )
        // deal with result
        if (authorizationNotAllowedList.nonEmpty) {
          val builder = new StringBuilder
          val disableOperations = sparkSession.sparkContext.getConf
            .getAllWithPrefix("spark.sql.authorization.disabled.")
            .filter(_._2.toBoolean)
            .map(_._1.toUpperCase())
          authorizationNotAllowedList
            .filter(input => !disableOperations.contains(input.getOperation.toString))
            .foreach(entity => {
              val databaseName = if (entity.getCatalogInnerObject.getDatabaseName == null || entity.getCatalogInnerObject.getDatabaseName.isEmpty) "" else entity.getCatalogInnerObject.getDatabaseName + "."
              builder.append(s"[object: $databaseName${entity.getCatalogInnerObject.getObjectName}] no permission [${entity.getOperation.getPrintName}] \n")
            })
          throw new CatalogException("permission error:\n" + builder.toString)
        }
      }
    }
  }

  def createAuthorizationInput(context: CatalogContext, catalogObject: CatalogOperationObject): AuthorizationInput = {
    val authorizationInput = new AuthorizationInput
    authorizationInput.setAuthorizationType(AuthorizationType.NORMAL_OPERATION)
    authorizationInput.setOperation(Operation.convertToParentOperation(catalogObject.getOperation))
    authorizationInput.setCatalogInnerObject(catalogObject.getCatalogObject)
    authorizationInput.setGrantObject(null)
    authorizationInput.setUser(context.getUser)
    authorizationInput.setToken(context.getToken)
    authorizationInput
  }
}

