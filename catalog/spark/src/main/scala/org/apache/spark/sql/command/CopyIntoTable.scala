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

import com.huaweicloud.sdk.core.auth.GlobalCredentials
import com.huaweicloud.sdk.core.exception.{ConnectionException, RequestTimeoutException, ServiceResponseException}
import com.huaweicloud.sdk.iam.v3.IamClient
import com.huaweicloud.sdk.iam.v3.model._
import com.huaweicloud.sdk.iam.v3.region.IamRegion
import io.polycat.catalog.client.PolyCatClient
import io.polycat.catalog.common.{GlobalConfig, Operation}
import io.polycat.catalog.common.exception.CarbonSqlException
import io.polycat.catalog.common.model.{CatalogInnerObject, Column, Table}
import io.polycat.catalog.common.plugin.CatalogContext
import io.polycat.catalog.common.plugin.request.{GetDelegateRequest, GetTableRequest}
import io.polycat.catalog.common.plugin.request.input.TableNameInput
import io.polycat.catalog.spark.PolyCatClientHelper
import io.polycat.common.sql.AuthSubject
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.connector.catalog.SparkHelper
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.text.SimpleDateFormat
import java.util
import scala.collection.JavaConversions._

case class CopyIntoTable(tableNameInput: TableNameInput, delegateName: String,
                         location: String, fileFormat: String, withHeader: Boolean, delimiter: String) extends RunnableCommand with AuthSubject {
private val obsEndpoint = "obs.myhuaweicloud.com";

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val polyCatClient = PolyCatClientHelper.getClientFromSession(sparkSession)
    val tempAccess = doExchangeTempAccessFromIAM(delegateName, polyCatClient)
    val del = if (delimiter == null || delimiter.isEmpty) {
      ","
    } else {
      delimiter
    }

    val table = getTable(polyCatClient)
    val schema = convertSchema(table.getFields(), table.getPartitionKeys())

    val df = sparkSession.read.format("csv")
      .option("encoding", "utf-8")
      .option("delimiter", del)
      .option("header", withHeader)
      .option("inferSchema", true)
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .options(tempAccess)
      .schema(schema)
      .load(location)

    df.write.format(table.getStorageDescriptor.getSourceShortName).mode("append")
      .saveAsTable(table.getDatabaseName + "." + table.getTableName)
    Seq.empty
  }

  def getTable(polyCatClient: PolyCatClient): Table = {
    initializePlan(polyCatClient.getContext)
    val request = new GetTableRequest(polyCatClient.getProjectId, tableNameInput.getCatalogName,
      tableNameInput.getDatabaseName, tableNameInput.getTableName);
    polyCatClient.getTable(request)
  }

  def convertSchema(columns: util.List[Column], partitionColumns: util.List[Column]) = {
    val columnNum = columns.size
    var partitionColumnNum = 0
    if (partitionColumns != null && partitionColumns != null) partitionColumnNum = partitionColumns.size
    val structFields = new Array[StructField](columnNum + partitionColumnNum)
    var i = 0
    for (schemaField <- columns) {
      structFields({
        i += 1; i - 1
      }) = SparkHelper.convertField(schemaField)
    }
    if (partitionColumnNum > 0) {
      for (schemaField <- partitionColumns) {
        structFields({
          i += 1; i - 1
        }) = SparkHelper.convertField(schemaField)
      }
    }
    new StructType(structFields)
  }

  def doExchangeTempAccessFromIAM(delegate:String, polyCatClient: PolyCatClient): Map[String, String] = {
    if (delegate == null || delegate.isEmpty) {
      return Map()
    }
    val getDelegateRequest = new GetDelegateRequest(delegate)
    getDelegateRequest.setProjectId(polyCatClient.getProjectId)
    val delegateOutput = polyCatClient.getDelegate(getDelegateRequest)
    val agencyName = delegateOutput.getAgencyName
    val domainName = delegateOutput.getProviderDomainName

    val auth = new GlobalCredentials()
      .withAk(GlobalConfig.getString(GlobalConfig.OBS_AK))
      .withSk(GlobalConfig.getString(GlobalConfig.OBS_SK))

    val iamClient = IamClient.newBuilder.withCredential(auth)
      .withRegion(IamRegion.CN_NORTH_1).build

    val request = new CreateTemporaryAccessKeyByAgencyRequest
    val body = new CreateTemporaryAccessKeyByAgencyRequestBody
    val assumeRoleIdentity = new IdentityAssumerole
    assumeRoleIdentity.withAgencyName(agencyName).withDomainName(domainName)
    val listIdentityMethods = new util.ArrayList[AgencyAuthIdentity.MethodsEnum]
    listIdentityMethods.add(AgencyAuthIdentity.MethodsEnum.fromValue("assume_role")) // iam fixed usage

    val identityAuth = new AgencyAuthIdentity
    identityAuth.withMethods(listIdentityMethods).withAssumeRole(assumeRoleIdentity)
    val authBody = new AgencyAuth
    authBody.withIdentity(identityAuth)
    body.withAuth(authBody)
    request.withBody(body)

    try {
      val response = iamClient.createTemporaryAccessKeyByAgency(request)
      val temporaryAK = response.getCredential.getAccess
      val temporarySK = response.getCredential.getSecret
      val securityToken = response.getCredential.getSecuritytoken
      val formattedTime = response.getCredential.getExpiresAt.replace("000Z", "UTC")
      val obsExpireFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
      val expireTime = obsExpireFormat.parse(formattedTime).getTime

      Map(GlobalConfig.getName(GlobalConfig.OBS_AK) -> temporaryAK,
        GlobalConfig.getName(GlobalConfig.OBS_SK) -> temporarySK,
        GlobalConfig.getName(GlobalConfig.OBS_TOKEN) -> securityToken,
        GlobalConfig.getName(GlobalConfig.OBS_ENDPOINT) -> obsEndpoint
      )
    } catch {
      case e@(_: ConnectionException | _: RequestTimeoutException) =>
        throw new CarbonSqlException(e)
      case e: ServiceResponseException =>
        throw new CarbonSqlException(e)
    }
  }

  override def initializePlan(context: CatalogContext): Unit = {
    if (StringUtils.isBlank(tableNameInput.getCatalogName)) {
      tableNameInput.setCatalogName(context.getCurrentCatalogName)
    }
    if (StringUtils.isBlank(tableNameInput.getDatabaseName)) {
      tableNameInput.setDatabaseName(context.getCurrentDatabaseName)
    }
  }

  override def getOperation: Operation = Operation.COPY_INTO

  override def getCatalogObject(context: CatalogContext): CatalogInnerObject = new CatalogInnerObject(context.getProjectId,
    tableNameInput.getCatalogName, tableNameInput.getDatabaseName, tableNameInput.getTableName)
}
