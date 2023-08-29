/*
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
package io.polycat.catalog.spark;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest;
import io.polycat.catalog.common.plugin.request.ListDatabasesRequest;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.utils.PathUtil;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;

public class PolyCatClientHelper {

    public static PolyCatClient getClientFromSession(SparkSession sparkSession) {
        CatalogPlugin catalogPlugin = sparkSession.sessionState().catalogManager().currentCatalog();
        PolyCatClient client = null;
        if (catalogPlugin != null) {
            if (catalogPlugin instanceof SparkCatalog) {
                client = ((SparkCatalog) catalogPlugin).getClient();
            }
        }
        if (client == null) {
            client = PolyCatClient.getInstance(sparkSession.sessionState().newHadoopConf());
        }
        client.setDefaultCatalog(defaultCatalog(sparkSession));
        client.setDefaultDatabase(defaultDatabase(sparkSession));
        return client;
    }

    public static String defaultCatalog(SparkSession sparkSession) {
        return sparkSession.sessionState().catalogManager().currentCatalog().name();
    }

    public static String defaultCatalog(SparkSession sparkSession, String defaultCatalog) {
        if (defaultCatalog != null) {
            return defaultCatalog;
        }
        return sparkSession.sessionState().catalogManager().currentCatalog().name();
    }

    public static String defaultDatabase(SparkSession sparkSession) {
        String[] currentNamespace = sparkSession.sessionState().catalogManager().currentNamespace();
        if (currentNamespace.length == 0) {
            return null;
        }
        return currentNamespace[0];
    }

    public static String defaultDatabase(SparkSession sparkSession, String defaultDatabase) {
        if (defaultDatabase != null) {
            return defaultDatabase;
        }
        return defaultDatabase(sparkSession);
    }

    public static void createDataBase(String catalogName, String databaseName, PolyCatClient client) {
        CreateDatabaseRequest request = new CreateDatabaseRequest();
        DatabaseInput input = new DatabaseInput();
        request.setInput(input);
        request.setProjectId(client.getProjectId());
        request.setCatalogName(catalogName);
        // init input
        input.setCatalogName(catalogName);
        input.setDatabaseName(databaseName);
        String uuid = UUID.randomUUID().toString();
        String databaseLocation = PathUtil.getDatabaseLocation(client.getProjectId(),
            input.getDatabaseName());
        input.setLocationUri(databaseLocation);
        input.setOwner(client.getUserName());
        client.createDatabase(request);
    }

    public static String[][] listDatabases(PolyCatClient client, String catalogName) {
        ListDatabasesRequest request = new ListDatabasesRequest();
        request.setProjectId(client.getProjectId());
        request.setCatalogName(catalogName);
        String pageToken = null;
        List<String[]> namespaceList = new LinkedList<>();
        do {
            request.setPageToken(pageToken);
            PagedList<Database> pagedList = client.listDatabases(request);
            pageToken = pagedList.getNextMarker();
            for (Database database : pagedList.getObjects()) {
                namespaceList.add(new String[]{database.getDatabaseName()});
            }
        } while (pageToken != null);
        return namespaceList.toArray(new String[0][]);
    }

    public static Map<String, String> getDatabaseProperties(String catalogName, String databaseName,
        PolyCatClient client)
        throws NoSuchNamespaceException {
        GetDatabaseRequest request = new GetDatabaseRequest();
        request.setProjectId(client.getProjectId());
        request.setCatalogName(catalogName);
        request.setDatabaseName(databaseName);
        try {
            Database database = client.getDatabase(request);
            return database.getParameters();
        } catch (CatalogException catalogException) {
            if (catalogException.getStatusCode() == 404) {
                throw new NoSuchNamespaceException(new String[]{databaseName});
            } else {
                throw catalogException;
            }
        }
    }

    public static boolean isSparkDefaultCatalog(String catalogName) {
        return catalogName.equalsIgnoreCase("spark_catalog");
    }
}
