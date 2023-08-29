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
package io.polycat.catalog.iceberg;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.polycat.catalog.client.PolyCatClient;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest;
import io.polycat.catalog.spark.PolyCatClientHelper;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;

public class PolyCatIcebergCatalog extends BaseMetastoreCatalog
    implements SupportsNamespaces, Configurable {

    private String name;

    private Configuration conf;

    private PolyCatClient client;

    private FileIO fileIO;

    public PolyCatIcebergCatalog() {

    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
        this.name = name;
        String fileIOImpl = properties.get("io-impl");
        this.fileIO = (fileIOImpl == null ? new HadoopFileIO(this.conf)
            : CatalogUtil.loadFileIO(fileIOImpl, properties, this.conf));
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
        return new PolyCatTableOperations(client, fileIO, name, tableIdentifier.namespace().level(0),
            tableIdentifier.name());
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        GetDatabaseRequest request = new GetDatabaseRequest();
        request.setProjectId(client.getProjectId());
        request.setCatalogName(this.name);
        request.setDatabaseName(tableIdentifier.namespace().level(0));
        Database database = client.getDatabase(request);
        String locationUri = database.getLocationUri();
        return locationUri + "/" + tableIdentifier.name();
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        return null;
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        return false;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {

    }


    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        initClientIfNeeded();
    }

    private void initClientIfNeeded() {
        if (client == null) {
            synchronized(this) {
                if (client == null) {
                    client = PolyCatClient.getInstance(conf);
                }
            }
        }
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> metadata) {
        assert namespace.levels().length == 1;
        PolyCatClientHelper.createDataBase(this.name, namespace.level(0), this.client);
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
        assert namespace.levels().length == 0;
        return Arrays.stream(PolyCatClientHelper.listDatabases(client, name))
            .map(x -> Namespace.of(x[0]))
            .collect(Collectors.toList());
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
        assert namespace.levels().length == 1;
        try {
            return PolyCatClientHelper.getDatabaseProperties(name, namespace.level(0), client);
        } catch (org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException e) {
            throw new NoSuchNamespaceException(namespace.level(0));
        }
    }

    @Override
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        return false;
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties)
        throws NoSuchNamespaceException {
        return false;
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties)
        throws NoSuchNamespaceException {
        return false;
    }
}
