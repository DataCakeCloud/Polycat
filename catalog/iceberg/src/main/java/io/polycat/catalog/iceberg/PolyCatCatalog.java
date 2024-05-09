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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.polycat.catalog.client.Client;
import io.polycat.catalog.client.PolyCatClientBuilder;
import io.polycat.catalog.client.PolyCatClientHelper;
import io.polycat.catalog.common.exception.AlreadyExistsException;
import io.polycat.catalog.common.exception.InvalidOperationException;
import io.polycat.catalog.common.exception.NoSuchObjectException;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.options.PolyCatOptions;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PolyCatCatalog extends BaseMetastoreCatalog
        implements SupportsNamespaces, Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(PolyCatCatalog.class);

    private String defaultWarehouse;

    private String name;

    private Client polycatClient;

    private Configuration conf;

    private FileIO fileIO;

    private Map<String, String> catalogProperties;

    @Override
    public void initialize(String name, Map<String, String> properties) {
        this.name = name;
        String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
        this.fileIO = fileIOImpl == null ? new HadoopFileIO(conf) : CatalogUtil.loadFileIO(fileIOImpl, properties, conf);
        this.catalogProperties = ImmutableMap.copyOf(properties);
        defaultWarehouse = PolyCatOptions.getValueViaPrefixKey(catalogProperties, PolyCatOptions.DEFAULT_WAREHOUSE);
        if (conf == null) {
            LOG.warn("No Hadoop Configuration was set, using the default environment Configuration");
            this.conf = new Configuration();
        }
        this.polycatClient = PolyCatClientBuilder.buildWithHadoopConf(conf);
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
        String dbName = tableIdentifier.namespace().level(0);
        String tableName = tableIdentifier.name();
        return new PolyCatTableOperations(name, dbName, tableName, conf, fileIO, polycatClient);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        final Database polycatDatabase = PolyCatClientHelper.getDatabase(polycatClient, name, tableIdentifier.namespace().levels()[0]);
        if (polycatDatabase.getLocationUri() != null) {
            return new Path(polycatDatabase.getLocationUri(), tableIdentifier.name()).toString();
        } else {
            final Catalog catalog = PolyCatClientHelper.getCatalog(polycatClient, name);
            if (catalog.getLocation() != null) {
                return new Path(new Path(catalog.getLocation(), polycatDatabase.getDatabaseName()), tableIdentifier.name()).toString();
            } else {
                return defaultWarehouse;
            }
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        throw new UnsupportedOperationException("listNamespaces is not supported");
    }

    private boolean isValidateNamespace(Namespace namespace) {
        return namespace.levels().length == 1;
    }

    @Override
    protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
        return tableIdentifier.namespace().levels().length == 1;
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        if (!isValidIdentifier(identifier)) {
            return false;
        }
        if (!isValidIdentifier(identifier)) {
            return false;
        }

        String database = identifier.namespace().level(0);

        TableOperations ops = newTableOps(identifier);
        TableMetadata lastMetadata = null;
        if (purge) {
            try {
                lastMetadata = ops.current();
            } catch (NotFoundException e) {
                LOG.warn(
                        "Failed to load table metadata for table: {}, continuing drop without purge",
                        identifier,
                        e);
            }
        }
        try {
            PolyCatClientHelper.dropTable(polycatClient, name, database, identifier.name());
            if (purge && lastMetadata != null) {
                CatalogUtil.dropTableData(ops.io(), lastMetadata);
            }

            LOG.info("Dropped table: {}", identifier);
            return true;
        } catch (NoSuchTableException | NoSuchObjectException e) {
            LOG.info("Skipping drop, table does not exist: {}", identifier, e);
            return false;

        }
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier originalTo) {
        if (!isValidIdentifier(from)) {
            throw new NoSuchTableException("Invalid identifier: %s", from);
        }
        TableIdentifier to = removeCatalogName(originalTo);
        Preconditions.checkArgument(isValidIdentifier(to), "Invalid identifier: %s", to);
        String toDatabase = to.namespace().level(0);
        String fromDatabase = from.namespace().level(0);
        String fromName = from.name();
        try {
            final Table polyCatTable = PolyCatClientHelper.getTable(polycatClient, name, fromDatabase, fromName);
            PolyCatTableOperations.validateTableIsIceberg(polyCatTable, fullTableName(name, from));
            PolyCatClientHelper.renameTable(polycatClient, polyCatTable, toDatabase, to.name());
        } catch (NoSuchObjectException e) {
            throw new NoSuchTableException("Table does not exist: %s", from);
        } catch (AlreadyExistsException e) {
            throw new org.apache.iceberg.exceptions.AlreadyExistsException(
                    "Table already exists: %s", to);
        }
    }

    private TableIdentifier removeCatalogName(TableIdentifier to) {
        if (isValidIdentifier(to)) {
            return to;
        }

        // check if the identifier includes the catalog name and remove it
        if (to.namespace().levels().length == 2 && name().equalsIgnoreCase(to.namespace().level(0))) {
            return TableIdentifier.of(Namespace.of(to.namespace().level(1)), to.name());
        }

        // return the original unmodified
        return to;
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> meta) {
        Preconditions.checkArgument(
                !namespace.isEmpty(), "Cannot create namespace with invalid name: %s", namespace);
        Preconditions.checkArgument(
                isValidateNamespace(namespace),
                "Cannot support multi part namespace in Hive Metastore: %s",
                namespace);
        if (!isValidateNamespace(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }
        final String location = meta.getOrDefault("location", defaultWarehouse);
        try {
            PolyCatClientHelper.createDatabase(polycatClient, name, namespace.level(0), meta.get("comment"), location, polycatClient.getUserName(), meta);
            LOG.info("Created namespace: {}", namespace);
        } catch (AlreadyExistsException e) {
            throw new org.apache.iceberg.exceptions.AlreadyExistsException(
                    e, "Namespace '%s' already exists!", namespace);
        }

    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
        throw new UnsupportedOperationException("listNamespaces is not supported");
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
        if (!isValidateNamespace(namespace)) {
            throw new NoSuchNamespaceException("Namespace invalid: %s", namespace);
        }
        try {
            final Database database = PolyCatClientHelper.getDatabase(polycatClient, name, namespace.level(0));
            return loadNamespaceMetadata(database);
        } catch (NoSuchObjectException e) {
            throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);
        }
    }

    private Map<String, String> loadNamespaceMetadata(Database database) {
        final Map<String, String> metadata = database.getParameters();
        metadata.put("location", database.getLocationUri());
        if (database.getDescription() != null) {
            metadata.put("comment", database.getDescription());
        }
        LOG.debug("Loaded metadata for namespace {} found {}", database.getDatabaseName(), metadata.keySet());
        return metadata;
    }

    @Override
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        if (!isValidateNamespace(namespace)) {
            return false;
        }
        try {
            PolyCatClientHelper.dropDatabase(polycatClient, name, namespace.level(0));
            LOG.info("Dropped namespace: {}", namespace);
            return true;
        } catch (InvalidOperationException e) {
            throw new NamespaceNotEmptyException(
                    e, "Namespace %s is not empty. One or more tables exist.", namespace);
        } catch (NoSuchObjectException e) {
            return false;
        }
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
        Map<String, String> parameter = Maps.newHashMap();
        try {
            final Database database = PolyCatClientHelper.getDatabase(polycatClient, name, namespace.level(0));
            parameter.putAll(loadNamespaceMetadata(database));
            parameter.putAll(properties);
            String comment = properties.get("comment");
            String location = properties.getOrDefault("location", defaultWarehouse);
            String owner = properties.getOrDefault("owner", database.getOwner());
            PolyCatClientHelper.alterDatabase(polycatClient, database, comment, location, owner, parameter);
        } catch (NoSuchObjectException e) {
            throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);
        }

        LOG.debug("Successfully set properties {} for {}", properties.keySet(), namespace);
        return true;
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
        Map<String, String> parameter = Maps.newHashMap();
        try {
            final Database database = PolyCatClientHelper.getDatabase(polycatClient, name, namespace.level(0));
            parameter.putAll(loadNamespaceMetadata(database));
            properties.forEach(key -> parameter.put(key, null));
            String comment = parameter.get("comment");
            String location = parameter.getOrDefault("location", defaultWarehouse);
            String owner = parameter.getOrDefault("owner", database.getOwner());
            PolyCatClientHelper.alterDatabase(polycatClient, database, comment, location, owner, parameter);
        } catch (NoSuchObjectException e) {
            throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);
        }

        LOG.debug("Successfully removed properties {} from {}", properties, namespace);
        return true;
    }

    @Override
    protected Map<String, String> properties() {
        return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = new Configuration(conf);
    }

    /**
     * Return the configuration used by this object.
     */
    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("uri", polycatClient.getEndpoint())
                .toString();
    }
}
