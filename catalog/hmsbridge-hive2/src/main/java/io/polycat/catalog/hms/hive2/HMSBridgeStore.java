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
package io.polycat.catalog.hms.hive2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.exception.CatalogException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;
import org.eclipse.jetty.http.HttpStatus;

public class HMSBridgeStore implements RawStore {

    private static final Logger LOG = Logger.getLogger(HMSBridgeStore.class);

    private final static String LMS_NAME = "lms_name";
    public final static String READONLY_CONFIG = "hive.hmsbridge.readonly";
    public final static String DOUBLE_WRITE_CONFIG = "hive.hmsbridge.doubleWrite";
    public final static String DELEGATE_ONLY_CONFIG = "hive.hmsbridge.delegateOnly";
    private final static String DEFAULT_CATALOG_NAME = "hive.hmsbridge.defaultCatalogName";

    private RawStore delegatedStore;
    private CatalogStore lmsStore;

    private boolean readonly;
    private boolean doubleWrite;
    private boolean delegateOnly;
    private String defaultCatalogName;

    public HMSBridgeStore() throws IOException {
        this.delegatedStore = new ObjectStore();
        this.lmsStore = new CatalogStore();
    }

    public HMSBridgeStore(Configuration configuration) {
        this.delegatedStore = new ObjectStore();
        this.lmsStore = new CatalogStore(configuration);
    }

    @Override
    public void shutdown() {
        delegatedStore.shutdown();
    }

    @Override
    public boolean openTransaction() {
        return delegatedStore.openTransaction();
    }

    @Override
    public boolean commitTransaction() {
        return delegatedStore.commitTransaction();
    }

    @Override
    public boolean isActiveTransaction() {
        return delegatedStore.isActiveTransaction();
    }

    @Override
    public void rollbackTransaction() {
        delegatedStore.rollbackTransaction();
    }

    @Override
    public void createDatabase(Database db) throws InvalidObjectException, MetaException {
        if (readonly) {
            throw new MetaException("Cannot create database in readonly mode");
        }
        if (delegateOnly) {
            delegatedStore.createDatabase(db);
            return;
        }
        db.setParameters(checkAndGetLmsFlagParameters(db.getParameters()));
        lmsStore.createDatabase(db);
        if (doubleWrite) {
            try {
                delegatedStore.createDatabase(db);
            } catch (Exception e) {
                LOG.error(
                        String.format("Cannot rollback createDatabase(%s) , please rollback the operation manually",
                                db.getName()), e);
                throw e;
            }
        }
    }

    private Map<String, String> checkAndGetLmsFlagParameters(Map<String, String> parameters) {
        if (parameters == null ) {
            parameters = new HashMap<>();
        }
        if (!parameters.containsKey(LMS_NAME) && StringUtils.isNotEmpty(defaultCatalogName)) {
            parameters.put(LMS_NAME, defaultCatalogName);
        }
        return parameters;
    }

    @Override
    public Database getDatabase(String name) throws NoSuchObjectException {
        if (delegateOnly) {
            return delegatedStore.getDatabase(name);
        }
        MetaObjectName objectName = getObjectFromNameMap(name);
        if (objectName != null) {
            return lmsStore.getDatabase(objectName.getCatalogName(), objectName.getDatabaseName());
        } else {
            throw new NoSuchObjectException("There is no database named " + name);
        }
    }

    @Override
    public boolean dropDatabase(String dbname) throws NoSuchObjectException, MetaException {
        if (readonly) {
            throw new MetaException("Cannot drop database in readonly mode");
        }
        if (delegateOnly) {
            return delegatedStore.dropDatabase(dbname);
        }
        MetaObjectName objectName = getObjectFromNameMap(dbname);
        boolean success = false;
        if (objectName != null) {
            success = lmsStore.dropDatabase(objectName.getCatalogName(), dbname);
            if (doubleWrite) {
                try {
                    return delegatedStore.dropDatabase(dbname) && success;
                } catch (Exception e) {
                    LOG.error(String
                            .format("Cannot rollback dropDatabase(%s), please rollback the operation manually", dbname), e);
                    throw e;
                }
            }
            return success;
        } else {
            throw new NoSuchObjectException("There is no database named " + dbname);
        }
    }

    @Override
    public boolean alterDatabase(String dbname, Database db)
        throws NoSuchObjectException, MetaException {
        if (readonly) {
            throw new MetaException("Cannot alter database in readonly mode");
        }
        if (delegateOnly) {
            return delegatedStore.alterDatabase(dbname, db);
        }
        MetaObjectName objectName = getObjectFromNameMap(dbname);
        boolean success = false;
        if (objectName != null) {
            success = lmsStore.alterDatabase(objectName.getCatalogName(), dbname, db);
            if (doubleWrite) {
                try {
                    return delegatedStore.alterDatabase(dbname, db) && success;
                } catch (Exception exception) {
                    LOG.error(String
                            .format("Cannot rollback alterDatabase(%s), please rollback the operation manually", dbname),
                        exception);
                    throw exception;
                }
            }
            return success;
        }
        return delegatedStore.alterDatabase(dbname, db);
    }

    @Override
    public List<String> getDatabases(String pattern) throws MetaException {
        List<String> dbs = new ArrayList<>();
        if (delegateOnly && doubleWrite) {
            dbs = delegatedStore.getDatabases(pattern);
        }
        List<String> dbsFromLms = new ArrayList<>();
        if (!doubleWrite && !delegateOnly) {
            dbsFromLms = lmsStore.getDatabases(pattern, defaultCatalogName);
            return dbsFromLms;
        }
        dbs.addAll(dbsFromLms);
        return dbs;
    }

    @Override
    public List<String> getAllDatabases() throws MetaException {
        List<String> dbs = new ArrayList<>();
        if (delegateOnly && doubleWrite) {
            dbs = delegatedStore.getAllDatabases();
        }
        List<String> dbsFromLms = new ArrayList<>();
        if (!doubleWrite && !delegateOnly) {
            dbsFromLms = lmsStore.getAllDatabases(defaultCatalogName);
            return dbsFromLms;
        }
        dbs.removeAll(dbsFromLms);
        dbs.addAll(dbsFromLms);
        return dbs;
    }

    @Override
    public boolean createType(Type type) {
        return delegatedStore.createType(type);
    }

    @Override
    public Type getType(String typeName) {
        return delegatedStore.getType(typeName);
    }

    @Override
    public boolean dropType(String typeName) {
        return delegatedStore.dropType(typeName);
    }

    @Override
    public void createTable(Table tbl) throws InvalidObjectException, MetaException {
        if (readonly) {
            throw new MetaException("Cannot create table in readonly mode");
        }
        if (delegateOnly) {
            delegatedStore.createTable(tbl);
            return;
        }
        tbl.setParameters(checkAndGetLmsFlagParameters(tbl.getParameters()));
        lmsStore.createTable(tbl);
        if (doubleWrite) {
            try {
                delegatedStore.createTable(tbl);
            } catch (Exception e) {
                LOG.error(String
                        .format("Cannot rollback createTable(%s) , please rollback the operation manually", tbl), e);
                throw e;
            }
        }
    }

    @Override
    public boolean dropTable(String dbName, String tableName)
        throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        if (readonly) {
            throw new MetaException("Cannot drop table in readonly mode");
        }
        if (delegateOnly) {
            delegatedStore.dropTable(dbName, tableName);
        }
        boolean success = false;
        MetaObjectName objectName = getObjectFromNameMap(dbName, tableName);
        if (objectName != null) {
            success = lmsStore.dropTable(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName());
            if (doubleWrite) {
                try {
                    return delegatedStore.dropTable(dbName, tableName) && success;
                } catch (Exception e) {
                    LOG.error(String
                        .format("Cannot rollback dropTable(%s, %s), please rollback the operation manually", dbName,
                            tableName), e);
                    throw e;
                }
            }
            return success;
        }
        return delegatedStore.dropTable(dbName, tableName);
    }

    @Override
    public Table getTable(String dbName, String tableName) throws MetaException {
        if (delegateOnly) {
            return delegatedStore.getTable(dbName, tableName);
        }
        MetaObjectName objectName = getObjectFromNameMap(dbName, tableName);
        if (objectName != null) {
            Table table = lmsStore.getTable(objectName.getCatalogName(), objectName.getDatabaseName(),
                    objectName.getObjectName());
            return table;
        }
        return null;
    }

    @Override
    public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
        if (readonly) {
            throw new MetaException("Cannot add partition in readonly mode");
        }
        if (delegateOnly) {
            return delegatedStore.addPartition(part);
        }
        MetaObjectName objectName = getObjectFromNameMap(part.getDbName(), part.getTableName());
        if (objectName != null) {
            lmsStore.addPartition(objectName.getCatalogName(), objectName.getDatabaseName(),
                    objectName.getObjectName(), part);
            if (doubleWrite) {
                try {
                    return delegatedStore.addPartition(part);
                } catch (Exception e) {
                    LOG.error(String
                            .format("Cannot rollback addPartition(%s), please rollback the operation manually", part), e);
                    throw e;
                }
            }
            return true;
        } else {
            throw new InvalidObjectException(
                    "Partition doesn't have a valid table or database name");
        }
    }

    @Override
    public boolean addPartitions(String dbName, String tblName, List<Partition> parts)
        throws InvalidObjectException, MetaException {
        if (readonly) {
            throw new MetaException("Cannot add partitions in readonly mode");
        }
        if (delegateOnly) {
            return delegatedStore.addPartitions(dbName, tblName, parts);
        }
        LOG.debug("addPartitions: {}.{} start", dbName, tblName);
        MetaObjectName objectName = getObjectFromNameMap(dbName, tblName);
        if (objectName != null) {
            Table table = lmsStore.getTable(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName());
            lmsStore.addPartitions(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), table, parts);
            LOG.debug("addPartitions: {}.{} end", dbName, tblName);
            if (doubleWrite) {
                try {
                    return delegatedStore.addPartitions(dbName, tblName, parts);
                } catch (Exception e) {
                        LOG.error(String
                            .format("Cannot rollback addPartitions(%s, %s, %s), please rollback the operation manually",
                                dbName, tblName, parts), e);
                        throw new MetaException(e.getMessage());
                }

            }
            return true;
        }
        return delegatedStore.addPartitions(dbName, tblName, parts);

    }

    @Override
    public boolean addPartitions(String dbName, String tblName, PartitionSpecProxy partitionSpec,
        boolean ifNotExists) throws InvalidObjectException, MetaException {
        if (readonly) {
            throw new MetaException("Cannot add partitions in readonly mode");
        }
        if (delegateOnly) {
            return delegatedStore.addPartitions(dbName, tblName, partitionSpec, ifNotExists);
        }
        LOG.debug("addPartitions: {}.{} start", dbName, tblName);
        MetaObjectName objectName = getObjectFromNameMap(dbName, tblName);
        if (objectName != null) {
            Table table = lmsStore.getTable(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName());
            List<Partition> parts = new ArrayList<>();
            partitionSpec.getPartitionIterator().forEachRemaining(parts::add);
            lmsStore.addPartitions(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), table, parts, ifNotExists);
            LOG.debug("addPartitions: {}.{} end", dbName, tblName);
            if (doubleWrite) {
                try {
                    return delegatedStore.addPartitions(dbName, tblName, partitionSpec, ifNotExists);
                } catch (Exception e) {
                    LOG.error(String
                        .format("Cannot rollback addPartitions(%s, %s, %s), please rollback the operation manually",
                            dbName, tblName, partitionSpec), e);
                    throw e;
                }
            }
            return true;
        }
        return delegatedStore.addPartitions(dbName, tblName, partitionSpec, ifNotExists);
    }

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> part_vals)
        throws MetaException, NoSuchObjectException {
        if (delegateOnly) {
            return delegatedStore.getPartition(dbName, tableName, part_vals);
        }
        MetaObjectName objectName = getObjectFromNameMap(dbName, tableName);
        if (objectName != null) {
            return lmsStore.getPartition(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), part_vals);
        }
        return delegatedStore.getPartition(dbName, tableName, part_vals);
    }

    @Override
    public boolean doesPartitionExist(String dbName, String tableName, List<String> part_vals)
        throws MetaException, NoSuchObjectException {
        if (delegateOnly) {
            return delegatedStore.doesPartitionExist(dbName, tableName, part_vals);
        }
        LOG.debug("doesPartitionExist: {}.{} start", dbName, tableName);
        boolean doesPartitionExist = lmsStore.doesPartitionExist(defaultCatalogName,
                dbName, tableName, part_vals);
        LOG.debug("doesPartitionExist: {}.{} end", dbName, tableName);
        return doesPartitionExist;
    }

    @Override
    public boolean dropPartition(String dbName, String tableName, List<String> part_vals)
        throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        if (readonly) {
            throw new MetaException("Cannot drop partition in readonly mode");
        }
        if (delegateOnly) {
            return delegatedStore.dropPartition(dbName, tableName, part_vals);
        }
        MetaObjectName objectName = getObjectFromNameMap(dbName, tableName);
        boolean success = false;
        if (objectName != null) {
            success = lmsStore.dropPartition(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), part_vals);
            if (doubleWrite) {
                try {
                    return delegatedStore.dropPartition(dbName, tableName, part_vals) && success;
                } catch (Exception exception) {
                    LOG.error(String
                        .format("Cannot rollback dropPartition(%s, %s, %s), please rollback the operation manually",
                            dbName, tableName, part_vals));
                    throw exception;
                }
            }
            return success;
        }
        return delegatedStore.dropPartition(dbName, tableName, part_vals);

    }

    @Override
    public List<Partition> getPartitions(String dbName, String tableName, int max)
        throws MetaException, NoSuchObjectException {
        if (delegateOnly) {
            return delegatedStore.getPartitions(dbName, tableName, max);
        }
        MetaObjectName objectName = getObjectFromNameMap(dbName, tableName);
        if (objectName != null) {
            List<Partition> partitions = lmsStore.getPartitions(objectName.getCatalogName(),
                objectName.getDatabaseName(), objectName.getObjectName(), max);
            return partitions;
        }
        return delegatedStore.getPartitions(dbName, tableName, max);
    }

    private void migrateHandleCatalogException(CatalogException e) {
        if (e.getStatusCode() != HttpStatus.FORBIDDEN_403) {
            throw new CatalogException(e);
        }
    }

    private boolean migrateCheckIsTableExist(CatalogException e) {
        if (e.getStatusCode() == HttpStatus.FORBIDDEN_403) {
            return true;
        }
        return false;
    }

    private void migrateTableFromHmsToLms(Table newTable, List<Partition> partitions)
        throws InvalidObjectException, MetaException, NoSuchObjectException {
        String catalogName = getCatName(newTable);
        newTable.getParameters().remove(LMS_NAME);
        try {
            lmsStore.createTable(catalogName, newTable);
        } catch (CatalogException e) {
            if (migrateCheckIsTableExist(e)) {
                return;
            }
            throw new CatalogException(e);
        }

        if (partitions != null && !partitions.isEmpty()) {
            lmsStore.addPartitions(catalogName, newTable.getDbName(), newTable.getTableName(), newTable, partitions);
        }

        newTable = delegatedStore.getTable(newTable.getDbName(), newTable.getTableName());
        newTable.getParameters().put(LMS_NAME, catalogName);
        lmsStore.alterTable(catalogName, newTable.getDbName(), newTable.getTableName(), newTable);
    }

    @Override
    public void alterTable(String dbname, String name, Table newTable)
        throws InvalidObjectException, MetaException {
        if (readonly) {
            throw new MetaException("Cannot alter table in readonly mode");
        }
        if (delegateOnly) {
            delegatedStore.alterTable(dbname, name, newTable);
            return;
        }
        MetaObjectName objectName = getObjectFromNameMap(dbname, name);
        if (objectName != null) {
            lmsStore.alterTable(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), newTable);
            if (doubleWrite) {
                try {
                    delegatedStore.alterTable(dbname, name, newTable);
                } catch (Exception e) {
                    LOG.error(String
                        .format("Cannot rollback alterTable(%s, %s, %s), please rollback the operation manually",
                            dbname, name, newTable), e);
                    throw e;
                }
            }
            return;
        }

        if (delegatedStore.getTable(dbname, name) == null) {
            if (newTable.getParameters().get(LMS_NAME) != null) {
                lmsStore.alterTable(getCatName(newTable), objectName.getDatabaseName(),
                    objectName.getObjectName(), newTable);
            }
            return;
        }

        try {
            if (newTable.getParameters().get(LMS_NAME) != null) {
                if (newTable.getPartitionKeys() != null && newTable.getPartitionKeys().size() != 0) {
                    List<Partition> partitions = delegatedStore.getPartitions(dbname, name, 1000);
                    migrateTableFromHmsToLms(newTable, partitions);
                } else {
                    migrateTableFromHmsToLms(newTable, null);
                }
            }
            delegatedStore.alterTable(dbname, name, newTable);
        } catch (NoSuchObjectException e) {
            throw new InvalidObjectException(e.getMessage());
        }
    }

    @Override
    public List<String> getTables(String dbName, String pattern) throws MetaException {
        List<String> tableList = new ArrayList<String>();
        if (delegateOnly) {
            tableList = delegatedStore.getTables(dbName, pattern);
        }
        MetaObjectName objectName = getObjectFromNameMap(dbName);
        if (objectName != null) {
            List<String> lmsTables = lmsStore.getTables(objectName.getCatalogName(), dbName, pattern);
            tableList.removeAll(lmsTables);
            tableList.addAll(lmsTables);
        }
        return tableList;
    }

    @Override
    public List<String> getTables(String dbName, String pattern, TableType tableType)
        throws MetaException {
        List<String> tableList = new ArrayList<String>();
        if (delegateOnly) {
            tableList = delegatedStore.getTables(dbName, pattern);
        }
        MetaObjectName objectName = getObjectFromNameMap(dbName);
        if (objectName != null) {
            List<String> lmsTables = lmsStore.getTables(objectName.getCatalogName(), dbName, pattern, tableType);
            tableList.removeAll(lmsTables);
            tableList.addAll(lmsTables);
        }
        return tableList;
    }

    @Override
    public List<TableMeta> getTableMeta(String dbNames, String tableNames, List<String> tableTypes)
        throws MetaException {
        return delegatedStore.getTableMeta(dbNames, tableNames, tableTypes);
    }

    @Override
    public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
        throws MetaException, UnknownDBException {
        if (delegateOnly) {
            return delegatedStore.getTableObjectsByName(dbName, tableNames);
        }
        MetaObjectName objectName = getObjectFromNameMap(dbName);
        if (objectName != null) {
            return lmsStore.getTableObjectsByName(objectName.getCatalogName(), dbName, tableNames);
        }
        return delegatedStore.getTableObjectsByName(dbName, tableNames);
    }

    @Override
    public List<String> getAllTables(String dbName) throws MetaException {
        return getTables(dbName, "*");
    }

    @Override
    public List<String> listTableNamesByFilter(String dbName, String filter, short max_tables)
        throws MetaException, UnknownDBException {
        final MetaObjectName objectName = getObjectFromNameMap(dbName);
        if (objectName != null) {
            return lmsStore.listTableNamesByFilter(objectName.getCatalogName(), dbName, filter, max_tables);
        }
        return delegatedStore.listTableNamesByFilter(dbName, filter, max_tables);
    }

    @Override
    public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts)
        throws MetaException {
        if (delegateOnly) {
            return delegatedStore.listPartitionNames(db_name, tbl_name, max_parts);
        }
        MetaObjectName objectName = getObjectFromNameMap(db_name, tbl_name);
        if (objectName != null) {
            return lmsStore.listPartitionNames(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), max_parts);
        }
        return delegatedStore.listPartitionNames(db_name, tbl_name, max_parts);
    }

    @Override
    public PartitionValuesResponse listPartitionValues(String db_name, String tbl_name,
        List<FieldSchema> cols, boolean applyDistinct, String filter, boolean ascending,
        List<FieldSchema> order, long maxParts) throws MetaException {
        if (delegateOnly) {
            return delegatedStore.listPartitionValues(db_name, tbl_name, cols, applyDistinct, filter, ascending, order,
                maxParts);
        }
        MetaObjectName objectName = getObjectFromNameMap(db_name, tbl_name);
        if (objectName != null) {
            return lmsStore.listPartitionValues(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), cols, applyDistinct, filter, ascending, order, maxParts);
        }
        return delegatedStore.listPartitionValues(db_name, tbl_name, cols, applyDistinct, filter, ascending, order,
            maxParts);
    }

    @Override
    public List<String> listPartitionNamesByFilter(String db_name, String tbl_name, String filter,
        short max_parts) throws MetaException {
        if (delegateOnly) {
            return delegatedStore.listPartitionNamesByFilter(db_name, tbl_name, filter, max_parts);
        }
        MetaObjectName objectName = getObjectFromNameMap(db_name, tbl_name);
        if (objectName != null) {
            return lmsStore.listPartitionNamesByFilter(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), filter, max_parts);
        }
        return delegatedStore.listPartitionNamesByFilter(db_name, tbl_name, filter, max_parts);
    }

    @Override
    public void alterPartition(String db_name, String tbl_name, List<String> part_vals,
        Partition new_part) throws InvalidObjectException, MetaException {
        if (readonly) {
            throw new MetaException("Cannot alter partition in readonly mode");
        }
        if (delegateOnly) {
            delegatedStore.alterPartition(db_name, tbl_name, part_vals, new_part);
        }
        MetaObjectName objectName = getObjectFromNameMap(db_name, tbl_name);
        if (objectName != null) {
            lmsStore.alterPartition(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), part_vals, new_part);
            if (doubleWrite) {
                try {
                    delegatedStore.alterPartition(db_name, tbl_name, part_vals, new_part);
                } catch (Exception e) {
                    LOG.error(String
                        .format("Cannot rollback alterPartition(%s, %s, %s), please rollback the operation manually",
                            db_name, tbl_name, part_vals), e);
                    throw e;
                }
            }
            return;
        }
        delegatedStore.alterPartition(db_name, tbl_name, part_vals, new_part);
    }

    @Override
    public void alterPartitions(String db_name, String tbl_name, List<List<String>> part_vals_list,
        List<Partition> new_parts) throws InvalidObjectException, MetaException {
        if (readonly) {
            throw new MetaException("Cannot alter partitions in readonly mode");
        }
        if (delegateOnly) {
            delegatedStore.alterPartitions(db_name, tbl_name, part_vals_list, new_parts);
        }
        MetaObjectName objectName = getObjectFromNameMap(db_name, tbl_name);
        if (objectName != null) {
            lmsStore.alterPartitions(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), part_vals_list, new_parts);
            if (doubleWrite) {
                try {
                    delegatedStore.alterPartitions(db_name, tbl_name, part_vals_list, new_parts);
                } catch (Exception e) {
                    LOG.error(String
                        .format(
                            "Cannot rollback alterPartitions(%s, %s, %s) error, please rollback the operation manually",
                            db_name, tbl_name, part_vals_list), e);
                    throw e;
                }
            }
            return;
        }
        delegatedStore.alterPartitions(db_name, tbl_name, part_vals_list, new_parts);
    }

    @Override
    public boolean addIndex(Index index) throws InvalidObjectException, MetaException {
        return delegatedStore.addIndex(index);
    }

    @Override
    public Index getIndex(String dbName, String origTableName, String indexName)
        throws MetaException {
        return delegatedStore.getIndex(dbName, origTableName, indexName);
    }

    @Override
    public boolean dropIndex(String dbName, String origTableName, String indexName)
        throws MetaException {
        return delegatedStore.dropIndex(dbName, origTableName, indexName);
    }

    @Override
    public List<Index> getIndexes(String dbName, String origTableName, int max)
        throws MetaException {
        return delegatedStore.getIndexes(dbName, origTableName, max);
    }

    @Override
    public List<String> listIndexNames(String dbName, String origTableName, short max)
        throws MetaException {
        return delegatedStore.listIndexNames(dbName, origTableName, max);
    }

    @Override
    public void alterIndex(String dbname, String baseTblName, String name, Index newIndex)
        throws InvalidObjectException, MetaException {
        delegatedStore.alterIndex(dbname, baseTblName, name, newIndex);
    }

    @Override
    public List<Partition> getPartitionsByFilter(String dbName, String tblName, String filter,
        short maxParts) throws MetaException, NoSuchObjectException {
        if (delegateOnly) {
            return delegatedStore.getPartitionsByFilter(dbName, tblName, filter, maxParts);
        }
        MetaObjectName objectName = getObjectFromNameMap(dbName, tblName);
        if (objectName != null) {
            return lmsStore.getPartitionsByFilter(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), filter, maxParts);
        }
        return delegatedStore.getPartitionsByFilter(dbName, tblName, filter, maxParts);
    }

    @Override
    public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr,
        String defaultPartitionName, short maxParts, List<Partition> result) throws TException {
        if (delegateOnly) {
            return delegatedStore.getPartitionsByExpr(dbName, tblName, expr, defaultPartitionName, maxParts, result);
        }
        MetaObjectName objectName = getObjectFromNameMap(dbName, tblName);
        if (objectName != null) {
            return lmsStore.getPartitionsByExpr(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), expr, defaultPartitionName, maxParts, result);
        }
        return delegatedStore.getPartitionsByExpr(dbName, tblName, expr, defaultPartitionName, maxParts, result);
    }

    @Override
    public int getNumPartitionsByFilter(String dbName, String tblName, String filter)
        throws MetaException, NoSuchObjectException {
        return delegatedStore.getNumPartitionsByFilter(dbName, tblName, filter);
    }

    @Override
    public int getNumPartitionsByExpr(String dbName, String tblName, byte[] expr)
        throws MetaException, NoSuchObjectException {
        return delegatedStore.getNumPartitionsByExpr(dbName, tblName, expr);
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tblName,
        List<String> partNames) throws MetaException, NoSuchObjectException {
        if (delegateOnly) {
            return delegatedStore.getPartitionsByNames(dbName, tblName, partNames);
        }
        MetaObjectName objectName = getObjectFromNameMap(dbName, tblName);
        if (objectName != null) {
            return lmsStore.getPartitionsByNames(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), partNames);
        }
        return delegatedStore.getPartitionsByNames(dbName, tblName, partNames);
    }

    @Override
    public Table markPartitionForEvent(String dbName, String tblName, Map<String, String> partVals,
        PartitionEventType evtType)
        throws MetaException, UnknownTableException, InvalidPartitionException,
        UnknownPartitionException {
        return delegatedStore.markPartitionForEvent(dbName, tblName, partVals, evtType);
    }

    @Override
    public boolean isPartitionMarkedForEvent(String dbName, String tblName,
        Map<String, String> partName, PartitionEventType evtType)
        throws MetaException, UnknownTableException, InvalidPartitionException,
        UnknownPartitionException {
        return delegatedStore.isPartitionMarkedForEvent(dbName, tblName, partName, evtType);
    }

    @Override
    public boolean addRole(String rowName, String ownerName)
        throws InvalidObjectException, MetaException, NoSuchObjectException {
        return delegatedStore.addRole(rowName, ownerName);
    }

    @Override
    public boolean removeRole(String roleName) throws MetaException, NoSuchObjectException {
        return delegatedStore.removeRole(roleName);
    }

    @Override
    public boolean grantRole(Role role, String userName, PrincipalType principalType,
        String grantor, PrincipalType grantorType, boolean grantOption)
        throws MetaException, NoSuchObjectException, InvalidObjectException {
        return delegatedStore.grantRole(role, userName, principalType, grantor, grantorType, grantOption);
    }

    @Override
    public boolean revokeRole(Role role, String userName, PrincipalType principalType,
        boolean grantOption) throws MetaException, NoSuchObjectException {
        return delegatedStore.revokeRole(role, userName, principalType, grantOption);
    }

    @Override
    public PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames)
        throws InvalidObjectException, MetaException {
        return delegatedStore.getUserPrivilegeSet(userName, groupNames);
    }

    @Override
    public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName, String userName,
        List<String> groupNames) throws InvalidObjectException, MetaException {
        return delegatedStore.getDBPrivilegeSet(dbName, userName, groupNames);
    }

    @Override
    public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName, String tableName,
        String userName, List<String> groupNames) throws InvalidObjectException, MetaException {
        return delegatedStore.getTablePrivilegeSet(dbName, tableName, userName, groupNames);
    }

    @Override
    public PrincipalPrivilegeSet getPartitionPrivilegeSet(String dbName, String tableName,
        String partition, String userName, List<String> groupNames)
        throws InvalidObjectException, MetaException {
        return delegatedStore.getPartitionPrivilegeSet(dbName, tableName, partition, userName, groupNames);
    }

    @Override
    public PrincipalPrivilegeSet getColumnPrivilegeSet(String dbName, String tableName,
        String partitionName, String columnName, String userName, List<String> groupNames)
        throws InvalidObjectException, MetaException {
        return delegatedStore.getColumnPrivilegeSet(dbName, tableName, partitionName, columnName, userName,
            groupNames);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName,
        PrincipalType principalType) {
        return delegatedStore.listPrincipalGlobalGrants(principalName, principalType);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
        PrincipalType principalType, String dbName) {
        return delegatedStore.listPrincipalDBGrants(principalName, principalType, dbName);
    }

    @Override
    public List<HiveObjectPrivilege> listAllTableGrants(String principalName,
        PrincipalType principalType, String dbName, String tableName) {
        return delegatedStore.listAllTableGrants(principalName, principalType, dbName, tableName);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String principalName,
        PrincipalType principalType, String dbName, String tableName, List<String> partValues,
        String partName) {
        return delegatedStore.listPrincipalPartitionGrants(principalName, principalType, dbName, tableName,
            partValues, partName);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String principalName,
        PrincipalType principalType, String dbName, String tableName, String columnName) {
        return delegatedStore.listPrincipalTableColumnGrants(principalName, principalType, dbName, tableName,
            columnName);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(String principalName,
        PrincipalType principalType, String dbName, String tableName, List<String> partValues,
        String partName, String columnName) {
        return delegatedStore.listPrincipalPartitionColumnGrants(principalName, principalType, dbName, tableName,
            partValues, partName, columnName);
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag privileges)
        throws InvalidObjectException, MetaException, NoSuchObjectException {
        return delegatedStore.grantPrivileges(privileges);
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
        throws InvalidObjectException, MetaException, NoSuchObjectException {
        return delegatedStore.revokePrivileges(privileges, grantOption);
    }

    @Override
    public Role getRole(String roleName) throws NoSuchObjectException {
        return delegatedStore.getRole(roleName);
    }

    @Override
    public List<String> listRoleNames() {
        return delegatedStore.listRoleNames();
    }

    @Override
    public List<Role> listRoles(String principalName, PrincipalType principalType) {
        return delegatedStore.listRoles(principalName, principalType);
    }

    @Override
    public List<RolePrincipalGrant> listRolesWithGrants(String principalName,
        PrincipalType principalType) {
        return delegatedStore.listRolesWithGrants(principalName, principalType);
    }

    @Override
    public List<RolePrincipalGrant> listRoleMembers(String roleName) {
        return delegatedStore.listRoleMembers(roleName);
    }

    @Override
    public Partition getPartitionWithAuth(String dbName, String tblName, List<String> partVals,
        String user_name, List<String> group_names)
        throws MetaException, NoSuchObjectException, InvalidObjectException {
        if (delegateOnly) {
            return delegatedStore.getPartitionWithAuth(dbName, tblName, partVals, user_name, group_names);
        }
        MetaObjectName objectName = getObjectFromNameMap(dbName, tblName);
        if (objectName != null) {
            return lmsStore.getPartitionWithAuth(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), partVals, user_name, group_names);
        }
        return delegatedStore.getPartitionWithAuth(dbName, tblName, partVals, user_name, group_names);
    }

    @Override
    public List<Partition> getPartitionsWithAuth(String dbName, String tblName, short maxParts,
        String userName, List<String> groupNames)
        throws MetaException, NoSuchObjectException, InvalidObjectException {
        if (delegateOnly) {
            return delegatedStore.getPartitionsWithAuth(dbName, tblName, maxParts, userName, groupNames);
        }
        MetaObjectName objectName = getObjectFromNameMap(dbName, tblName);
        if (objectName != null) {
            return lmsStore.getPartitionsWithAuth(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), maxParts, userName, groupNames);
        }
        return delegatedStore.getPartitionsWithAuth(dbName, tblName, maxParts, userName, groupNames);
    }

    @Override
    public List<String> listPartitionNamesPs(String db_name, String tbl_name,
        List<String> part_vals, short max_parts) throws MetaException, NoSuchObjectException {
        if (delegateOnly) {
            return delegatedStore.listPartitionNamesPs(db_name, tbl_name, part_vals, max_parts);
        }
        MetaObjectName objectName = getObjectFromNameMap(db_name, tbl_name);
        if (objectName != null) {
            return lmsStore.listPartitionNamesPs(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), part_vals, max_parts);
        }
        return delegatedStore.listPartitionNamesPs(db_name, tbl_name, part_vals, max_parts);
    }

    @Override
    public List<Partition> listPartitionsPsWithAuth(String db_name, String tbl_name,
        List<String> part_vals, short max_parts, String userName, List<String> groupNames)
        throws MetaException, InvalidObjectException, NoSuchObjectException {
        if (delegateOnly) {
            return delegatedStore.listPartitionsPsWithAuth(db_name, tbl_name, part_vals, max_parts, userName,
                groupNames);
        }
        MetaObjectName objectName = getObjectFromNameMap(db_name, tbl_name);
        if (objectName != null) {
            return lmsStore.listPartitionsPsWithAuth(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), part_vals, max_parts, userName, groupNames);
        }
        return delegatedStore.listPartitionsPsWithAuth(db_name, tbl_name, part_vals, max_parts, userName,
            groupNames);
    }

    @Override
    public boolean updateTableColumnStatistics(ColumnStatistics colStats)
        throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        ColumnStatisticsDesc statsObj = colStats.getStatsDesc();
        MetaObjectName objectName = getObjectFromNameMap(statsObj.getDbName(), statsObj.getTableName());
        if (objectName != null) {
            return lmsStore.updateTableColumnStatistics(objectName.getCatalogName(), colStats);
        }
        return delegatedStore.updateTableColumnStatistics(colStats);
    }

    @Override
    public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj, List<String> partVals)
        throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        ColumnStatisticsDesc statsDesc = statsObj.getStatsDesc();
        MetaObjectName objectName = getObjectFromNameMap(statsDesc.getDbName(), statsDesc.getTableName());
        if (objectName != null) {
            return lmsStore.updatePartitionColumnStatistics(objectName.getCatalogName(), statsObj, partVals);
        }
        return delegatedStore.updatePartitionColumnStatistics(statsObj, partVals);
    }

    @Override
    public ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
        List<String> colName) throws MetaException, NoSuchObjectException {
        MetaObjectName objectName = getObjectFromNameMap(dbName, tableName);
        if (objectName != null) {
            return lmsStore.getTableColumnStatistics(objectName.getCatalogName(), dbName, tableName, colName);
        }
        return delegatedStore.getTableColumnStatistics(dbName, tableName, colName);
    }

    @Override
    public List<ColumnStatistics> getPartitionColumnStatistics(String dbName, String tblName,
        List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
        MetaObjectName objectName = getObjectFromNameMap(dbName, tblName);
        if (objectName != null) {
            return lmsStore.getPartitionColumnStatistics(objectName.getCatalogName(), dbName,
                    tblName, partNames, colNames);
        }
        return delegatedStore.getPartitionColumnStatistics(dbName, tblName, partNames, colNames);
    }

    @Override
    public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
        List<String> partVals, String colName)
        throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        MetaObjectName objectName = getObjectFromNameMap(dbName, tableName);
        if (objectName != null) {
            return lmsStore.deletePartitionColumnStatistics(objectName.getCatalogName(),
                    dbName, tableName, partName, partVals, colName);
        }
        return delegatedStore.deletePartitionColumnStatistics(dbName, tableName, partName, partVals, colName);
    }

    @Override
    public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
        throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        MetaObjectName objectName = getObjectFromNameMap(dbName, tableName);
        if (objectName != null) {
            return lmsStore.deleteTableColumnStatistics(objectName.getCatalogName(), dbName, tableName, colName);
        }
        return delegatedStore.deleteTableColumnStatistics(dbName, tableName, colName);
    }

    @Override
    public long cleanupEvents() {
        return delegatedStore.cleanupEvents();
    }

    @Override
    public boolean addToken(String tokenIdentifier, String delegationToken) {
        return delegatedStore.addToken(tokenIdentifier, delegationToken);
    }

    @Override
    public boolean removeToken(String tokenIdentifier) {
        return delegatedStore.removeToken(tokenIdentifier);
    }

    @Override
    public String getToken(String tokenIdentifier) {
        return delegatedStore.getToken(tokenIdentifier);
    }

    @Override
    public List<String> getAllTokenIdentifiers() {
        return delegatedStore.getAllTokenIdentifiers();
    }

    @Override
    public int addMasterKey(String key) throws MetaException {
        return delegatedStore.addMasterKey(key);
    }

    @Override
    public void updateMasterKey(Integer seqNo, String key)
        throws NoSuchObjectException, MetaException {
        delegatedStore.updateMasterKey(seqNo, key);
    }

    @Override
    public boolean removeMasterKey(Integer keySeq) {
        return delegatedStore.removeMasterKey(keySeq);
    }

    @Override
    public String[] getMasterKeys() {
        return delegatedStore.getMasterKeys();
    }

    @Override
    public void verifySchema() throws MetaException {
        delegatedStore.verifySchema();
    }

    @Override
    public String getMetaStoreSchemaVersion() throws MetaException {
        return delegatedStore.getMetaStoreSchemaVersion();
    }

    @Override
    public void setMetaStoreSchemaVersion(String version, String comment) throws MetaException {
        delegatedStore.setMetaStoreSchemaVersion(version, comment);
    }

    @Override
    public void dropPartitions(String dbName, String tblName, List<String> partNames)
        throws MetaException, NoSuchObjectException {
        if (readonly) {
            throw new MetaException("Cannot drop partitions in readonly mode");
        }
        if (delegateOnly) {
            delegatedStore.dropPartitions(dbName, tblName, partNames);
            return;
        }
        MetaObjectName objectName = getObjectFromNameMap(dbName, tblName);
        if (objectName != null) {
            lmsStore.dropPartitions(objectName.getCatalogName(), objectName.getDatabaseName(),
                objectName.getObjectName(), partNames);
            if (doubleWrite) {
                try {
                    delegatedStore.dropPartitions(dbName, tblName, partNames);
                } catch (Exception e) {
                    LOG.error(String
                        .format(
                            "Cannot rollback dropPartitions(%s, %s, %s) error, please rollback the operation manually",
                            dbName, tblName, partNames), e);
                    throw e;
                }
            }
        } else {
            delegatedStore.dropPartitions(dbName, tblName, partNames);
        }

    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(String principalName,
        PrincipalType principalType) {
        return delegatedStore.listPrincipalDBGrantsAll(principalName, principalType);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String principalName,
        PrincipalType principalType) {
        return delegatedStore.listPrincipalTableGrantsAll(principalName, principalType);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(String principalName,
        PrincipalType principalType) {
        return delegatedStore.listPrincipalPartitionGrantsAll(principalName, principalType);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(String principalName,
        PrincipalType principalType) {
        return delegatedStore.listPrincipalTableColumnGrantsAll(principalName, principalType);
    }

    @Override
    public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(String principalName,
        PrincipalType principalType) {
        return delegatedStore.listPrincipalPartitionColumnGrantsAll(principalName, principalType);
    }

    @Override
    public List<HiveObjectPrivilege> listGlobalGrantsAll() {
        return delegatedStore.listGlobalGrantsAll();
    }

    @Override
    public List<HiveObjectPrivilege> listDBGrantsAll(String dbName) {
        return delegatedStore.listDBGrantsAll(dbName);
    }

    @Override
    public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String dbName, String tableName,
        String partitionName, String columnName) {
        return delegatedStore.listPartitionColumnGrantsAll(dbName, tableName, partitionName, columnName);
    }

    @Override
    public List<HiveObjectPrivilege> listTableGrantsAll(String dbName, String tableName) {
        return delegatedStore.listTableGrantsAll(dbName, tableName);
    }

    @Override
    public List<HiveObjectPrivilege> listPartitionGrantsAll(String dbName, String tableName,
        String partitionName) {
        return delegatedStore.listPartitionGrantsAll(dbName, tableName, partitionName);
    }

    @Override
    public List<HiveObjectPrivilege> listTableColumnGrantsAll(String dbName, String tableName,
        String columnName) {
        return delegatedStore.listTableColumnGrantsAll(dbName, tableName, columnName);
    }

    @Override
    public void createFunction(Function func) throws InvalidObjectException, MetaException {
        if (readonly) {
            throw new MetaException("Cannot create function in readonly mode");
        }
        if (delegateOnly) {
            delegatedStore.createFunction(func);
            return;
        }
        MetaObjectName objectName = getObjectFromNameMap(func.getDbName());
        if (objectName != null) {
            lmsStore.createFunction(objectName.getCatalogName(), func);
            if (doubleWrite) {
                try {
                    delegatedStore.createFunction(func);
                } catch (Exception e) {
                    LOG.error(String
                        .format("Cannot rollback createFunction(%s) error, please rollback the operation manually",
                            func), e);
                    throw e;
                }
            }
            return;
        }
        delegatedStore.createFunction(func);
    }

    @Override
    public void alterFunction(String dbName, String funcName, Function newFunction)
        throws InvalidObjectException, MetaException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(String dbName, String funcName)
        throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        if (readonly) {
            throw new MetaException("Cannot drop function in readonly mode");
        }
        if (delegateOnly) {
            delegatedStore.dropFunction(dbName, funcName);
            return;
        }
        MetaObjectName objectName = getObjectFromNameMap(dbName);
        if (objectName != null) {
            lmsStore.dropFunction(objectName.getCatalogName(), dbName, funcName);
            if (doubleWrite) {
                try {
                    delegatedStore.dropFunction(dbName, funcName);
                } catch (Exception e) {
                    LOG.error(String
                        .format("Cannot rollback createFunction(%s, %s) error, please rollback the operation manually",
                            dbName, funcName), e);
                    throw e;
                }
            }
        }

        delegatedStore.dropFunction(dbName, funcName);
    }

    @Override
    public Function getFunction(String dbName, String funcName) throws MetaException {
        if (delegateOnly) {
            return delegatedStore.getFunction(dbName, funcName);
        }
        Function function = null;
        MetaObjectName objectName = getObjectFromNameMap(dbName);
        if (objectName != null) {
            function = lmsStore.getFunction(objectName.getCatalogName(), dbName, funcName);
        }
        if (function == null) {
            return delegatedStore.getFunction(dbName, funcName);
        }
        return function;
    }

    @Override
    public List<Function> getAllFunctions() throws MetaException {
        List<Function> functions = lmsStore.getAllFunctions(defaultCatalogName);
        functions.addAll(delegatedStore.getAllFunctions());
        return functions;
    }

    @Override
    public List<String> getFunctions(String dbName, String pattern) throws MetaException {
        List<String> functions = new ArrayList<>();
        MetaObjectName objectName = getObjectFromNameMap(dbName);
        if (objectName != null) {
            String resolvedPattern = resolveSyntaxSugar(pattern);
            functions = lmsStore.getFunctions(objectName.getCatalogName(), objectName.getDatabaseName(),
                resolvedPattern);
        }
        functions.addAll(delegatedStore.getFunctions(dbName, pattern));
        return functions;
    }

    private String resolveSyntaxSugar(String pattern) {
        // todo more wildcard character to be resolved
        if (pattern.equals("*")) {
            return "";
        }
        return pattern.replaceAll("\\*", "%");
    }

    @Override
    public AggrStats get_aggr_stats_for(String dbName, String tblName, List<String> partNames,
        List<String> colNames) throws MetaException, NoSuchObjectException {
        MetaObjectName objectName = getObjectFromNameMap(dbName, tblName);
        if (objectName != null) {
            return lmsStore.getAggrColStats(objectName.getCatalogName(), dbName, tblName, partNames, colNames);
        }
        return delegatedStore.get_aggr_stats_for(dbName, tblName, partNames, colNames);
    }

    @Override
    public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
        return delegatedStore.getNextNotification(rqst);
    }

    @Override
    public void addNotificationEvent(NotificationEvent event) {
        delegatedStore.addNotificationEvent(event);
    }

    @Override
    public void cleanNotificationEvents(int olderThan) {
        delegatedStore.cleanNotificationEvents(olderThan);
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() {
        return delegatedStore.getCurrentNotificationEventId();
    }

    @Override
    public void flushCache() {
        delegatedStore.flushCache();
    }

    @Override
    public ByteBuffer[] getFileMetadata(List<Long> fileIds) throws MetaException {
        return delegatedStore.getFileMetadata(fileIds);
    }

    @Override
    public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata,
        FileMetadataExprType type) throws MetaException {
        delegatedStore.putFileMetadata(fileIds, metadata, type);
    }

    @Override
    public boolean isFileMetadataSupported() {
        return delegatedStore.isFileMetadataSupported();
    }

    @Override
    public void getFileMetadataByExpr(List<Long> fileIds, FileMetadataExprType type, byte[] expr,
        ByteBuffer[] metadatas, ByteBuffer[] exprResults, boolean[] eliminated)
        throws MetaException {
        delegatedStore.getFileMetadataByExpr(fileIds, type, expr, metadatas, exprResults, eliminated);
    }

    @Override
    public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type) {
        return delegatedStore.getFileMetadataHandler(type);
    }

    @Override
    public int getTableCount() throws MetaException {
        return delegatedStore.getTableCount();
    }

    @Override
    public int getPartitionCount() throws MetaException {
        return delegatedStore.getPartitionCount();
    }

    @Override
    public int getDatabaseCount() throws MetaException {
        return delegatedStore.getDatabaseCount();
    }

    @Override
    public List<SQLPrimaryKey> getPrimaryKeys(String db_name, String tbl_name)
        throws MetaException {
        return delegatedStore.getPrimaryKeys(db_name, tbl_name);
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(String parent_db_name, String parent_tbl_name,
        String foreign_db_name, String foreign_tbl_name) throws MetaException {
        return delegatedStore.getForeignKeys(parent_db_name, parent_tbl_name, foreign_db_name, foreign_tbl_name);
    }

    @Override
    public void createTableWithConstraints(Table tbl, List<SQLPrimaryKey> primaryKeys,
        List<SQLForeignKey> foreignKeys) throws InvalidObjectException, MetaException {
        delegatedStore.createTableWithConstraints(tbl, primaryKeys, foreignKeys);
    }

    @Override
    public void dropConstraint(String dbName, String tableName, String constraintName)
        throws NoSuchObjectException {
        delegatedStore.dropConstraint(dbName, tableName, constraintName);
    }

    @Override
    public void addPrimaryKeys(List<SQLPrimaryKey> pks)
        throws InvalidObjectException, MetaException {
        delegatedStore.addPrimaryKeys(pks);
    }

    @Override
    public void addForeignKeys(List<SQLForeignKey> fks)
        throws InvalidObjectException, MetaException {
        delegatedStore.addForeignKeys(fks);
    }

    @Override
    public void setConf(Configuration configuration) {
        String readonlyConfig = configuration.get(READONLY_CONFIG);
        readonly = readonlyConfig != null && readonlyConfig.equalsIgnoreCase("true");
        String doubleWriteConfig = configuration.get(DOUBLE_WRITE_CONFIG);
        if (doubleWriteConfig != null && doubleWriteConfig.equalsIgnoreCase("true")) {
            doubleWrite = true;
            LOG.warn("Enable double write mode, the update operations will be non-atomic");
        }
        String delegateOnlyConfig = configuration.get(DELEGATE_ONLY_CONFIG);
        delegateOnly = delegateOnlyConfig != null && delegateOnlyConfig.equalsIgnoreCase("true");
        defaultCatalogName = configuration.get(DEFAULT_CATALOG_NAME);

        if (lmsStore != null) {
            lmsStore.setConf(configuration);
        }
        delegatedStore.setConf(configuration);
    }

    @Override
    public Configuration getConf() {
        return delegatedStore.getConf();
    }

    private String getCatName(Table table) {
        return table.getParameters().get(LMS_NAME);
    }

    public RawStore getDelegatedStore() {
        return delegatedStore;
    }

    public MetaObjectName getObjectFromNameMap(String dbName) {
        try {
            Database database = lmsStore.getDatabase(defaultCatalogName, dbName);
            if (database != null) {
                return new MetaObjectName(lmsStore.getProjectId(), defaultCatalogName, dbName, null);
            }
        }catch (Exception e) {
            LOG.warn("Get database from polycat error: {}, databaseName: {}", e.getMessage(), dbName);
        }
        return null;
    }

    public MetaObjectName getObjectFromNameMap(String dbName, String tableName) {
        try {
            Table table = lmsStore.getTable(defaultCatalogName, dbName, tableName);
            if (table != null) {
                return new MetaObjectName(lmsStore.getProjectId(), defaultCatalogName, dbName, tableName);
            }
        }catch (Exception e) {
            LOG.warn("Get table from polycat error: {}, table: {}.{}", e.getMessage(), dbName, tableName);
        }
        return null;
    }

    static class MetaObjectName {

        private String projectId;

        private String catalogName;

        private String databaseName;

        private String objectName;

        public MetaObjectName(String projectId, String catalogName, String databaseName, String objectName) {
            this.projectId = projectId;
            this.catalogName = catalogName;
            this.databaseName = databaseName;
            this.objectName = objectName;
        }

        public String getProjectId() {
            return projectId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public String getCatalogName() {
            return catalogName;
        }

        public void setCatalogName(String catalogName) {
            this.catalogName = catalogName;
        }

        public String getDatabaseName() {
            return databaseName;
        }

        public void setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
        }

        public String getObjectName() {
            return objectName;
        }

        public void setObjectName(String objectName) {
            this.objectName = objectName;
        }
    }
}