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
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.hive;

import java.util.List;
import java.util.Map;

import io.polycat.hivesdk.hive3.impl.PolyCatMetaStoreClient;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreClient;

import static java.util.Objects.requireNonNull;

public class PolyCatFailureAwareThriftMetastoreClient
    implements ThriftMetastoreClient {

    public interface Callback {

        void success();

        void failed(TException e);
    }

    private static final Logger log = Logger.get(PolyCatFailureAwareThriftMetastoreClient.class);
    private final PolyCatMetaStoreClient delegate;
    private final Callback callback;

    public PolyCatFailureAwareThriftMetastoreClient(PolyCatMetaStoreClient client, Callback callback) {
        this.delegate = requireNonNull(client, "client is null");
        this.callback = requireNonNull(callback, "callback is null");
    }

    @VisibleForTesting
    public PolyCatMetaStoreClient getDelegate() {
        return delegate;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public List<String> getAllDatabases()
        throws TException {
        return runWithHandle(() -> delegate.getAllDatabases());
    }

    @Override
    public Database getDatabase(String databaseName)
        throws TException {
        log.debug("[DEBUG-INFO] getDatabase ");
        return runWithHandle(() -> delegate.getDatabase(databaseName));
    }

    @Override
    public List<String> getAllTables(String databaseName)
        throws TException {
        log.debug("[DEBUG-INFO] getAllTables");
        return runWithHandle(() -> delegate.getAllTables(databaseName));
    }

    @Override
    public List<String> getTableNamesByFilter(String databaseName, String filter)
        throws TException {
        log.debug("[DEBUG-INFO] getTableNamesByFilter");
        return runWithHandle(() -> delegate.getTables(databaseName, filter));
    }

    @Override
    public List<String> getTableNamesByType(String databaseName, String tableType)
        throws TException {
        log.debug("[DEBUG-INFO] getTableNamesByType");
        TableType type = Enum.valueOf(TableType.class, tableType);
        return runWithHandle(() -> delegate.getTables(databaseName, ".*", type));
    }

    @Override
    public void createDatabase(Database database)
        throws TException {
        log.debug("[DEBUG-INFO] createDatabase");
        runWithHandle(() -> delegate.createDatabase(database));
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData, boolean cascade)
        throws TException {
        log.debug("[DEBUG-INFO] dropDatabase");
        runWithHandle(() -> delegate.dropDatabase(databaseName, deleteData, cascade));
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
        throws TException {
        log.debug("[DEBUG-INFO] alterDatabase");
        runWithHandle(() -> delegate.alterDatabase(databaseName, database));
    }

    @Override
    public void createTable(Table table)
        throws TException {
        log.debug("[DEBUG-INFO] createTable");
        runWithHandle(() -> delegate.createTable(table));
    }

    @Override
    public void dropTable(String databaseName, String name, boolean deleteData)
        throws TException {
        log.debug("[DEBUG-INFO] dropTable");
        runWithHandle(() -> delegate.dropTable(databaseName, name));
    }

    @Override
    public void alterTable(String databaseName, String tableName, Table newTable)
        throws TException {
        log.debug("[DEBUG-INFO] alterTable");
        runWithHandle(() -> delegate.alter_table(databaseName, tableName, newTable));
    }

    @Override
    public Table getTable(String databaseName, String tableName)
        throws TException {
        log.debug("[DEBUG-INFO] getTable");
        return runWithHandle(() -> delegate.getTable(databaseName, tableName));
    }

    @Override
    public Table getTableWithCapabilities(String databaseName, String tableName)
        throws TException {
        log.debug("[DEBUG-INFO] getTableWithCapabilities");
        return runWithHandle(() -> delegate.getTable(databaseName, tableName));
    }

    @Override
    public List<FieldSchema> getFields(String databaseName, String tableName)
        throws TException {
        log.debug("[DEBUG-INFO] getFields");
        return runWithHandle(() -> delegate.getFields(databaseName, tableName));
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String databaseName, String tableName,
        List<String> columnNames)
        throws TException {
        log.debug("[DEBUG-INFO] getTableColumnStatistics");
        return runWithHandle(() -> delegate.getTableColumnStatistics(databaseName, tableName, columnNames));
    }

    @Override
    public void setTableColumnStatistics(String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
        throws TException {
        log.debug("[DEBUG-INFO] setTableColumnStatistics");
        ColumnStatisticsDesc statisticsDescription = new ColumnStatisticsDesc(true, databaseName, tableName);
        ColumnStatistics request = new ColumnStatistics(statisticsDescription, statistics);
        runWithHandle(() -> delegate.updateTableColumnStatistics(request));
    }

    @Override
    public void deleteTableColumnStatistics(String databaseName, String tableName, String columnName)
        throws TException {
        log.debug("[DEBUG-INFO] deleteTableColumnStatistics");
        runWithHandle(() -> delegate.deleteTableColumnStatistics(databaseName, tableName, columnName));
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String databaseName, String tableName,
        List<String> partitionNames, List<String> columnNames)
        throws TException {
        log.debug("[DEBUG-INFO] getPartitionColumnStatistics");
        return runWithHandle(
            () -> delegate.getPartitionColumnStatistics(databaseName, tableName, partitionNames, columnNames));
    }

    @Override
    public void setPartitionColumnStatistics(String databaseName, String tableName, String partitionName,
        List<ColumnStatisticsObj> statistics)
        throws TException {
        log.debug("[DEBUG-INFO] setPartitionColumnStatistics");
        ColumnStatisticsDesc statisticsDescription = new ColumnStatisticsDesc(false, databaseName, tableName);
        statisticsDescription.setPartName(partitionName);
        SetPartitionsStatsRequest request = new SetPartitionsStatsRequest();
        request.addToColStats(new ColumnStatistics(statisticsDescription, statistics));
        runWithHandle(() -> delegate.setPartitionColumnStatistics(request));
    }

    @Override
    public void deletePartitionColumnStatistics(String databaseName, String tableName, String partitionName,
        String columnName)
        throws TException {
        log.debug("[DEBUG-INFO] deletePartitionColumnStatistics");
        runWithHandle(
            () -> delegate.deletePartitionColumnStatistics(databaseName, tableName, partitionName, columnName));
    }

    @Override
    public List<String> getPartitionNames(String databaseName, String tableName)
        throws TException {
        log.debug("[DEBUG-INFO] getPartitionNames");
        return runWithHandle(() -> delegate.listPartitionNames(databaseName, tableName, (short) -1));
    }

    @Override
    public List<String> getPartitionNamesFiltered(String databaseName, String tableName, List<String> partitionValues)
        throws TException {
        log.debug("[DEBUG-INFO] getPartitionNamesFiltered");
        return runWithHandle(() -> delegate.listPartitionNames(databaseName, tableName, partitionValues, (short) -1));
    }

    @Override
    public int addPartitions(List<Partition> newPartitions)
        throws TException {
        log.debug("[DEBUG-INFO] addPartitions");
        return runWithHandle(() -> delegate.add_partitions(newPartitions));
    }

    @Override
    public boolean dropPartition(String databaseName, String tableName, List<String> partitionValues,
        boolean deleteData)
        throws TException {
        log.debug("[DEBUG-INFO] dropPartition");
        return runWithHandle(() -> delegate.dropPartition(databaseName, tableName, partitionValues, deleteData));
    }

    @Override
    public void alterPartition(String databaseName, String tableName, Partition partition)
        throws TException {
        log.debug("[DEBUG-INFO] alterPartition");
        runWithHandle(() -> delegate.alter_partition(databaseName, tableName, partition));
    }

    @Override
    public Partition getPartition(String databaseName, String tableName, List<String> partitionValues)
        throws TException {
        log.debug("[DEBUG-INFO] getPartition");
        return runWithHandle(() -> delegate.getPartition(databaseName, tableName, partitionValues));
    }

    @Override
    public List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
        throws TException {
        log.debug("[DEBUG-INFO] getPartitionsByNames");
        return runWithHandle(() -> delegate.getPartitionsByNames(databaseName, tableName, partitionNames));
    }

    @Override
    public List<Role> listRoles(String principalName, PrincipalType principalType)
        throws TException {
        log.debug("[DEBUG-INFO] listRoles");
        return runWithHandle(() -> delegate.list_roles(principalName, principalType));
    }

    @Override
    public List<HiveObjectPrivilege> listPrivileges(String principalName, PrincipalType principalType,
        HiveObjectRef hiveObjectRef)
        throws TException {
        log.debug("[DEBUG-INFO] listPrivileges");
        return runWithHandle(() -> delegate.list_privileges(principalName, principalType, hiveObjectRef));
    }

    @Override
    public List<String> getRoleNames()
        throws TException {
        log.debug("[DEBUG-INFO] getRoleNames");
        return runWithHandle(delegate::listRoleNames);
    }

    @Override
    public void createRole(String role, String grantor)
        throws TException {
        log.debug("[DEBUG-INFO] createRole");
        Role creatRole = new Role(role, 0, grantor);
        runWithHandle(() -> delegate.create_role(creatRole));
    }

    @Override
    public void dropRole(String role)
        throws TException {
        log.debug("[DEBUG-INFO] dropRole");
        runWithHandle(() -> delegate.drop_role(role));
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag privilegeBag)
        throws TException {
        log.debug("[DEBUG-INFO] grantPrivileges");
        return runWithHandle(() -> delegate.grant_privileges(privilegeBag));
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag privilegeBag)
        throws TException {
        log.debug("[DEBUG-INFO] revokePrivileges");
        return runWithHandle(() -> delegate.revoke_privileges(privilegeBag, false));
    }

    @Override
    public void grantRole(String role, String granteeName, PrincipalType granteeType, String grantorName,
        PrincipalType grantorType, boolean grantOption)
        throws TException {
        log.debug("[DEBUG-INFO] grantRole");
        runWithHandle(() -> delegate.grant_role(role, granteeName, granteeType, grantorName, grantorType, grantOption));
    }

    @Override
    public void revokeRole(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
        throws TException {
        log.debug("[DEBUG-INFO] revokeRole");
        runWithHandle(() -> delegate.revoke_role(role, granteeName, granteeType, grantOption));
    }

    @Override
    public List<RolePrincipalGrant> listRoleGrants(String name, PrincipalType principalType)
        throws TException {
        log.debug("[DEBUG-INFO] listRoleGrants");
        GetRoleGrantsForPrincipalRequest req = new GetRoleGrantsForPrincipalRequest(name, principalType);
        GetRoleGrantsForPrincipalResponse resp = runWithHandle(() -> delegate.get_role_grants_for_principal(req));
        return ImmutableList.copyOf(resp.getPrincipalGrants());
    }

    @Override
    public void setUGI(String userName)
        throws TException {
        log.debug("[DEBUG-INFO] setUGI");
        return;
        //runWithHandle(() -> delegate.setUGI(userName));
    }

    @Override
    public long openTransaction(String user)
        throws TException {
        log.debug("[DEBUG-INFO] openTransaction");
        return 0;
        //return runWithHandle(() -> delegate.openTransaction(user));
    }

    @Override
    public void commitTransaction(long transactionId)
        throws TException {
        log.debug("[DEBUG-INFO] commitTransaction");
        return;
        //runWithHandle(() -> delegate.commitTransaction(transactionId));
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
        throws TException {
        log.debug("[DEBUG-INFO] sendTransactionHeartbeat");
        return;
        //runWithHandle(() -> delegate.sendTransactionHeartbeat(transactionId));
    }

    @Override
    public LockResponse acquireLock(LockRequest lockRequest)
        throws TException {
        log.debug("[DEBUG-INFO] acquireLock");
        return runWithHandle(() -> delegate.lock(lockRequest));
    }

    @Override
    public LockResponse checkLock(long lockId)
        throws TException {
        log.debug("[DEBUG-INFO] checkLock");
        return runWithHandle(() -> delegate.checkLock(lockId));
    }

    @Override
    public String getValidWriteIds(List<String> tableList, long currentTransactionId)
        throws TException {
        log.debug("[DEBUG-INFO] getValidWriteIds");
        return null;
        //return runWithHandle(() -> delegate.getValidWriteIds(tableList, currentTransactionId));
    }

    @Override
    public String get_config_value(String name, String defaultValue)
        throws TException {
        log.debug("[DEBUG-INFO] get_config_value");
        return runWithHandle(() -> delegate.getConfigValue(name, defaultValue));
    }

    @Override
    public String getDelegationToken(String userName)
        throws TException {
        log.debug("[DEBUG-INFO] getDelegationToken");
        return runWithHandle(() -> delegate.getDelegationToken(userName, userName));
    }

    private <T> T runWithHandle(ThrowingSupplier<T> supplier)
        throws TException {
        try {
            T result = supplier.get();
            callback.success();
            return result;
        } catch (TException thriftException) {
            try {
                callback.failed(thriftException);
            } catch (RuntimeException callbackException) {
                callbackException.addSuppressed(thriftException);
                throw callbackException;
            }
            throw thriftException;
        }
    }

    private void runWithHandle(ThrowingRunnable runnable)
        throws TException {
        try {
            runnable.run();
            callback.success();
        } catch (TException thriftException) {
            try {
                callback.failed(thriftException);
            } catch (RuntimeException callbackException) {
                callbackException.addSuppressed(thriftException);
                throw callbackException;
            }
            throw thriftException;
        }
    }

    private interface ThrowingSupplier<T> {

        T get()
            throws TException;
    }

    private interface ThrowingRunnable {

        void run()
            throws TException;
    }
}