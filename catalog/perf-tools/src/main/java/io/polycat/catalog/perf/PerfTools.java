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
package io.polycat.catalog.perf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.plugin.request.input.ColumnInput;
import io.polycat.catalog.common.plugin.request.input.StorageInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.store.VersionStore;
import io.polycat.catalog.store.VersionStoreFactory;
import io.polycat.catalog.store.common.StoreConvertor;
import io.polycat.catalog.store.fdb.record.RecordStoreHelper;
import io.polycat.catalog.store.fdb.record.impl.TableStoreImpl;
import io.polycat.catalog.store.protos.CatalogIdent;
import io.polycat.catalog.store.protos.CatalogName;
import io.polycat.catalog.store.protos.CatalogRecord;
import io.polycat.catalog.store.protos.Column;
import io.polycat.catalog.store.protos.DataFile;
import io.polycat.catalog.store.protos.DataType;
import io.polycat.catalog.store.protos.DatabaseIdent;
import io.polycat.catalog.store.protos.DatabaseName;
import io.polycat.catalog.store.protos.DatabaseRecord;
import io.polycat.catalog.store.protos.FileStats;
import io.polycat.catalog.store.protos.Partition;
import io.polycat.catalog.store.protos.PartitionType;
import io.polycat.catalog.store.protos.TableIdent;
import io.polycat.catalog.store.protos.TableName;
import io.polycat.catalog.store.protos.TableRecord;
import io.polycat.catalog.store.protos.TableReference;
import io.polycat.catalog.store.protos.TableSchemaHistory;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.subspace.Subspace;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerfTools {
    static VersionStore versionStore;
    static TableStoreImpl tableStoreImpl;

    static String projectIdTest = "project1";
    static String catalogNameTest = "c1";
    static String databaseNameTest = "db1";
    static String tableNameTest = "t1";
    private static final String USERID = "wukong";

    public static void setUp() throws NoSuchFieldException, IllegalAccessException {
        RecordStoreHelper.createAllRecordStores();

        versionStore = VersionStoreFactory.getVersionStoreImpl();
        tableStoreImpl = new TableStoreImpl(RecordStoreHelper.getFdbDatabase());
    }

    public static Partition buildPartitionParams(TableSchemaHistory tableSchemaHistory, String partitionPath) throws MetaStoreException {
        FileStats file1Stats = FileStats.newBuilder()
            .addMinValue(ByteString.copyFromUtf8("abc"))
            .addMaxValue(ByteString.copyFromUtf8("hbc"))
            .addMinValue(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(1).array()))
            .addMaxValue(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(50).array()))
            .build();
        FileStats file2Stats = FileStats.newBuilder()
            .addMinValue(ByteString.copyFromUtf8("ebc"))
            .addMaxValue(ByteString.copyFromUtf8("obc"))
            .addMinValue(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(40).array()))
            .addMaxValue(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(70).array()))
            .build();
        DataFile file1 = DataFile.newBuilder().setFileName("file1.carbondata").setOffset(10).setLength(30)
            .setRowCount(2).build();
        DataFile file2 = DataFile.newBuilder().setFileName("file2.carbondata").setOffset(11).setLength(40)
            .setRowCount(2).build();
        Partition partition = Partition.newBuilder()
            .setName(partitionPath)
            .setType(PartitionType.INTERNAL)
            .setPartitionId(RecordStoreHelper.generateId())
            .setPartitionFolderUrl(partitionPath)
            .setSchemaVersion(tableSchemaHistory.getVersion())
            .setFileFormat("carbondata")
            .addFile(file1)
            .addFile(file2)
            .addStats(file1Stats)
            .addStats(file2Stats)
            .buildPartial();
        return partition;
    }

    public static void addPartitions(String[] args) throws NoSuchFieldException, IllegalAccessException {
        if (args.length < 2) {
            System.out.println("wrong Parameters of CMD create-partitions");
            return;
        }

        int batchSize = Integer.parseInt(args[1]);
        int batchCount = Integer.parseInt(args[2]);

        System.out.println("batchSize: " + batchSize + ", batchCount: " + batchCount);

        TableName mainTableName = StoreConvertor.tableName(projectIdTest, catalogNameTest,
            databaseNameTest, tableNameTest);
        TableRecord tableRecord = versionStore.getTableByName(mainTableName);
        TableIdent tableIdent = StoreConvertor.tableIdent(tableRecord.getProjectId(),
            tableRecord.getCatalogId(), tableRecord.getDatabaseId(), tableRecord.getTableId());
        TableSchemaHistory tableSchemaHistory = versionStore.getLatestTableSchemaWithVersion(tableIdent);

        List<Partition> partitions = new ArrayList<>();

        for (int batch = 0; batch < batchSize; batch++) {
            partitions.clear();
            for (int i = 0; i < batchCount; i++) {
                Partition partition = buildPartitionParams(tableSchemaHistory, "p0=" + (batch * batchCount + i) + "/p1=beijing");
                partitions.add(partition);
            }

            long start = System.currentTimeMillis();
            versionStore.addPartitions(mainTableName, partitions, false);
            long end = System.currentTimeMillis();
            long time = end - start;
            System.out.println("batch partition added, " + (batch+1)*batchCount + ", time used: " + time + " ms");
        }
    }

    public static void addHMSPartitions(String[] args) throws NoSuchFieldException, IllegalAccessException {
        if (args.length < 2) {
            System.out.println("wrong Parameters of CMD create-partitions");
            return;
        }

        int tableCount = Integer.parseInt(args[1]);
        int batchSize = Integer.parseInt(args[2]);
        int batchCount = Integer.parseInt(args[3]);

        System.out.println("tableCount: " + tableCount + ", batchSize: " + batchSize + ", batchCount: " + batchCount);

        for (int tableIdx = 0; tableIdx < tableCount; tableIdx++) {
            TableName mainTableName = StoreConvertor.tableName(projectIdTest, catalogNameTest,
                databaseNameTest, "t" + tableIdx);
            TableRecord tableRecord = versionStore.getTableByName(mainTableName);
            TableIdent tableIdent = StoreConvertor.tableIdent(tableRecord.getProjectId(),
                tableRecord.getCatalogId(), tableRecord.getDatabaseId(), tableRecord.getTableId());
            TableSchemaHistory tableSchemaHistory = versionStore.getLatestTableSchemaWithVersion(tableIdent);

            List<Partition> partitions = new ArrayList<>();

            for (int batch = 0; batch < batchSize; batch++) {
                partitions.clear();
                for (int i = 0; i < batchCount; i++) {
                    Partition partition = buildPartitionParams(tableSchemaHistory, "p0=" + (batch * batchCount + i) + "/p1=beijing");
                    partitions.add(partition);
                }

                long start = System.currentTimeMillis();
                versionStore.addPartitions(mainTableName, partitions, false);
                long end = System.currentTimeMillis();
                long time = end - start;
                System.out.println("For Table: t" + tableIdx + ", batch partition added, " + (batch+1)*batchCount + ", time used: " + time + " ms");
            }
        }
    }

    private static CatalogRecord createCatalogPrepareTest(String projectId, String catalogName) {

        CatalogRecord newCatalog = CatalogRecord.newBuilder()
            .setProjectId(projectId)
            .setName(catalogName)
            .setUserId(USERID)
            .buildPartial();

        CatalogRecord catalogRecord = versionStore.createCatalog(newCatalog);
        return catalogRecord;
    }

    private void createBaseMetadata() {
        CatalogRecord mainCatalog = createCatalogPrepareTest(projectIdTest, catalogNameTest);

        CatalogName mainCatalogName = StoreConvertor.catalogName(mainCatalog.getProjectId(), mainCatalog.getName());
        CatalogIdent mainCatalogIdent = StoreConvertor.catalogIdent(mainCatalog.getProjectId(), mainCatalog.getCatalogId());
        DatabaseName mainDatabaseName = StoreConvertor.databaseName(mainCatalog.getProjectId(),
            mainCatalogName.getCatalogName(), databaseNameTest);
        DatabaseRecord mainDatabase = createDatabase(mainDatabaseName);

        DatabaseIdent mainDatabaseIdent = StoreConvertor.databaseIdent(mainCatalogIdent, mainDatabase.getDatabaseId());
        TableInput tableInput = getTableDTO(mainDatabaseIdent.getProjectId(), mainDatabaseIdent.getCatalogId(),
            mainDatabaseIdent.getDatabaseId(), tableNameTest, 3);
        TableReference tableReference = versionStore.createTable(mainDatabaseName, tableInput);
    }


    public static List<ColumnInput> getColumnDTO(int columnNum) {
        List<ColumnInput> columnInputs = new ArrayList<>(columnNum);

        for (int i=0; i<columnNum; i++) {
            ColumnInput columnInput = new ColumnInput();
            columnInput.setColumnName("c" + i);
            if (i % 2 == 0) {
                columnInput.setDataType(DataType.INT.name());
            } else {
                columnInput.setDataType(DataType.STRING.name());
            }
            columnInputs.add(columnInput);
        }

        return columnInputs;
    }

    public static List<ColumnInput> getPartitionsDTO() {
        List<ColumnInput> columnInputs = new ArrayList<>(2);

        ColumnInput columnInput;

        columnInput = new ColumnInput();
        columnInput.setColumnName("p0");
        columnInput.setDataType(DataType.INT.name());
        columnInputs.add(columnInput);

        columnInput = new ColumnInput();
        columnInput.setColumnName("p1");
        columnInput.setDataType(DataType.STRING.name());
        columnInputs.add(columnInput);

//        columnInput = new ColumnInput();
//        columnInput.setColumnName("p2");
//        columnInput.setDataType(DataType.INT.name());
//        columnInputs.add(columnInput);
//
//        columnInput = new ColumnInput();
//        columnInput.setColumnName("p3");
//        columnInput.setDataType(DataType.STRING.name());
//        columnInputs.add(columnInput);

        return columnInputs;
    }

    public static TableInput getTableDTO(String projectId, String catalogId, String databaseId, String tableName, int colNum) {
        TableInput tableInput = new TableInput();
        StorageInput storageInput = new StorageInput();
        tableInput.setStorageInput(storageInput);
        tableInput.setName(tableName);

        tableInput.setColumns(getColumnDTO(colNum));
        tableInput.setPartitions(getPartitionsDTO());

        tableInput.setUserId(USERID);

        return tableInput;
    }

    public static DatabaseRecord createDatabase(DatabaseName databaseName) {
        DatabaseRecord newDatabase = versionStore.getDatabaseByName(databaseName);
        if (newDatabase != null) {
            versionStore.dropDatabaseByName(databaseName);
        }

        newDatabase = DatabaseRecord.newBuilder()
            .setProjectId(databaseName.getProjectId())
            .setCatalogName(databaseName.getCatalogName())
            .setDatabaseId(RecordStoreHelper.generateId())
            .setName(databaseName.getDatabaseName())
            .setLocation("./target/database")
            .setCreateTime(System.currentTimeMillis())
            .setUserId(USERID)
            .buildPartial();
        return versionStore.createDatabase(databaseName, newDatabase);
    }

    private static void createTable(String[] args) {
        CatalogRecord mainCatalog = createCatalogPrepareTest(projectIdTest, catalogNameTest);

        CatalogName mainCatalogName = StoreConvertor.catalogName(mainCatalog.getProjectId(), mainCatalog.getName());
        CatalogIdent mainCatalogIdent = StoreConvertor.catalogIdent(mainCatalog.getProjectId(), mainCatalog.getCatalogId());
        DatabaseName mainDatabaseName = StoreConvertor.databaseName(mainCatalog.getProjectId(),
            mainCatalogName.getCatalogName(), databaseNameTest);
        DatabaseRecord mainDatabase = createDatabase(mainDatabaseName);

        DatabaseIdent mainDatabaseIdent = StoreConvertor.databaseIdent(mainCatalogIdent, mainDatabase.getDatabaseId());
        TableInput tableInput = getTableDTO(mainDatabaseIdent.getProjectId(), mainDatabaseIdent.getCatalogId(),
            mainDatabaseIdent.getDatabaseId(), tableNameTest, 3);
        TableReference tableReference = versionStore.createTable(mainDatabaseName, tableInput);
    }

    /**
     * 1. Create 1 Catalog(c1)
     * 2. Create 1 Database(db1)
     * 3. Create n Tables(t0~n-1) with m columns (4 partitions key, p0~3)
     *
     * @param args
     */
    private static void createHMSTables(String[] args) {
        if (args.length < 2) {
            System.out.println("wrong Parameters of CMD create-hms-tables");
            return;
        }

        int tableCount = Integer.parseInt(args[1]);
        int columnCount = Integer.parseInt(args[2]);

        System.out.println("tableCount: " + tableCount + ", columnCount: " + columnCount);

        // 1.
        CatalogRecord mainCatalog = createCatalogPrepareTest(projectIdTest, catalogNameTest);

        // 2.
        CatalogName mainCatalogName = StoreConvertor.catalogName(mainCatalog.getProjectId(), mainCatalog.getName());
        CatalogIdent mainCatalogIdent = StoreConvertor.catalogIdent(mainCatalog.getProjectId(), mainCatalog.getCatalogId());
        DatabaseName mainDatabaseName = StoreConvertor.databaseName(mainCatalog.getProjectId(),
            mainCatalogName.getCatalogName(), databaseNameTest);
        DatabaseRecord mainDatabase = createDatabase(mainDatabaseName);

        // 3.
        DatabaseIdent mainDatabaseIdent = StoreConvertor.databaseIdent(mainCatalogIdent, mainDatabase.getDatabaseId());
        for (int i=0; i<tableCount; i++) {
            if (i > 0 && i % 100 == 0) {
                System.out.println(i + " tables created.");
            }

            TableInput tableInput = getTableDTO(mainDatabaseIdent.getProjectId(), mainDatabaseIdent.getCatalogId(),
                mainDatabaseIdent.getDatabaseId(), "t" + i, columnCount);
            TableReference tableReference = versionStore.createTable(mainDatabaseName, tableInput);
        }
        System.out.println(tableCount + " tables created.");
    }

    public static void clearFDB() {
        FDBDatabase fdb = RecordStoreHelper.getFdbDatabase();
        try (FDBRecordContext context = fdb.openContext()) {
            Transaction tx = RecordStoreHelper.getTransaction(context);
            final byte[] st = new Subspace(new byte[]{(byte) 0x00}).getKey();
            final byte[] en = new Subspace(new byte[]{(byte) 0xFF}).getKey();
            tx.clear(st, en);
            context.commit();
        } catch (MetaStoreException e) {
            e.printStackTrace();
        }
    }

    private static void clearDb(String[] args) {
        clearFDB();
        System.out.println("clearFDB() End.");
    }

    private static void usage() {
        String[] help = {
            "perf-tools CMD CMD-Parameter",
            "  CMD:",
            "    clear-db",
            "    create-table",
            "    create-partitions 100 1000",
            "    create-hms-tables 20000 22",
            "    create-hms-partitions 20000 1 250"};

        for (String u : help) {
            System.out.println(u);
        }
    }

    public static void main(String[] args) {
        try {
            setUp();

            if (args.length < 1) {
                usage();
                return;
            }

            switch (args[0]) {
                case "create-partitions":
                    addPartitions(args);
                    break;
                case "create-table":
                    createTable(args);
                    break;
                case "create-hms-tables":
                    createHMSTables(args);
                    break;
                case "create-hms-partitions":
                    addHMSPartitions(args);
                    break;
                case "clear-db":
                    clearDb(args);
                    break;
                default:
                    System.out.println("wrong CMD.");
                    break;
            }
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
