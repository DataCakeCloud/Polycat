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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.polycat.catalog.client.Client;
import io.polycat.catalog.client.PolyCatClientHelper;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.exception.LockException;
import io.polycat.catalog.common.exception.NoSuchObjectException;
import io.polycat.catalog.common.model.SerDeInfo;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableName;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author liangyouze
 * @date 2024/2/2
 */
public class PolyCatTableOperations extends BaseMetastoreTableOperations {

    private static final Logger LOG = LoggerFactory.getLogger(PolyCatTableOperations.class);
    private static final String POLYCAT_TABLE_LEVEL_LOCK_EVICT_MS = "iceberg.polycat.table-level-lock-evict-ms";
    private static final long POLYCAT_TABLE_LEVEL_LOCK_EVICT_MS_DEFAULT = TimeUnit.MINUTES.toMillis(10);
    ;
    private static final String POLYCAT_ICEBERG_METADATA_REFRESH_MAX_RETRIES = "iceberg.polycat.metadata-refresh-max-retries";
    private static final int POLYCAT_ICEBERG_METADATA_REFRESH_MAX_RETRIES_DEFAULT = 2;

    private static final String HIVE_META_TABLE_STORAGE = "storage_handler";

    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final Configuration conf;

    private final int metadataRefreshMaxRetries;

    private final FileIO fileIO;

    private final Client polycatClient;

    protected PolyCatTableOperations(String catalogName, String databaseName, String tableName, Configuration conf, FileIO fileIO, Client polycatClient) {
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.conf = conf;
        this.fileIO = fileIO;
        this.polycatClient = polycatClient;
        this.metadataRefreshMaxRetries =
                conf.getInt(
                        POLYCAT_ICEBERG_METADATA_REFRESH_MAX_RETRIES,
                        POLYCAT_ICEBERG_METADATA_REFRESH_MAX_RETRIES_DEFAULT);
    }

    @Override
    protected String tableName() {
        return catalogName + "." + databaseName + "." + tableName;
    }

    @Override
    public FileIO io() {
        return this.fileIO;
    }

    @Override
    protected void doRefresh() {
        String metadataLocation = null;
        try {
            Table table = PolyCatClientHelper.getTable(polycatClient, catalogName, databaseName, this.tableName);
            validateTableIsIceberg(table, tableName());
            metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
        } catch (NoSuchObjectException e) {
            if (currentMetadataLocation() != null) {
                throw new NoSuchTableException("No such table: %s.%s.%s", catalogName, databaseName, tableName);
            }
        }
        refreshFromMetadataLocation(metadataLocation, metadataRefreshMaxRetries);
    }

    @Override
    protected void doCommit(TableMetadata base, TableMetadata metadata) {
        String newMetadataLocation = writeNewMetadataIfRequired(base == null, metadata);
        CommitStatus commitStatus = CommitStatus.FAILURE;
        PolyCatLock lock = new PolyCatLock(conf, catalogName, databaseName, tableName, tableName(), polycatClient);
        try {
            lock.lock();
            Table tbl = loadTable();
            checkMetadataLocation(tbl, base);
            final Map<String, String> properties = prepareProperties(tbl, base, metadata, newMetadataLocation);
            final StorageDescriptor sd = storageDescriptor(metadata, hiveEngineEnabled(metadata, conf));
            lock.ensureActive();
            try {
                persistTable(tbl, properties, sd);
                lock.ensureActive();
                commitStatus = CommitStatus.SUCCESS;
            } catch (LockException e) {
                commitStatus = CommitStatus.UNKNOWN;
                final LockException lockException = new LockException(e, "Failed to heartbeat for lock while "
                        + "committing changes. This can lead to a concurrent commit attempt be able to overwrite this commit. "
                        + "Please check the commit history. If you are running into this issue, try reducing "
                        + "iceberg.polycat.lock-heartbeat-interval-ms.");
                throw new CommitStateUnknownException(lockException);
            } catch (io.polycat.catalog.common.exception.AlreadyExistsException e) {
                throw new AlreadyExistsException(e, "Table already exists: %s.%s.%s", catalogName, databaseName, tableName);
            } catch (CommitFailedException | CommitStateUnknownException e) {
                throw e;
            } catch (Throwable e) {
                LOG.error(
                        "Cannot tell if commit to {}.{}.{} succeeded, attempting to reconnect and check.",
                        catalogName,
                        databaseName,
                        tableName,
                        e);
                commitStatus = checkCommitStatus(newMetadataLocation, metadata);
                switch (commitStatus) {
                    case SUCCESS:
                        break;
                    case FAILURE:
                        throw e;
                    case UNKNOWN:
                        throw new CommitStateUnknownException(e);
                }
            }
        } catch (LockException e) {
            throw new CommitFailedException(e, e.getMessage());
        } catch (CatalogException e) {
            throw new RuntimeException(
                    String.format("Metastore operation failed for %s.%s.%s", catalogName, databaseName, tableName), e);
        }  finally {
            cleanupMetadataAndUnlock(commitStatus, newMetadataLocation, lock);
        }
        LOG.info(
                "Committed to table {} with the new metadata location {}", tableName(), newMetadataLocation);
    }

    private Map<String, String> prepareProperties(Table table, TableMetadata base, TableMetadata metadata, String newMetadataLocation) {
        Map<String, String> properties =
                table != null ? table.getParameters() : Maps.newHashMap();
        // get Iceberg props that have been removed
        Set<String> removedProps = Collections.emptySet();
        if (base != null) {
            removedProps =
                    base.properties().keySet().stream()
                            .filter(key -> !metadata.properties().containsKey(key))
                            .collect(Collectors.toSet());
        }

        Map<String, String> summary =
                Optional.ofNullable(metadata.currentSnapshot())
                        .map(Snapshot::summary)
                        .orElseGet(ImmutableMap::of);
        final Map<String, String> finalParameters = buildFinalParameters(newMetadataLocation, properties, metadata, removedProps, hiveEngineEnabled(metadata, conf), summary);
        if (!conf.getBoolean(ConfigProperties.KEEP_HIVE_STATS, false)) {
            finalParameters.remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
        }
        finalParameters.put("EXTERNAL", "TRUE");
        return finalParameters;
    }

    private void checkMetadataLocation(Table table, TableMetadata base) {
        String metadataLocation = table != null ? table.getParameters().get(METADATA_LOCATION_PROP) : null;
        String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
        if (base == null && metadataLocation != null) {
            throw new AlreadyExistsException("Table already exists: %s.%s.%s", catalogName, databaseName, this.tableName);
        }
        if (!Objects.equals(baseMetadataLocation, metadataLocation)) {
            throw new CommitFailedException(
                    "Base metadata location '%s' is not same as the current table metadata location '%s' for %s.%s.%s",
                    baseMetadataLocation, metadataLocation, catalogName, databaseName, this.tableName);
        }
    }

    void persistTable(Table tbl, Map<String, String> parameters, StorageDescriptor sd) {
        final TableName tableNameObj = new TableName(polycatClient.getProjectId(), catalogName, databaseName, tableName);
        if (tbl != null) {
            LOG.debug("Committing existing table: {}", tableName());
            if (StringUtils.isNotEmpty(parameters.get("comment"))) {
                tbl.setDescription(parameters.get("comment"));
            }
            PolyCatClientHelper.alterTable(polycatClient, tbl, parameters, sd);
        } else {
            LOG.debug("Committing new table: {}", tableName());
            PolyCatClientHelper.createTable(polycatClient, tableNameObj, parameters, sd, new ArrayList<>(), parameters.get("comment"));
        }
    }

    private Map<String, String> buildFinalParameters(
            String newMetadataLocation,
            Map<String, String> parameters,
            TableMetadata metadata,
            Set<String> obsoleteProps,
            boolean hiveEngineEnabled,
            Map<String, String> summary) {
        if (metadata.uuid() != null) {
            parameters.put(TableProperties.UUID, metadata.uuid());
        }

        // remove any props from HMS that are no longer present in Iceberg table props
        obsoleteProps.forEach(parameters::remove);

        parameters.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
        parameters.put(METADATA_LOCATION_PROP, newMetadataLocation);

        if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
            parameters.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
        }

        // If needed set the 'storage_handler' property to enable query from Hive
        if (hiveEngineEnabled) {
            parameters.put(
                    HIVE_META_TABLE_STORAGE,
                    "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");
        } else {
            parameters.remove(HIVE_META_TABLE_STORAGE);
        }

        // Set the basic statistics
        if (summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP) != null) {
            parameters.put(StatsSetupConst.NUM_FILES, summary.get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
        }
        if (summary.get(SnapshotSummary.TOTAL_RECORDS_PROP) != null) {
            parameters.put(StatsSetupConst.ROW_COUNT, summary.get(SnapshotSummary.TOTAL_RECORDS_PROP));
        }
        if (summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP) != null) {
            parameters.put(StatsSetupConst.TOTAL_SIZE, summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP));
        }

        setSnapshotStats(metadata, parameters);
        setSchema(metadata, parameters);
        setPartitionSpec(metadata, parameters);
        setSortOrder(metadata, parameters);
        return parameters;
    }

    void setSnapshotStats(TableMetadata metadata, Map<String, String> parameters) {
        parameters.remove(TableProperties.CURRENT_SNAPSHOT_ID);
        parameters.remove(TableProperties.CURRENT_SNAPSHOT_TIMESTAMP);
        parameters.remove(TableProperties.CURRENT_SNAPSHOT_SUMMARY);

        Snapshot currentSnapshot = metadata.currentSnapshot();
        if (currentSnapshot != null) {
            parameters.put(
                    TableProperties.CURRENT_SNAPSHOT_ID, String.valueOf(currentSnapshot.snapshotId()));
            parameters.put(
                    TableProperties.CURRENT_SNAPSHOT_TIMESTAMP,
                    String.valueOf(currentSnapshot.timestampMillis()));
            setSnapshotSummary(parameters, currentSnapshot);
        }

        parameters.put(TableProperties.SNAPSHOT_COUNT, String.valueOf(metadata.snapshots().size()));
    }

    void setSnapshotSummary(Map<String, String> parameters, Snapshot currentSnapshot) {
        try {
            String summary = JsonUtil.mapper().writeValueAsString(currentSnapshot.summary());
            parameters.put(TableProperties.CURRENT_SNAPSHOT_SUMMARY, summary);
        } catch (JsonProcessingException e) {
            LOG.warn(
                    "Failed to convert current snapshot({}) summary to a json string",
                    currentSnapshot.snapshotId(),
                    e);
        }
    }

    void setSchema(TableMetadata metadata, Map<String, String> parameters) {
        parameters.remove(TableProperties.CURRENT_SCHEMA);
        if (metadata.schema() != null) {
            String schema = SchemaParser.toJson(metadata.schema());
            parameters.put(TableProperties.CURRENT_SCHEMA, schema);
        }
    }

    void setPartitionSpec(TableMetadata metadata, Map<String, String> parameters) {
        parameters.remove(TableProperties.DEFAULT_PARTITION_SPEC);
        if (metadata.spec() != null && metadata.spec().isPartitioned()) {
            String spec = PartitionSpecParser.toJson(metadata.spec());
            parameters.put(TableProperties.DEFAULT_PARTITION_SPEC, spec);
        }
    }

    void setSortOrder(TableMetadata metadata, Map<String, String> parameters) {
        parameters.remove(TableProperties.DEFAULT_SORT_ORDER);
        if (metadata.sortOrder() != null
                && metadata.sortOrder().isSorted()) {
            String sortOrder = SortOrderParser.toJson(metadata.sortOrder());
            parameters.put(TableProperties.DEFAULT_SORT_ORDER, sortOrder);
        }
    }

    Table loadTable() {
        try {
            return PolyCatClientHelper.getTable(polycatClient, catalogName, databaseName, tableName);
        } catch (NoSuchObjectException e) {
            LOG.debug(String.format("Failed to get table %s.%s.%s", catalogName, databaseName, tableName), e);
            return null;
        }
    }

    static void validateTableIsIceberg(Table table, String fullName) {
        String tableType = table.getParameters().get(TABLE_TYPE_PROP);
        NoSuchIcebergTableException.check(
                tableType != null && tableType.equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE),
                "Not an iceberg table: %s (type=%s)",
                fullName,
                tableType);
    }

    private StorageDescriptor storageDescriptor(TableMetadata metadata, boolean hiveEngineEnabled) {

        final StorageDescriptor storageDescriptor = new StorageDescriptor();
        storageDescriptor.setColumns(PolyCatObjectUtils.convert(metadata.schema()));
        storageDescriptor.setLocation(metadata.location());
        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setParameters(Maps.newHashMap());
        if (hiveEngineEnabled) {
            storageDescriptor.setInputFormat("org.apache.iceberg.mr.hive.HiveIcebergInputFormat");
            storageDescriptor.setOutputFormat("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
            serDeInfo.setSerializationLibrary("org.apache.iceberg.mr.hive.HiveIcebergSerDe");
        } else {
            storageDescriptor.setOutputFormat("org.apache.hadoop.mapred.FileOutputFormat");
            storageDescriptor.setInputFormat("org.apache.hadoop.mapred.FileInputFormat");
            serDeInfo.setSerializationLibrary("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        }
        storageDescriptor.setSerdeInfo(serDeInfo);
        return storageDescriptor;
    }

    protected String writeNewMetadataIfRequired(boolean newTable, TableMetadata metadata) {
        return newTable && metadata.metadataFileLocation() != null
                ? metadata.metadataFileLocation()
                : writeNewMetadata(metadata, currentVersion() + 1);
    }

    /**
     * Returns if the hive engine related values should be enabled on the table, or not.
     *
     * <p>The decision is made like this:
     *
     * <ol>
     *   <li>Table property value {@link TableProperties#ENGINE_HIVE_ENABLED}
     *   <li>If the table property is not set then check the hive-site.xml property value {@link
     *       ConfigProperties#ENGINE_HIVE_ENABLED}
     *   <li>If none of the above is enabled then use the default value {@link
     *       TableProperties#ENGINE_HIVE_ENABLED_DEFAULT}
     * </ol>
     *
     * @param metadata Table metadata to use
     * @param conf     The hive configuration to use
     * @return if the hive engine related values should be enabled or not
     */
    private static boolean hiveEngineEnabled(TableMetadata metadata, Configuration conf) {
        if (metadata.properties().get(TableProperties.ENGINE_HIVE_ENABLED) != null) {
            // We know that the property is set, so default value will not be used,
            return metadata.propertyAsBoolean(TableProperties.ENGINE_HIVE_ENABLED, false);
        }

        return conf.getBoolean(
                ConfigProperties.ENGINE_HIVE_ENABLED, TableProperties.ENGINE_HIVE_ENABLED_DEFAULT);
    }

    private void cleanupMetadataAndUnlock(
            CommitStatus commitStatus, String metadataLocation, PolyCatLock lock) {
        try {
            if (commitStatus == CommitStatus.FAILURE) {
                // If we are sure the commit failed, clean up the uncommitted metadata file
                io().deleteFile(metadataLocation);
            }
        } catch (RuntimeException e) {
            LOG.error("Failed to cleanup metadata file at {}", metadataLocation, e);
        } finally {
            lock.unlock();
        }
    }
}
