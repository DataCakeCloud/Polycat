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
package io.trino.plugin.hive;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.CatalogNameModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorAccessControl;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeEventListener;
import io.trino.plugin.base.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.authentication.HdfsAuthenticationModule;
import io.trino.plugin.hive.azure.HiveAzureModule;
import io.trino.plugin.hive.gcs.HiveGcsModule;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreModule;
import io.trino.plugin.hive.procedure.HiveProcedureModule;
import io.trino.plugin.hive.rubix.RubixEnabledConfig;
import io.trino.plugin.hive.rubix.RubixModule;
import io.trino.plugin.hive.s3.HiveS3Module;
import io.trino.plugin.hive.security.HiveSecurityModule;
import io.trino.plugin.hive.security.SystemTableAwareAccessControl;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.procedure.Procedure;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static java.util.Objects.requireNonNull;

public class InternalPolyCatConnectorFactory {

    private InternalPolyCatConnectorFactory() {
    }

    public static Connector createConnector(String catalogName, Map<String, String> config, ConnectorContext context,
        Module module) {
        return createConnector(catalogName, config, context, module, Optional.empty());
    }

    public static Connector createConnector(String catalogName, Map<String, String> config, ConnectorContext context,
        Module module, Optional<HiveMetastore> metastore) {
        requireNonNull(config, "config is null");

        ClassLoader classLoader = InternalHiveConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                new CatalogNameModule(catalogName),
                new EventModule(),
                new MBeanModule(),
                new ConnectorObjectNameGeneratorModule(catalogName, "io.trino.plugin.hive", "trino.plugin.hive"),
                new JsonModule(),
                new TypeDeserializerModule(context.getTypeManager()),
                new HiveModule(),
                new HiveHdfsModule(),
                new HiveS3Module(),
                new HiveGcsModule(),
                new HiveAzureModule(),
                conditionalModule(RubixEnabledConfig.class, RubixEnabledConfig::isCacheEnabled, new RubixModule()),
                new PolyCatHiveMetastoreModule(metastore),
                new HiveSecurityModule(),
                new HdfsAuthenticationModule(),
                new HiveProcedureModule(),
                new MBeanServerModule(),
                binder -> {
                    binder.bind(NodeVersion.class)
                        .toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(VersionEmbedder.class).toInstance(context.getVersionEmbedder());
                    binder.bind(MetadataProvider.class).toInstance(context.getMetadataProvider());
                    binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                    binder.bind(PageSorter.class).toInstance(context.getPageSorter());
                },
                binder -> newSetBinder(binder, EventListener.class),
                binder -> bindSessionPropertiesProvider(binder, HiveSessionProperties.class),
                module);

            Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            TransactionalMetadataFactory metadataFactory = injector.getInstance(TransactionalMetadataFactory.class);
            HiveTransactionManager transactionManager = injector.getInstance(HiveTransactionManager.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorPageSinkProvider pageSinkProvider = injector.getInstance(ConnectorPageSinkProvider.class);
            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(
                ConnectorNodePartitioningProvider.class);
            Set<SessionPropertiesProvider> sessionPropertiesProviders = injector.getInstance(
                Key.get(new TypeLiteral<Set<SessionPropertiesProvider>>() {
                }));
            HiveTableProperties hiveTableProperties = injector.getInstance(HiveTableProperties.class);
            HiveAnalyzeProperties hiveAnalyzeProperties = injector.getInstance(HiveAnalyzeProperties.class);
            HiveMaterializedViewPropertiesProvider hiveMaterializedViewPropertiesProvider = injector.getInstance(
                HiveMaterializedViewPropertiesProvider.class);
            Set<Procedure> procedures = injector.getInstance(Key.get(new TypeLiteral<Set<Procedure>>() {
            }));
            Set<TableProcedureMetadata> tableProcedures = injector.getInstance(
                Key.get(new TypeLiteral<Set<TableProcedureMetadata>>() {
                }));
            Set<EventListener> eventListeners = injector.getInstance(Key.get(new TypeLiteral<Set<EventListener>>() {
                }))
                .stream()
                .map(listener -> new ClassLoaderSafeEventListener(listener, classLoader))
                .collect(toImmutableSet());
            Set<SystemTableProvider> systemTableProviders = injector.getInstance(
                Key.get(new TypeLiteral<Set<SystemTableProvider>>() {
                }));
            Optional<ConnectorAccessControl> hiveAccessControl = injector.getInstance(
                    Key.get(new TypeLiteral<Optional<ConnectorAccessControl>>() {
                    }))
                .map(accessControl -> new SystemTableAwareAccessControl(accessControl, systemTableProviders))
                .map(accessControl -> new ClassLoaderSafeConnectorAccessControl(accessControl, classLoader));

            return new HiveConnector(
                lifeCycleManager,
                metadataFactory,
                transactionManager,
                new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
                new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                procedures,
                tableProcedures,
                eventListeners,
                sessionPropertiesProviders,
                HiveSchemaProperties.SCHEMA_PROPERTIES,
                hiveTableProperties.getTableProperties(),
                hiveAnalyzeProperties.getAnalyzeProperties(),
                hiveMaterializedViewPropertiesProvider.getMaterializedViewProperties(),
                hiveAccessControl,
                classLoader);
        }
    }

    public static void bindSessionPropertiesProvider(Binder binder, Class<? extends SessionPropertiesProvider> type) {
        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(type).in(Scopes.SINGLETON);
    }
}
