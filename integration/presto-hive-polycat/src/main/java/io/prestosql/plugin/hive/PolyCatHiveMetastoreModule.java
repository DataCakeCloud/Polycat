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

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.MetastoreConfig;
import io.prestosql.plugin.hive.metastore.alluxio.AlluxioMetastoreModule;
import io.prestosql.plugin.hive.metastore.cache.CachingHiveMetastoreModule;
import io.prestosql.plugin.hive.metastore.cache.ForCachingHiveMetastore;
import io.prestosql.plugin.hive.metastore.file.FileMetastoreModule;
import io.prestosql.plugin.hive.metastore.glue.GlueMetastoreModule;

import java.util.Optional;

import static io.airlift.configuration.ConditionalModule.installModuleIf;

public class PolyCatHiveMetastoreModule
    extends AbstractConfigurationAwareModule {

    private final Optional<HiveMetastore> metastore;

    public PolyCatHiveMetastoreModule(Optional<HiveMetastore> metastore) {
        this.metastore = metastore;
    }

    @Override
    protected void setup(Binder binder) {
        if (metastore.isPresent()) {
            binder.bind(HiveMetastore.class).annotatedWith(ForCachingHiveMetastore.class).toInstance(metastore.get());
            install(new CachingHiveMetastoreModule());
        } else {
            //插件模块涉及到的类初始化注入点
            bindMetastoreModule("thrift", new PolyCatThriftMetastoreModule());
            bindMetastoreModule("file", new FileMetastoreModule());
            bindMetastoreModule("glue", new GlueMetastoreModule());
            bindMetastoreModule("alluxio", new AlluxioMetastoreModule());
        }
    }

    private void bindMetastoreModule(String name, Module module) {
        install(installModuleIf(
            MetastoreConfig.class,
            metastore -> name.equalsIgnoreCase(metastore.getMetastoreType()),
            module));
    }
}
