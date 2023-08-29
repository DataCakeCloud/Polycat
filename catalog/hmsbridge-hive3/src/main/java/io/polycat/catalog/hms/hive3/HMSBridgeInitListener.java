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
package io.polycat.catalog.hms.hive3;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import io.polycat.catalog.common.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreInitContext;
import org.apache.hadoop.hive.metastore.MetaStoreInitListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

public class HMSBridgeInitListener extends MetaStoreInitListener {

    private static final Logger LOG = Logger.getLogger(HMSBridgeInitListener.class);

    public HMSBridgeInitListener(Configuration config) {
        super(config);
    }

    /**
     * User configurable Metastore vars
     */
    private static final MetastoreConf.ConfVars[] CONF_VARS = {
        MetastoreConf.ConfVars.TRY_DIRECT_SQL,
        MetastoreConf.ConfVars.TRY_DIRECT_SQL_DDL,
        MetastoreConf.ConfVars.CLIENT_SOCKET_TIMEOUT,
        MetastoreConf.ConfVars.PARTITION_NAME_WHITELIST_PATTERN,
        MetastoreConf.ConfVars.CAPABILITY_CHECK,
        MetastoreConf.ConfVars.DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES
    };

    /**
     * The MetastoreConf ConfVars needs to be added to the User configurable Metastore vars
     */
    private static final MetastoreConf.ConfVars[] CONFIGURABLE_CONF_VARS = {
        MetastoreConf.ConfVars.CATALOG_DEFAULT
    };

    @SuppressWarnings("checkstyle:RegexpSingleline")
    @Override
    public void onInit(MetaStoreInitContext context) throws MetaException {
        try {
            LOG.info("start invoke metaConfVars in MetaConf");
            Field metaConfVars = MetastoreConf.class.getDeclaredField("metaConfVars");
            metaConfVars.setAccessible(true);
            Field modifiers1 = Field.class.getDeclaredField("modifiers");
            modifiers1.setAccessible(true);
            modifiers1.setInt(metaConfVars, metaConfVars.getModifiers() & ~Modifier.FINAL);

            Field metaConfs = MetastoreConf.class.getDeclaredField("metaConfs");
            metaConfs.setAccessible(true);
            Field field1 = Field.class.getDeclaredField("modifiers");
            field1.setAccessible(true);
            field1.set(metaConfs, metaConfs.getModifiers() & ~Modifier.FINAL);
            loadConfVars(metaConfVars, metaConfs, field1);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            LOG.error("failed to init HMSBridgeInitListener", e);
            throw new MetaException(e.getMessage());
        }
    }

    private static void loadConfVars(Field metaConfVars, Field metaConfs, Field modifiersField) throws IllegalAccessException {
        int confVarsSize = CONF_VARS.length;
        int configurableConfVarsSize = CONFIGURABLE_CONF_VARS.length;
        int dashConfVarsSize = ConfVars.values().length;
        MetastoreConf.ConfVars[] metaConfVarsInvoke = new MetastoreConf.ConfVars[confVarsSize + configurableConfVarsSize + dashConfVarsSize];
        System.arraycopy(CONF_VARS, 0, metaConfVarsInvoke, 0, CONF_VARS.length);
        Map<String, MetastoreConf.ConfVars> map = new HashMap<>();

        // load CONFIGURABLE_CONF_VARS
        for (int i = 0; i < configurableConfVarsSize; i++) {
            MetastoreConf.ConfVars confVars = CONFIGURABLE_CONF_VARS[i];
            metaConfVarsInvoke[confVarsSize + i] = confVars;
            map.put(confVars.getVarname(), confVars);
        }

        // load ConfVars
        for (int j = 0; j < dashConfVarsSize; j++) {
            ConfVars dashConfVar = ConfVars.values()[j];
            addEnum(dashConfVar);
            MetastoreConf.ConfVars confVars = MetastoreConf.ConfVars.valueOf(dashConfVar.name());
            metaConfVarsInvoke[confVarsSize + configurableConfVarsSize + j] = confVars;
            map.put(dashConfVar.getVarname(), confVars);
        }
        metaConfs.set(modifiersField, map);
        metaConfVars.set(metaConfVars, metaConfVarsInvoke);
    }

    /**
     * Dynamically Adding Customized Enumeration Values to MetastoreConf.ConfVars[]
     *
     * @param dashConfVar Custom Enumeration Value
     */
    private static void addEnum(ConfVars dashConfVar) {
        DynamicEnumUtils.addEnum(MetastoreConf.ConfVars.class, dashConfVar.name(),
                new Class[]{String.class, String.class, String.class, String.class},
                new Object[]{dashConfVar.getVarname(), dashConfVar.getHiveName(),
                        dashConfVar.getDefaultVal(), dashConfVar.getDescription()});
    }
}
