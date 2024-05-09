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

import io.polycat.catalog.common.exception.CatalogException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreInitContext;
import org.apache.hadoop.hive.metastore.MetaStoreInitListener;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

public class CatalogBridgeInitListener extends MetaStoreInitListener {

    public CatalogBridgeInitListener(Configuration config) {
        super(config);
    }

    @Override
    public void onInit(MetaStoreInitContext context) throws MetaException {
        // catalog.hive.catalog.names.mapping=hive:ue1|sg1,hive1:sg2|sg3
        String catName = MetastoreConf.getVar(getConf(), MetastoreConf.ConfVars.CATALOG_DEFAULT);
        if (catName == null || catName.isEmpty()) {
            catName = Warehouse.DEFAULT_CATALOG_NAME;
        }
        CatalogStore catalogStore = null;
        System.out.println("onInit startup, create catalogName: " + catName);
        if (catName.equals(Warehouse.DEFAULT_CATALOG_NAME)) {
            return;
        }
        try {
            catalogStore = new CatalogStore(getConf());
            catalogStore.getCatalog(catName);
        } catch (NoSuchObjectException e ) {
            final String location = MetastoreConf.getVar(getConf(), MetastoreConf.ConfVars.WAREHOUSE);
            final Catalog catalog = new Catalog(catName, location);
            catalog.setDescription(Warehouse.DEFAULT_CATALOG_COMMENT);
            catalogStore.createCatalog(catalog);
        } catch (MetaException e) {
            throw new CatalogException(e);
        }
    }
}
