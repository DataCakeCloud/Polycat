#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


cat << EOF > ./conf/hive-site.xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>hive.metastore.rawstore.impl</name>
        <value>io.polycat.catalog.hms.hive2.HMSBridgeStore</value>
    </property>
    <property>
        <name>hive.hmsbridge.defaultCatalogName</name>
        <value>${HIVE_SITE_CONF_hive_hmsbridge_defaultCatalogName}</value>
    </property>
    <property>
        <name>polycat.user.name</name>
        <value>user</value>
        <description>The default does not support modification</description>
    </property>
    <property>
        <name>polycat.user.password</name>
        <value>password</value>
        <description>The default does not support modification</description>
    </property>
    <property>
        <name>polycat.user.project</name>
        <value>${HIVE_SITE_CONF_polycat_user_project}</value>
    </property>
    <property>
        <name>polycat.user.tenant</name>
        <value>tenant</value>
        <description>The default does not support modification</description>
    </property>
    <property>
        <name>hive.hmsbridge.delegateOnly</name>
        <value>false</value>
    </property>
    <property>
        <name>hive.hmsbridge.doubleWrite</name>
        <value>false</value>
    </property>
    <property>
        <name>hive.hmsbridge.readonly</name>
        <value>false</value>
    </property>
    <property>
        <name>hive.metastore.schema.verification</name>
        <value>${HIVE_SITE_CONF_hive_metastore_schema_verification}</value>
    </property>
    <property>
        <name>datanucleus.schema.autoCreateAll</name>
        <value>${HIVE_SITE_CONF_datanucleus_schema_autoCreateAll}</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>${HIVE_SITE_CONF_javax_jdo_option_ConnectionURL}</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>${HIVE_SITE_CONF_javax_jdo_option_ConnectionDriverName}</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>${HIVE_SITE_CONF_javax_jdo_option_ConnectionUserName}</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>${HIVE_SITE_CONF_javax_jdo_option_ConnectionPassword}</value>
    </property>
    <property>
        <name>hive.metastore.metrics.enabled</name>
        <value>true</value>
    </property>
</configuration>
EOF

hive --service metastore
