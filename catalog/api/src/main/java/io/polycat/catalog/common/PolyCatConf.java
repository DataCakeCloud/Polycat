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
package io.polycat.catalog.common;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class PolyCatConf {
    private static final Logger logger = Logger.getLogger(PolyCatConf.class.getName());

    public static final String DRIVER_ENABLE = "driver";

    public static final String ENGINE_SERVICE_HOST = "engine.host";

    public static final String ENGINE_SERVICE_PORT = "engine.port";

    public static final String DRIVER_REGISTRY_HOST = "driver.registry.host";

    public static final String DRIVER_REGISTRY_PORT = "driver.registry.port";

    public static final String DRIVER_STREAMER_HOST = "driver.streamer.host";

    public static final String DRIVER_STREAMER_PORT = "driver.streamer.port";

    public static final String CATALOG_HOST = "catalog.host";

    public static final String CATALOG_PORT = "catalog.port";

    public static final String QUERY_REWRITE_HOST = "driver.mvrewrite.host";

    public static final String QUERY_REWRITE_PORT = "driver.mvrewrite.port";

    public static final String GATEWAY_CLI_PORT = "gateway.cli.port";

    public static final String GATEWAY_THRIFT_PORT = "gateway.thrift.port";

    public static final String WORKER_STAGE_HOST = "worker.stage.host";

    public static final String WORKER_STAGE_PORT = "worker.stage.port";

    public static final String WORKER_PRODUCER_PORT = "worker.producer.port";

    public static final String WORKER_CONSUMER_PORT = "worker.consumer.port";

    public static final String WORKER_CPU_CORES = "worker.cpu.cores";

    public static final String WORKER_GPU_CORES = "worker.gpu.cores";

    public static final String METRICS_EXPORTER_PORT = "metrics.exporter.port";

    public static final String WORKER_LIB_DIR = "worker.lib.dir";

    public static final String WAREHOUSE = "catalog.sql.warehouse.dir";

    public static final String OBS_AK = "fs.obs.access.key";

    public static final String OBS_SK = "fs.obs.secret.key";

    public static final String OBS_ENDPOINT = "fs.obs.endpoint";

    public static final String POLYCAT_CONFI_DIR = "polycat.conf.dir";

    public static final String CATALOG_IMC_URL = "catalog.imc.url";

    public static final String CATALOG_IMC_KEY = "catalog.imc.key";

    public static final String CATALOG_IMC_SECRET = "catalog.imc.secret";

    public static final String AUTH_IMPL = "auth.impl";

    public static final String METRICS_MYPERF4J_PORT = "myperf4j.port";

    public static final String LMS_SERVER_XML = "lms-server.xml";

    public static final String KEY_RANGER_ADMIN_URL_SINGLETON = "lms.ranger.admin.url.singleton";

    public static final String KEY_RANGER_REST_CLIENT_USERNAME = "lms.ranger.rest.client.username";

    public static final String KEY_RANGER_REST_CLIENT_PASSWORD = "lms.ranger.rest.client.password";

    public static final String KEY_RANGER_AUTH_SKIP = "lms.ranger.authorization.skip";

    public static final Boolean VAL_DEFAULT_AUTHENTICATION_SKIP = false;

    private static Configuration XMLconf;

    public static final String DRIVER_MVREWRITE_HOST = "catalog.imc.secret";

    public static final String DRIVER_MVREWRITE_PORT = "catalog.imc.secret";

    public static final String CACHE_DIR = "catalog.auth.cache.dir";

    public static final String POLICY_UPDATE_CYCLE_TIME = "catalog.auth.policy.update.time";

    @Getter
    private final Map<String, String> conf = new HashMap<>();

    public PolyCatConf() {

    }

    public PolyCatConf(String confFilePath) {
        load(confFilePath);
    }


    public PolyCatConf(String confFilePath, boolean isResourceXml) {
        if (isResourceXml) {
            loadResourceXml(confFilePath);
        } else {
            load(confFilePath);
        }
    }

    private void loadResourceXml(String fileName) {
        Configuration conf = new Configuration();
        conf.addResource(PolyCatConf.class.getClassLoader().getResource(fileName));
        logger.info("load XML Configuration success. " + conf);
        XMLconf = conf;
    }

    public Configuration getXMLConfiguration() { return XMLconf; }

    public String getRangerURL() { return XMLconf.get(KEY_RANGER_ADMIN_URL_SINGLETON); }

    public String getRangerUserName() {
        return XMLconf.get(KEY_RANGER_REST_CLIENT_USERNAME);
    }

    public String getRangerPasswd() {
        return XMLconf.get(KEY_RANGER_REST_CLIENT_PASSWORD);
    }

    public boolean getRangerAuthSwitch() {
        return XMLconf.getBoolean(KEY_RANGER_AUTH_SKIP, VAL_DEFAULT_AUTHENTICATION_SKIP);
    }

    public static String getConfPath(Configuration conf) {
        if (conf != null) {
            String confDir = conf.get(POLYCAT_CONFI_DIR);
            if (!StringUtils.isBlank(confDir)) {
                String confInSpark = confDir + "/gateway.conf";
                if (new File(confInSpark).exists()) {
                    logger.info("using configuration " + confInSpark);
                    return confInSpark;
                } else {
                    logger.warn("XML configured gateway.conf file not found: " + confInSpark);
                }
            }
        }

        String sparkHome = System.getenv("SPARK_HOME");
        if (!StringUtils.isBlank(sparkHome)) {
            String confInSpark = sparkHome + "/conf/gateway.conf";
            if (new File(confInSpark).exists()) {
                logger.info("using configuration " + confInSpark);
                return confInSpark;
            } else {
                logger.warn("System env configured gateway.conf file not found: " + confInSpark);
            }
        }

        String confPath = GlobalConfig.getString(GlobalConfig.CONF_DIR) + "/gateway.conf";
        if (new File(confPath).exists()) {
            logger.info("using configuration " + confPath);
            return confPath;
        } else {
            logger.warn("gateway.conf not found: " + confPath);
        }

        return null;
    }

    private void conf(String key, String value) {
        conf.put(key, value);
    }

    private void load(String filePath) {
        try (InputStream input = FileUtils.openInputStream(FileUtils.getFile(filePath))) {
            Properties prop = new Properties();
            prop.load(input);
            for (Map.Entry<Object, Object> entry : prop.entrySet()) {
                conf(entry.getKey().toString(), entry.getValue().toString());
            }
            logger.info("loaded properties: " + filePath);
        } catch (IOException ex) {
            logger.error("Failed to load properties file " + filePath, ex);
        }
    }

    protected String stringValue(String key) {
        Object obj = conf.get(key);
        if (obj == null) {
            logger.warn("stringValue is null:" + key);
            return "";
        }
        return obj.toString();
    }

    protected int intValue(String key) {
        String value = conf.get(key);
        if (value == null || StringUtils.isBlank(value)) {
            logger.warn("intValue is null:" + key);
            return 0;
        }
        return Integer.parseInt(value);
    }

    protected long longValue(String key) {
        String value = conf.get(key);
        if (value == null || StringUtils.isBlank(value)) {
            logger.warn("longValue is null:" + key);
            return 0;
        }
        return Long.parseLong(value);
    }

    private boolean boolValue(String key) {
        String value = conf.get(key);
        if (value == null || StringUtils.isBlank(value)) {
            logger.warn("boolValue is null:" + key);
            return false;
        }
        return Boolean.parseBoolean(value);
    }

    public boolean isDriver() {
        return boolValue(DRIVER_ENABLE);
    }

    public int getThriftServicePort() {
        return intValue(GATEWAY_THRIFT_PORT);
    }

    public int getCliServicePort() {
        return intValue(GATEWAY_CLI_PORT);
    }

    public int getMetricsPort() {
        return intValue(METRICS_EXPORTER_PORT);
    }

    public int getCpuCores() {
        return intValue(WORKER_CPU_CORES);
    }

    public int getGpuCores() {
        return intValue(WORKER_GPU_CORES);
    }

    public int getStageServicePort() {
        return intValue(WORKER_STAGE_PORT);
    }

    public String getStageServiceHost() {
        return stringValue(WORKER_STAGE_HOST);
    }

    public int getRegistryServicePort() {
        return intValue(DRIVER_REGISTRY_PORT);
    }

    public String getStreamerHost() {
        return stringValue(DRIVER_STREAMER_HOST);
    }

    public int getStreamerPort() {
        return intValue(DRIVER_STREAMER_PORT);
    }

    public String getRegistryServiceHost() { return stringValue(DRIVER_REGISTRY_HOST); }

    public String getCatalogHost() {
        return stringValue(CATALOG_HOST);
    }

    public int getCatalogPort() {
        return intValue(CATALOG_PORT);
    }

    public String getQueryRewriteHost() {
        return stringValue(QUERY_REWRITE_HOST);
    }

    public int getQueryRewritePort() {
        return intValue(QUERY_REWRITE_PORT);
    }

    public int getProducerPort() {
        return intValue(WORKER_PRODUCER_PORT);
    }

    public int getConsumerPort() {
        return intValue(WORKER_CONSUMER_PORT);
    }

    public Address getStageAddress() {
        return new Address(getStageServiceHost(), getStageServicePort());
    }

    public Address getProducerAddress() {
        return new Address(getStageServiceHost(), getProducerPort());
    }

    public Address getConsumerAddress() {
        return new Address(getStageServiceHost(), getConsumerPort());
    }

    public Address getRegistryAddress() {
        return new Address(getRegistryServiceHost(), getRegistryServicePort());
    }

    public String getLibDir() {
        return stringValue(WORKER_LIB_DIR);
    }

    public String getWarehouse() {
        return stringValue(WAREHOUSE);
    }

    public String getObsAk() {
        return stringValue(OBS_AK);
    }

    public String getObsSk() {
        return stringValue(OBS_SK);
    }

    public String getObsEndpoint() {
        return stringValue(OBS_ENDPOINT);
    }

    public Address getEngineServiceAddress() {
        return new Address(stringValue(ENGINE_SERVICE_HOST), intValue(ENGINE_SERVICE_PORT));
    }

    public String getCatalogImcUrl() { return stringValue(CATALOG_IMC_URL); }

    public String getCatalogImcKey() { return stringValue(CATALOG_IMC_KEY); }

    public String getCatalogImcSecret() { return stringValue(CATALOG_IMC_SECRET); }

    public String getAuthImpl() { return stringValue(AUTH_IMPL); }

    public int getMyperf4jPort() {
        return intValue(METRICS_MYPERF4J_PORT);
    }

    public String getDriverMvRewriteHost() {
        return stringValue(DRIVER_MVREWRITE_HOST);
    }

    public String getDriverMvRewritePort() {
        return stringValue(DRIVER_MVREWRITE_PORT);
    }

    public String getCacheDir() { return  stringValue(CACHE_DIR); }

    public long getPolicyUpdateCycleTime() { return longValue(POLICY_UPDATE_CYCLE_TIME); }
}
