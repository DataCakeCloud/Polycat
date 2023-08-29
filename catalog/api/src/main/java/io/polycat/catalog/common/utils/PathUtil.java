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
package io.polycat.catalog.common.utils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import io.polycat.catalog.common.GlobalConfig;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.exception.CarbonSqlException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PathUtil {

    private static final Logger LOG = Logger.getLogger(PathUtil.class);

    public static String normalizePath(String path) {
        return new Path(path).toUri().toString();
    }

    public static String normalizePath(String parent, String child) {
        return new Path(parent, child).toUri().toString();
    }

    public static String tableFolderName(String tableName, String tableId) {
        return tableName + "_" + tableId;
    }

    public static String tablePath(String databaseLocation, String tableName, String tableId) {
        return normalizePath(databaseLocation, tableFolderName(tableName, tableId));
    }

    public static String generateFileName(String ext) {
        return new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS").format(new Date()) + "." + ext;
    }

    public static String generatePartitionFolder() {
        return UUID.randomUUID().toString();
    }


    public static String getDatabaseLocation(String projectId, String databaseName) {

        String filePath = GlobalConfig.getString(GlobalConfig.WAREHOUSE);
        try {
            Configuration configuration = ConfigurationUtil.getOBSConfiguration();

            Path path = new Path(filePath);
            FileSystem fileSystem = path.getFileSystem(configuration);
            if (!fileSystem.exists(path)) {
                fileSystem.mkdirs(path);
            }
            FileStatus fileStatus = fileSystem.getFileStatus(path);
            filePath = fileStatus.getPath().toString();
        } catch (IOException e) {
            LOG.error(e);
            throw new CarbonSqlException("Failed to get database location");
        }

        String projectIdPath = PathUtil.normalizePath(filePath, projectId);
        return PathUtil.normalizePath(projectIdPath, databaseName);
    }
}
