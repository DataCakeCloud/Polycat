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

import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.exception.CarbonSqlException;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.base.PartitionInput;
import io.polycat.catalog.common.plugin.request.input.FileInput;
import io.polycat.catalog.common.plugin.request.input.FileStatsInput;
import io.polycat.catalog.common.model.Column;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.util.Shell;

public class PartitionUtil {

    public static final String DEFAULT_PARTITION_NAME = "__HIVE_DEFAULT_PARTITION__";

    public static final Character[] ESCAPE_SKIP_CHAR_PARTITION_NAME_FILE = new Character[]{'=', '/'};

    private static final Logger LOG = Logger.getLogger(PartitionUtil.class);

    private static void checkInputPartitionColumns(List<Column> schemaFieldList, List<Column> partitionColumns) {
        HashSet<String> nameSet = new HashSet<>();
        schemaFieldList.stream().map(Column::getColumnName).forEach(nameSet::add);
        partitionColumns.stream().map(Column::getColumnName).forEach(name -> {
            if (!nameSet.contains(name)) {
                throw new CarbonSqlException("The input partition does not match the partition of the table");
            }
        });
    }

    /**
     * TODO partitionName contain '=' or '/' will write and query questions, fix later
     * @param partitionName
     * @return
     */
    public static List<String> convertNameToVals(String partitionName) {
        return Arrays.stream(partitionName.split("/"))
            .map(x -> x.split("=")[1])
            .collect(Collectors.toList());
    }

    public static List<List<String>> convertNameToValsList(List<String> partNames) {
        List<List<String>> partValuesList = new ArrayList<>();
        for (String partName : partNames) {
            List<String> onePartVal = convertNameToVals(partName);
            partValuesList.add(onePartVal);
        }
        return partValuesList;
    }

    public static Map<String, String> convertNameToKvMap(String partitionName) {
        final Map<String, String> partitionKV = new HashMap<>();
        Arrays.stream(partitionName.split("/")).forEach(x -> {
            final String[] split = x.split("=");
            partitionKV.put(split[0], split[1]);
        });
        return partitionKV;
    }

    public static String escapePartitionName(String partitionName) {
        return escapePathName(partitionName, ESCAPE_SKIP_CHAR_PARTITION_NAME_FILE);
    }

    public static String unescapePartitionName(String partitionName) {
        return unescapePathName(partitionName);
    }

    public static String makePartitionName(List<String> partitionKeys, List<String> partitionValues) {
        StringBuilder partitionFolder = new StringBuilder();
        if (partitionKeys.size() != partitionValues.size()) {
            throw new CatalogException("partition values number not equals keys");
        }
        for (int i = 0; i < partitionKeys.size(); i++) {
            partitionFolder.append(partitionKeys.get(i))
                .append("=")
                .append(FileUtils.escapePathName(partitionValues.get(i)))
                .append("/");
        }
        return partitionFolder.substring(0, partitionFolder.length() - 1);
    }

    public static String generatePartitionFileName(String partitionFolderName, String fileName) {
        if (partitionFolderName == null) {
            return PathUtil.normalizePath(fileName);
        } else {
            return PathUtil.normalizePath(partitionFolderName, fileName);
        }
    }

    public static PartitionInput buildPartitionBaseInput(Table table) {
        PartitionInput partitionBase = new PartitionInput();
        List<Column> columnInputs = new ArrayList<>();
        if (table.getStorageDescriptor() != null) {
            List<Column> schemas = table.getStorageDescriptor().getColumns();
            for (Column field : schemas) {
                Column columnInput = new Column();
                columnInput.setColumnName(field.getColumnName());
                columnInput.setComment(field.getComment());
                columnInput.setColType(field.getColType());
                columnInputs.add(columnInput);
            }
            StorageDescriptor sd = new StorageDescriptor();
            sd.setColumns(columnInputs);
            partitionBase.setStorageDescriptor(sd);
        }
        return partitionBase;
    }

    public static void collectFileList(String tablePath, List<String> partitionNames, List<String> partitionValues,
        PartitionInput partitionBase, Configuration conf) {

        String partitionName = makePartitionName(partitionNames, partitionValues);
        partitionBase.setPartitionValues(partitionValues);
        String partitionLocation = tablePath + File.separator + partitionName;
        collectFileList(partitionLocation, partitionBase, conf);
    }

    public static void collectFileList(String partitionLocation, PartitionInput partitionBase, Configuration conf) {
        if (partitionBase.getStorageDescriptor() == null) {
            partitionBase.setStorageDescriptor(new StorageDescriptor());
        }

        partitionBase.getStorageDescriptor().setLocation(partitionLocation);
        if (conf == null) {
            conf = new Configuration();
        }
        FileStatus[] fileStatuses = listDataFiles(partitionLocation, conf);
        Objects.requireNonNull(fileStatuses);
        FileInput[] fileInputs = Arrays.stream(fileStatuses)
            .map(file -> {
                FileInput fileInput = new FileInput();
                fileInput.setFileName(file.getPath().getName());
                fileInput.setLength(file.getLen());
                fileInput.setOffset(0);
                fileInput.setRowCount(-1);
                return fileInput;
            }).toArray(FileInput[]::new);
        partitionBase.setFiles(fileInputs);
        partitionBase.setIndex(new FileStatsInput[]{});
    }

    private static FileStatus[] listDataFiles(String partitionLocation, Configuration conf) {
        try {
            Path path = new Path(partitionLocation);
            FileSystem fileSystem = path.getFileSystem(conf);
            return fileSystem.listStatus(path);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new CatalogException(e);
        }
    }

    private static BitSet charToEscape = new java.util.BitSet(128);

    static {

        /**
         * ASCII 01-1F are HTTP control characters that need to be escaped.
         * \u000A and \u000D are \n and \r, respectively.
        */
        char[] clist = new char[] {
        '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007', '\u0008', '\u0009',
        '\n', '\u000B', '\u000C', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012', '\u0013',
        '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019', '\u001A', '\u001B', '\u001C',
        '\u001D', '\u001E', '\u001F', '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '\u007F',
        '{', '[', ']', '^'};
        for (char c : clist) {
            charToEscape.set(c);
        }
        if (Shell.WINDOWS) {
            charToEscape.set(' ');
            charToEscape.set('<');
            charToEscape.set('>');
            charToEscape.set('|');
        }
    }

    private static boolean needsEscaping(char c) {
        return c >= 0 && c < charToEscape.size() && charToEscape.get(c);
    }

    public static String escapePathName(String path, Character[] skipChars) {
        StringBuilder builder = new StringBuilder();
        Set<Character> skipCharSet = new HashSet<>();
        if (skipChars != null && skipChars.length > 0) {
            skipCharSet.addAll(Arrays.asList(skipChars));
        }
        for (char c : path.toCharArray()) {
            if (!skipCharSet.contains(c) && needsEscaping(c)) {
                builder.append('%');
                builder.append(String.format("%1$02X", (int) c));
            } else {
                builder.append(c);
            }
        }
        return builder.toString();
    }

    public static String escapePathName(String path) {
        StringBuilder builder = new StringBuilder();
        for (char c : path.toCharArray()) {
            if (needsEscaping(c)) {
                builder.append('%');
                String string16 = Integer.toString(c, 16);
                if (string16.length() == 1) {
                    builder.append("0");
                }
                builder.append(string16);
            } else {
                builder.append(c);
            }
        }
        return builder.toString();
    }


    public static String unescapePathName(String path) {
        StringBuilder builder = new StringBuilder();
        int i = 0;
        while (i < path.length()) {
            char c = path.charAt(i);
            if (c == '%' && i + 2 < path.length()) {
                int code = -1;
                try {
                    code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
                } catch (Exception ignored) {
                }
                if (code >= 0) {
                    builder.append((char) code);
                    i += 3;
                } else {
                    builder.append(c);
                    i += 1;
                }
            } else {
                builder.append(c);
                i += 1;
            }
        }

        return builder.toString();
    }

    public static List<String> getPartitionKeysFromTable(io.polycat.catalog.common.model.Table table){
        return table.getPartitionKeys().stream()
            .map(Column::getColumnName)
            .collect(Collectors.toList());
    }

}
