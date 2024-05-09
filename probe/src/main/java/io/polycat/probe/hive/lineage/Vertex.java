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
package io.polycat.probe.hive.lineage;

public class Vertex {
    public Type type;
    public String label;
    public String dbName;
    public String tableName;
    public String columnName;

    public Vertex(String label, Type type, String dbName, String tableName, String columnName) {
        this.label = label;
        this.type = type;
        this.dbName = dbName;
        this.tableName = tableName;
        this.columnName = columnName;
    }

    @Override
    public int hashCode() {
        return this.label.hashCode() + this.type.hashCode() * 3;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (!(obj instanceof Vertex)) {
            return false;
        } else {
            Vertex vertex = (Vertex) obj;
            return this.label.equals(vertex.label) && this.type == vertex.type;
        }
    }

    public enum Type {
        COLUMN,
        TABLE;

        Type() {}
    }
}
