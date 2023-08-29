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
package io.polycat.probe.model;

import io.polycat.catalog.common.model.DataLineage;
import io.polycat.catalog.common.model.TableUsageProfile;
import io.polycat.catalog.common.plugin.request.input.AuthorizationInput;

import java.util.List;

public class SqlInfo {
    private String catalog;
    private List<TableUsageProfile> tableUsageProfiles;
    private DataLineage dataLineage;
    private Boolean isPermit = true;
    private List<AuthorizationInput> authorizationNowAllowList;

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public List<TableUsageProfile> getTableUsageProfiles() {
        return tableUsageProfiles;
    }

    public void setTableUsageProfiles(List<TableUsageProfile> tableUsageProfiles) {
        this.tableUsageProfiles = tableUsageProfiles;
    }

    public DataLineage getDataLineage() {
        return dataLineage;
    }

    public void setDataLineage(DataLineage dataLineage) {
        this.dataLineage = dataLineage;
    }

    public Boolean getPermit() {
        return isPermit;
    }

    public void setPermit(Boolean permit) {
        isPermit = permit;
    }

    public List<AuthorizationInput> getAuthorizationNowAllowList() {
        return authorizationNowAllowList;
    }

    public void setAuthorizationNowAllowList(List<AuthorizationInput> authorizationNowAllowList) {
        this.authorizationNowAllowList = authorizationNowAllowList;
    }

    @Override
    public String toString() {
        return "SqlInfo{"
                + "catalog='"
                + catalog
                + '\''
                + ", tableUsageProfiles="
                + tableUsageProfiles
                + ", dataLineage="
                + dataLineage
                + ", isPermit="
                + isPermit
                + ", authorizationNowAllowList="
                + authorizationNowAllowList
                + '}';
    }
}
