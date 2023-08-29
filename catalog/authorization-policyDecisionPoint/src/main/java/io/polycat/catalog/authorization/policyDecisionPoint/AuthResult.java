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
package io.polycat.catalog.authorization.policyDecisionPoint;

import java.util.List;

import io.polycat.catalog.common.model.ColumnFilter;
import io.polycat.catalog.common.model.DataMask;
import io.polycat.catalog.common.model.RowFilter;

import lombok.Getter;

@Getter
public class AuthResult {

    private boolean isAllowed;
    private String policyId;
    private String reason;
    private AuthRequest request;
    private ColumnFilter columnFilter;
    private RowFilter rowFilter;
    private List<DataMask> dataMasks;

    private AuthResult() {
    }

    public static class Builder {

        private boolean isAllowed;
        private String policyId;
        private String reason;
        private AuthRequest request;
        private ColumnFilter columnFilter;
        private RowFilter rowFilter;
        private List<DataMask> dataMasks;

        public AuthResult.Builder setIsAllowed(boolean isAllowed) {
            this.isAllowed = isAllowed;
            return this;
        }

        public AuthResult.Builder setPolicyId(String policyId) {
            this.policyId = policyId;
            return this;
        }

        public AuthResult.Builder setReason(String reason) {
            this.reason = reason;
            return this;
        }

        public AuthResult.Builder setRequest(AuthRequest request) {
            this.request = request;
            return this;
        }

        public AuthResult.Builder setColumnFilter(ColumnFilter columnFilter) {
            this.columnFilter = columnFilter;
            return this;
        }

        public AuthResult.Builder setRowFilter(RowFilter rowFilter) {
            this.rowFilter = rowFilter;
            return this;
        }

        public AuthResult.Builder setDataMask(List<DataMask> dataMasks) {
            this.dataMasks = dataMasks;
            return this;
        }

        public AuthResult build() {
            AuthResult result = new AuthResult();
            result.isAllowed = isAllowed;
            result.policyId = policyId;
            result.reason = reason;
            result.request = request;
            result.columnFilter = columnFilter;
            result.rowFilter = rowFilter;
            result.dataMasks = dataMasks;
            return result;
        }
    }
}


