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
package io.polycat.catalog.common.lineage;

public enum EJobStatus {
    /**
     * job status success.
     */
    SUCCESS(0),
    FAILED(1);

    private final int num;

    EJobStatus(int num) {
        this.num = num;
    }

    public static EJobStatus forNum(int num) {
        switch (num) {
            case 0:
                return SUCCESS;
            case 1:
                return FAILED;
            default:
                throw new IllegalArgumentException("Unsupported job status.");
        }
    }

    public int getNum() {
        return num;
    }
}
