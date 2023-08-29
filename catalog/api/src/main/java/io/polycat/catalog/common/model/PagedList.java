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
package io.polycat.catalog.common.model;


import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PagedList<T> {

    @Getter
    static public class PageInfo{
        private String nextMarker;
        private String previousMarker;
        private Integer currentCount;
    }

    private T[] objects;
    private final PageInfo pageInfo = new PageInfo();

    public void setPageInfo(String nextMarker, String previousMarker, Integer currentCount) {
        pageInfo.nextMarker = nextMarker;
        pageInfo.previousMarker = previousMarker;
        pageInfo.currentCount = currentCount;
    }

    public void setNextMarker(String nextMarker) {
        pageInfo.nextMarker = nextMarker;
    }

    public void setPreviousMarker(String previousMarker) {
        pageInfo.previousMarker = previousMarker;
    }

    public void setCurrentCount(Integer currentCount) {
        pageInfo.currentCount = currentCount;
    }

    public String getNextMarker() {
        return pageInfo.nextMarker;
    }

    public String getPreviousMarker() {
        return pageInfo.previousMarker;
    }

    public Integer getCurrentCount() {
        return pageInfo.currentCount;
    }
}
