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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(description = "data source")
@Data
public class DataSource {

    @ApiModelProperty(value = "source type", required = true)
    private DataSourceType sourceType;

    @ApiModelProperty(value = "table source", required = true)
    private TableSource tableSource;

    @ApiModelProperty(value = "stream source", required = true)
    private StreamerSource streamerSource;

    @ApiModelProperty(value = "file source", required = true)
    private FileSource fileSource;

    private AppSource appSource;

    public DataSource() {
    }

    public DataSource(TableSource tableSource) {
        this.sourceType = DataSourceType.SOURCE_TABLE;
        this.tableSource = tableSource;
    }

    public DataSource(StreamerSource streamerSource) {
        this.sourceType = DataSourceType.SOURCE_STREAM;
        this.streamerSource = streamerSource;
    }

    public DataSource(FileSource fileSource) {
        this.sourceType = DataSourceType.SOURCE_FILE;
        this.fileSource = fileSource;
    }

    public DataSource(AppSource appSource) {
        this.sourceType = DataSourceType.SOURCE_APP;
        this.appSource = appSource;
    }
}
