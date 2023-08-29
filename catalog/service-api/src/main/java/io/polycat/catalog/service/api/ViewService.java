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
package io.polycat.catalog.service.api;

import io.polycat.catalog.common.model.DatabaseName;
import io.polycat.catalog.common.model.ViewIdent;
import io.polycat.catalog.common.model.ViewName;
import io.polycat.catalog.common.model.ViewRecordObject;
import io.polycat.catalog.common.plugin.request.input.ViewInput;

public interface ViewService {

    /**
     * create view
     *
     * @param databaseName
     * @param viewInput
     */
    void createView(DatabaseName databaseName, ViewInput viewInput);

    /**
     * drop view
     *
     * @param viewName
     */
    void dropView(ViewName viewName);

    /**
     * query view by roleName
     *
     * @param viewName
     * @return view
     */
    ViewRecordObject getViewByName(ViewName viewName);

    /**
     * query view by viewId
     *
     * @param viewIdent
     * @return view
     */
    ViewRecordObject getViewById(ViewIdent viewIdent);

    /**
     * alter view
     *
     * @param viewName
     * @param viewInput
     */
    void alterView(ViewName viewName, ViewInput viewInput);

    /**
     * list files
     *
     * @param viewNameParam
     * @param filterInput
    */
//    Partition[] listFiles(ViewName viewNameParam, FilterInput filterInput);
}