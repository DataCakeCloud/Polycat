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
package io.polycat.catalog.client;

import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.discovery.DiscoverySearchBase;
import io.polycat.catalog.common.model.discovery.TableCategories;
import io.polycat.catalog.common.model.discovery.TableSearch;
import io.polycat.catalog.common.plugin.request.CreateGlossaryRequest;
import io.polycat.catalog.common.plugin.request.GetTableCategoriesRequest;
import io.polycat.catalog.common.plugin.request.SearchBaseRequest;
import io.polycat.catalog.common.plugin.request.SearchDiscoveryNamesRequest;
import io.polycat.catalog.common.plugin.request.TableSearchRequest;
import io.polycat.catalog.common.plugin.request.input.FilterConditionInput;
import io.polycat.catalog.common.plugin.request.input.GlossaryInput;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class SDKDiscoveryTest {

    /**
     * TODO
     * @return
     */
    public static PolyCatClient getClient() {
        PolyCatClient client = new PolyCatClient();
        client.setContext(CatalogUserInformation.doAuth("test", "dash"));
        client.getContext().setTenantName("shenzhen");
        client.getContext().setProjectId("default_project");
        client.getContext().setToken("nonull");
        return client;
    }

    @Test
    public void test_searchTable() {
        final PolyCatClient client = getClient();
        TableSearchRequest request = new TableSearchRequest(client.getProjectId(),"test part", null);
        PagedList<TableSearch> searchTable = client.searchTable(request);
        for (TableSearch table : searchTable.getObjects()) {
            log.info("searchTable: {}", table);
        }
    }

    @Test
    public void test_search() {
        final PolyCatClient client = getClient();
        final PagedList<DiscoverySearchBase> search = client.search(new SearchBaseRequest("xxxxxxxx", "", "xxxxxxxx_ue1", ObjectType.DATABASE, new FilterConditionInput()));

        for (DiscoverySearchBase table : search.getObjects()) {
            log.info("searchTable: {}", table);
        }
    }

    @Test
    public void test_searchDiscoveryNames() {
        final PolyCatClient client = getClient();
        SearchDiscoveryNamesRequest request = new SearchDiscoveryNamesRequest(client.getProjectId(),null, null, null);
        PagedList<String> searchTable = client.searchDiscoveryNames(request);
        for (String name : searchTable.getObjects()) {
            log.info("name: {}", name);
        }
    }

    @Test
    public void testCreateGlossary() {
        final PolyCatClient client = getClient();
        final CreateGlossaryRequest createGlossaryRequest = new CreateGlossaryRequest();
        createGlossaryRequest.setProjectId("xxxxxxxx");
        final GlossaryInput glossaryInput = new GlossaryInput();
        glossaryInput.setName("testCreateGlossary");
        createGlossaryRequest.setInput(glossaryInput);
        client.createGlossary(createGlossaryRequest);
    }

    @Test
    public void testGetTableCategories() {
        final PolyCatClient client = getClient();
        final GetTableCategoriesRequest getTableCategoriesRequest = new GetTableCategoriesRequest();
        getTableCategoriesRequest.setProjectId("xxxxxxxx");
        getTableCategoriesRequest.setQualifiedName("xxxxxxxx_ue1.mps_dmp.dws_user_push_detail_di");
        final TableCategories tableCategories = client.getTableCategories(getTableCategoriesRequest);
        System.out.println(tableCategories);
    }

}
