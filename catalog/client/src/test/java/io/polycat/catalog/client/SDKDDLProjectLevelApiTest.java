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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.http.HttpResponseException;
import io.polycat.catalog.common.model.DelegateBriefInfo;
import io.polycat.catalog.common.model.DelegateOutput;
import io.polycat.catalog.common.model.DelegateStorageProvider;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.plugin.request.CreateDelegateRequest;
import io.polycat.catalog.common.plugin.request.CreateShareRequest;
import io.polycat.catalog.common.plugin.request.DeleteDelegateRequest;
import io.polycat.catalog.common.plugin.request.DropShareRequest;
import io.polycat.catalog.common.plugin.request.GetDelegateRequest;
import io.polycat.catalog.common.plugin.request.ListDelegatesRequest;
import io.polycat.catalog.common.plugin.request.input.DelegateInput;
import io.polycat.catalog.common.plugin.request.input.ShareInput;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SDKDDLProjectLevelApiTest extends SDKTestUtil {
    private final PolyCatClient client = SDKTestUtil.getClient();

    @Test
    public void createShareTest() throws CatalogException {
        String shareName = "share_name_s1";

        CreateShareRequest createShareRequest = new CreateShareRequest();
        ShareInput shareInput = new ShareInput();
        shareInput.setShareName(shareName);
        String accountId = "TestAccount";
        shareInput.setAccountId(accountId);
        shareInput.setUserId(OWNER);
        createShareRequest.setProjectId(PROJECT_ID);
        createShareRequest.setInput(shareInput);
        client.createShare(createShareRequest);

        //drop share
        DropShareRequest dropShareRequest = new DropShareRequest();
        dropShareRequest.setProjectId(PROJECT_ID);
        dropShareRequest.setShareName(shareName);
        client.dropShare(dropShareRequest);
    }

    @Test
    public void should_success_create_get_delete_delegates() {
        // case: create delegate
        String delegateName = "my_basic_delegate";
        DelegateInput delegateIn = makeDelegateWithName(delegateName);

        CreateDelegateRequest createRequest = new CreateDelegateRequest();
        createRequest.setProjectId(PROJECT_ID);
        createRequest.setInput(delegateIn);
        DelegateOutput delegateOut1 = client.createDelegate(createRequest);
        assertEquals(DelegateStorageProvider.OBS.getValue(), delegateOut1.getStorageProvider());
        assertEquals(delegateIn.getProviderDomainName(), delegateOut1.getProviderDomainName());
        assertEquals(delegateIn.getAgencyName(), delegateOut1.getAgencyName());

        // case: get delegate
        GetDelegateRequest getRequest = new GetDelegateRequest();
        getRequest.setProjectId(PROJECT_ID);
        getRequest.setDelegateName(delegateName);
        DelegateOutput delegateOut2 = client.getDelegate(getRequest);
        assertEquals(delegateOut2, delegateOut1);

        // case: drop delegate
        DeleteDelegateRequest delRequest = new DeleteDelegateRequest();
        delRequest.setProjectId(PROJECT_ID);
        delRequest.setDelegateName(delegateName);
        assertDoesNotThrow(() -> client.deleteDelegate(delRequest));
        CatalogException e = assertThrows(CatalogException.class, () -> client.getDelegate(getRequest));
        assertEquals(HttpResponseException.class, e.getCause().getClass());
        assertEquals(404, ((HttpResponseException)e.getCause()).getStatusCode());
        assertEquals(String.format("Storage delegate [%s] does not exist", delegateName),
            ((HttpResponseException)e.getCause()).getReasonPhrase());
    }

    @Test
    public void should_success_list_delegates() {
        String delegateName = "list_delegate";
        String prefix = "list";
        DelegateInput delegateIn = makeDelegateWithName(delegateName);
        CreateDelegateRequest createRequest = new CreateDelegateRequest();
        createRequest.setProjectId(PROJECT_ID);
        createRequest.setInput(delegateIn);
        DelegateOutput delegateOut1 = client.createDelegate(createRequest);
        assertEquals(DelegateStorageProvider.OBS.getValue(), delegateOut1.getStorageProvider());
        assertEquals(delegateIn.getProviderDomainName(), delegateOut1.getProviderDomainName());
        assertEquals(delegateIn.getAgencyName(), delegateOut1.getAgencyName());

        // case: list delegates without prefix
        List<DelegateBriefInfo> delegateInfos = new ArrayList<>();
        ListDelegatesRequest listRequest = new ListDelegatesRequest();
        listRequest.setProjectId(PROJECT_ID);
        PagedList<DelegateBriefInfo> response = client.listDelegates(listRequest);
        Collections.addAll(delegateInfos, response.getObjects());
        assertEquals(1, delegateInfos.size());
        assertEquals(delegateName, delegateInfos.get(0).getDelegateName());
        assertEquals(delegateOut1.getStorageProvider(), delegateInfos.get(0).getStorageProvider());
        assertEquals(OWNER, delegateInfos.get(0).getUserId());

        // case: list delegates with prefix
        List<DelegateBriefInfo> prefixedDelegateInfos = new ArrayList<>();
        ListDelegatesRequest listRequest2 = new ListDelegatesRequest();
        listRequest2.setProjectId(PROJECT_ID);
        listRequest2.setPattern(prefix);
        PagedList<DelegateBriefInfo> response2 = client.listDelegates(listRequest2);
        Collections.addAll(prefixedDelegateInfos, response2.getObjects());
        assertEquals(1, prefixedDelegateInfos.size());
        assertEquals(delegateName, prefixedDelegateInfos.get(0).getDelegateName());
        assertEquals(delegateOut1.getStorageProvider(), prefixedDelegateInfos.get(0).getStorageProvider());
        assertEquals(OWNER, prefixedDelegateInfos.get(0).getUserId());
    }

    private DelegateInput makeDelegateWithName(String delegateName) {
        DelegateInput delegateIn = new DelegateInput();
        delegateIn.setDelegateName(delegateName);
        delegateIn.setUserId(OWNER);
        delegateIn.setStorageProvider("obs");
        delegateIn.setProviderDomainName("my_basic_domain_name");
        delegateIn.setAgencyName("my_basic_agency_name");
        delegateIn.setAllowedLocationList(new ArrayList<>());
        delegateIn.setBlockedLocationList(new ArrayList<>());
        return delegateIn;
    }
}
