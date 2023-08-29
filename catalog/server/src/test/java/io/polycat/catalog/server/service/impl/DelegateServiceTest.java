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
package io.polycat.catalog.server.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import io.polycat.catalog.common.CatalogServerException;
import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.model.DelegateBriefInfo;
import io.polycat.catalog.common.model.DelegateOutput;
import io.polycat.catalog.common.plugin.request.input.DelegateInput;

import io.polycat.catalog.service.api.DelegateService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
@MapperScan(basePackages = {"io.polycat.catalog.store.mapper"})
public class DelegateServiceTest extends TestUtil {
    private static final Logger logger = Logger.getLogger(DatabaseServiceImplTest.class);

    @Autowired
    @Qualifier("delegateServiceImpl")
    private DelegateService delegateService;

    @BeforeEach
    public void clearAndSetup() {
        clearFDB();
    }

    @Test
    public void should_success_create_delegate() {
        DelegateInput sdInput = makeRandomDelegate();
        DelegateOutput sdOut = delegateService.createDelegate(projectId, sdInput);
        assertNotNull(sdOut);
    }

    @Test
    public void should_fail_create_delegate_with_same_name() {
        String providerName = "oBs"; // provider name is case-insensitive
        DelegateInput sdInput1 = makeRandomDelegate(providerName);
        DelegateOutput sdOut = delegateService.createDelegate(projectId, sdInput1);
        assertNotNull(sdOut);

        String sdName = sdInput1.getDelegateName();
        DelegateInput sdInput2 = makeDelegateWithName(sdName);
        MetaStoreException e = assertThrows(MetaStoreException.class,
                () -> delegateService.createDelegate(projectId, sdInput2));
        assertEquals(e.getErrorCode(), ErrorCode.DELEGATE_ALREADY_EXIST);
    }

    @Test
    public void should_fail_create_delegate_with_illegal_params() {
        // illegal provider name
        String illegalProvider = "OBSS3";
        DelegateInput sdInput = makeRandomDelegate(illegalProvider);
        CatalogServerException e = assertThrows(CatalogServerException.class,
                () -> delegateService.createDelegate(projectId, sdInput));
        assertEquals(e.getErrorCode(), ErrorCode.DELEGATE_PROVIDER_ILLEGAL);
    }

    @Test
    public void should_success_get_delegate() {
        DelegateInput sdInput = makeRandomDelegate();
        DelegateOutput sdOut = delegateService.createDelegate(projectId, sdInput);
        assertNotNull(sdOut);

        String sdName = sdInput.getDelegateName();
        DelegateOutput sdOut2 = delegateService.getDelegate(projectId, sdName);
        assertEquals(sdOut, sdOut2);
    }

    @Test
    public void should_fail_get_delegate_with_wrong_name() {
        DelegateInput sdInput = makeRandomDelegate();
        DelegateOutput sdOut = delegateService.createDelegate(projectId, sdInput);
        assertNotNull(sdOut);

        String sdName = sdInput.getDelegateName();
        String wrongSDName = "tt";
        assertNotEquals(sdName, wrongSDName);
        CatalogServerException e = assertThrows(CatalogServerException.class,
                () -> delegateService.getDelegate(projectId, wrongSDName));
        assertEquals(ErrorCode.DELEGATE_NOT_FOUND, e.getErrorCode());
        assertEquals("Storage delegate [tt] does not exist", e.getMessage());
    }

    @Test
    public void should_success_drop_delegate() {
        DelegateInput sdInput = makeRandomDelegate();
        DelegateOutput sdOut = delegateService.createDelegate(projectId, sdInput);
        assertNotNull(sdOut);

        String sdName = sdInput.getDelegateName();
        assertDoesNotThrow(() -> delegateService.dropDelegate(projectId, sdName));
    }

    @Test
    public void should_fail_drop_delegate_with_wrong_name() {
        DelegateInput sdInput = makeRandomDelegate();
        DelegateOutput sdOut = delegateService.createDelegate(projectId, sdInput);
        assertNotNull(sdOut);

        String sdName = sdInput.getDelegateName();
        String wrongSDName = UUID.randomUUID().toString();
        assertNotEquals(sdName, wrongSDName);
        MetaStoreException e = assertThrows(MetaStoreException.class,
                () -> delegateService.dropDelegate(projectId, wrongSDName));
        assertEquals(ErrorCode.DELEGATE_NOT_FOUND, e.getErrorCode());
    }

    @Test
    public void should_success_list_delegates() {
        String prefix = "delegate_with_same_prefix_";
        List<String> delegateNamesIn = new ArrayList<>();
        List<String> prefixedDelegateNamesIn = new ArrayList<>();
        List<String> delegateNamesOut = new ArrayList<>();
        List<String> prefixedDelegateNamesOut = new ArrayList<>();

        DelegateInput sdInput = makeRandomDelegate();
        DelegateOutput sdOut = delegateService.createDelegate(projectId, sdInput);
        assertNotNull(sdOut);
        delegateNamesIn.add(sdInput.getDelegateName());

        DelegateInput sdInput2 = makeDelegateWithName(prefix + "d1");
        DelegateOutput sdOut2 = delegateService.createDelegate(projectId, sdInput2);
        assertNotNull(sdOut2);
        delegateNamesIn.add(sdInput2.getDelegateName());
        prefixedDelegateNamesIn.add(sdInput2.getDelegateName());

        DelegateInput sdInput3 = makeDelegateWithName(prefix + "d2");
        DelegateOutput sdOut3 = delegateService.createDelegate(projectId, sdInput3);
        assertNotNull(sdOut3);
        delegateNamesIn.add(sdInput3.getDelegateName());
        prefixedDelegateNamesIn.add(sdInput3.getDelegateName());
        Collections.sort(delegateNamesIn);
        Collections.sort(prefixedDelegateNamesIn);

        // case: list all delegates
        List<DelegateBriefInfo> allDelegates = delegateService.listDelegates(projectId, "");
        for (DelegateBriefInfo dbi : allDelegates) {
            delegateNamesOut.add(dbi.getDelegateName());
        }
        Collections.sort(delegateNamesOut);
        assertEquals(delegateNamesIn, delegateNamesOut);

        // case: list delegates with prefix
        List<DelegateBriefInfo> prefixedDelegates = delegateService.listDelegates(projectId, prefix);
        for (DelegateBriefInfo dbi : prefixedDelegates) {
            prefixedDelegateNamesOut.add(dbi.getDelegateName());
        }
        Collections.sort(prefixedDelegateNamesOut);
        assertEquals(prefixedDelegateNamesIn, prefixedDelegateNamesOut);
    }


    private DelegateInput makeDelegateWithName(String sdName) {
        String delegateName = sdName;
        String domainName = "agency_domain_name_" + UUID.randomUUID();
        String agencyName = "agency_name_" + UUID.randomUUID();

        DelegateInput delegateInput = new DelegateInput();
        delegateInput.setDelegateName(delegateName);
        delegateInput.setUserId(userId);
        delegateInput.setStorageProvider("OBS");
        delegateInput.setProviderDomainName(domainName);
        delegateInput.setAgencyName(agencyName);
        delegateInput.setAllowedLocationList(new ArrayList<>());
        delegateInput.setBlockedLocationList(new ArrayList<>());
        return delegateInput;
    }

    private DelegateInput makeRandomDelegate() {
        return makeRandomDelegate("OBS");
    }

    private DelegateInput makeRandomDelegate(String provider) {
        return makeRandomDelegate(provider, new ArrayList<>(), new ArrayList<>());
    }

    private DelegateInput makeRandomDelegate(String provider, List<String> allowedLocation,
            List<String> blockedLocation) {
        String delegateName = "my_delegate_" + UUID.randomUUID();
        String domainName = "agency_domain_name_" + UUID.randomUUID();
        String agencyName = "agency_name_" + UUID.randomUUID();

        DelegateInput delegateInput = new DelegateInput();
        delegateInput.setDelegateName(delegateName);
        delegateInput.setUserId(userId);
        delegateInput.setStorageProvider(provider);
        delegateInput.setProviderDomainName(domainName);
        delegateInput.setAgencyName(agencyName);
        delegateInput.setAllowedLocationList(allowedLocation);
        delegateInput.setBlockedLocationList(blockedLocation);
        return delegateInput;
    }
}
