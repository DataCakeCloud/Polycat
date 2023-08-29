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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import io.polycat.catalog.client.util.Constants;
import io.polycat.catalog.client.util.RequestChecker;
import io.polycat.catalog.common.PolyCatConf;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.http.CatalogClientHelper;
import io.polycat.catalog.common.model.*;
import io.polycat.catalog.common.model.stats.AggrStatisticData;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import io.polycat.catalog.common.model.stats.PartitionStatisticData;
import io.polycat.catalog.common.plugin.CatalogContext;
import io.polycat.catalog.common.plugin.CatalogPlugin;
import io.polycat.catalog.common.plugin.request.*;
import io.polycat.catalog.common.plugin.request.base.AcceleratorRequestBase;
import io.polycat.catalog.common.plugin.request.input.AcceleratorInput;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.CreateMaterializedViewRequest;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import static io.polycat.catalog.common.http.CatalogClientHelper.deleteWithHeader;
import static io.polycat.catalog.common.http.CatalogClientHelper.getWithHeader;
import static io.polycat.catalog.common.http.CatalogClientHelper.patchWithHeader;
import static io.polycat.catalog.common.http.CatalogClientHelper.postWithHeader;
import static io.polycat.catalog.common.http.CatalogClientHelper.putWithHeader;
import static io.polycat.catalog.common.http.CatalogClientHelper.withCatalogException;
import static java.util.stream.Collectors.toList;

public class PolyCatClient implements CatalogPlugin {
    private final String catalogServerUrlPrefix;

    private CatalogContext context;

    public PolyCatClient() {
        this("127.0.0.1", 8082);
    }

    public PolyCatClient(String catalogHost, int catalogPort) {
        catalogServerUrlPrefix = "http://" + catalogHost + ":" + catalogPort + "/v1/";
    }

    public static PolyCatClient getInstance(Configuration conf) {
        return getInstance(conf, true);
    }

    public static PolyCatClient getInstance(Configuration conf, boolean initContext) {
        // default values
        String catalogHost = conf.get(PolyCatConf.CATALOG_HOST, "127.0.0.1");
        int catalogPort = Integer.parseInt(conf.get(PolyCatConf.CATALOG_PORT, "8082"));
        // use configuration
        String confPath = PolyCatConf.getConfPath(conf);
        if (confPath != null) {
            PolyCatConf polyCatConf = new PolyCatConf(confPath);
            catalogHost = polyCatConf.getCatalogHost();
            catalogPort = polyCatConf.getCatalogPort();
        }
        // user system env variables
        String systemCatalogHost = System.getenv("CATALOG_HOST");
        if (StringUtils.isNotBlank(systemCatalogHost)) {
            catalogHost = systemCatalogHost;
        }
        String systemCatalogPort = System.getenv("CATALOG_PORT");
        if (StringUtils.isNotBlank(systemCatalogPort)) {
            catalogPort = Integer.parseInt(systemCatalogPort);
        }
        PolyCatClient client = new PolyCatClient(catalogHost, catalogPort);

        if (initContext) {
            client.initContext(conf);
        }
        return client;
    }

    private void initContext(Configuration conf) {
        CatalogContext currentUser = CatalogUserInformation.getCurrentUser();
        if (currentUser != null) {
            setContext(currentUser);
        } else {
            setContext(CatalogUserInformation.createContextFromConf(conf));
        }
    }

    @Override
    public void setContext(CatalogContext context) {
        this.context = context;
    }

    @Override
    public CatalogContext getContext() {
        return this.context;
    }

    public String getToken() {
        return context.getToken();
    }

    public String getUserName() {
        return context.getUserName();
    }

    public String getProjectId() {
        return context.getProjectId();
    }

    public void setDefaultCatalog(String defaultCatalog) {
        context.setCurrentCatalogName(defaultCatalog);
    }

    public void setDefaultDatabase(String defaultDatabase) {
        context.setCurrentDatabaseName(defaultDatabase);
    }

    @Override
    public Catalog createCatalog(CreateCatalogRequest request) throws CatalogException {
        String url = Constants.getCatalogUrlPrefix(catalogServerUrlPrefix, request);
        return withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())),
            Catalog.class);
    }

    @Override
    public Database createDatabase(CreateDatabaseRequest request) throws CatalogException {
        String url = Constants.getDatabaseUrlPrefix(catalogServerUrlPrefix, request);
        return withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())),
            Database.class);
    }

    @Override
    public void createShare(CreateShareRequest request) throws CatalogException {
        String url = Constants.getShareUrlPrefix(catalogServerUrlPrefix, request);
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void dropShare(DropShareRequest request) throws CatalogException {
        String url = Constants.getShareUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.QUESTION + Constants.REQ_PARAM_SHARE_NAME + Constants.EQUAL + request.getShareName();
        withCatalogException(() -> deleteWithHeader(url, request.getHeader(getToken())));
    }

    @Override
    public void alterShareAddAccounts(AlterShareRequest request) throws CatalogException {
        String url = Constants.getShareUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getInput().getShareName() + Constants.ADD_ACCOUNTS;
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void alterShareRemoveAccounts(AlterShareRequest request) throws CatalogException {
        String url = Constants.getShareUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getInput().getShareName() + Constants.REMOVE_ACCOUNTS;
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void grantShareToUser(AlterShareRequest request) throws CatalogException {
        String url = Constants.getShareUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getInput().getShareName() + Constants.ADD_USERS;
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void revokeShareFromUser(AlterShareRequest request) throws CatalogException {
        String url = Constants.getShareUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getInput().getShareName() + Constants.REMOVE_USERS;
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void grantPrivilegeToShare(AlterShareRequest request) throws CatalogException {
        String url = Constants.getShareUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getInput().getShareName() + Constants.GRANT_PRIVILEGE;
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void revokePrivilegeFromShare(AlterShareRequest request) throws CatalogException {
        String url = Constants.getShareUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getInput().getShareName() + Constants.REVOKE_PRIVILEGE;
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void alterShare(AlterShareRequest request) throws CatalogException {
        String url = Constants.getShareUrlPrefix(catalogServerUrlPrefix, request);
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public Share getShare(GetShareRequest request) throws CatalogException {
        String url = Constants.getShareUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.QUESTION + Constants.REQ_PARAM_SHARE_NAME + Constants.EQUAL + request.getShareName();
        return withCatalogException(() -> getWithHeader(url, request.getHeader(getToken())), Share.class);
    }

    @Override
    public PagedList<Share> showShares(ShowSharesRequest request) throws CatalogException {
        StringBuilder sb = new StringBuilder(Constants.getShareUrlPrefix(catalogServerUrlPrefix, request))
            .append(Constants.SHOW_SHARES);

        sb.append(Constants.QUESTION)
            .append(Constants.REQ_PARAM_ACCOUNT_ID)
            .append(Constants.EQUAL)
            .append(request.getAccountId());

        sb.append(Constants.AND)
            .append(Constants.REQ_PARAM_USER_ID)
            .append(Constants.EQUAL)
            .append(request.getUserId());

        appendPattern(sb, request.getPattern());

        return CatalogClientHelper.listWithCatalogException(
            () -> getWithHeader(sb.toString(), request.getHeader(getToken())),
            Share.class);
    }

    private void appendPattern(StringBuilder sb, String pattern) {
        if (Objects.nonNull(pattern)) {
            sb.append(Constants.AND)
                .append(Constants.REQ_PARAM_PATTERN)
                .append(Constants.EQUAL)
                .append(pattern);
        }
    }

    @Override
    public void createRole(CreateRoleRequest request) throws CatalogException {
        String url = Constants.getRoleUrlPrefix(catalogServerUrlPrefix, request);
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void dropRole(DropRoleRequest request) throws CatalogException {
        String url = Constants.getRoleUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.QUESTION + Constants.REQ_PARAM_ROLE_NAME + Constants.EQUAL + request.getRoleName();
        withCatalogException(() -> deleteWithHeader(url, request.getHeader(getToken())));
    }

    @Override
    public void alterRole(AlterRoleRequest request) throws CatalogException {
        String url = Constants.getRoleUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH + request.getRoleName();
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void grantRoleToUser(AlterRoleRequest request) throws CatalogException {
        String url = Constants.getRoleUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getInput().getRoleName() + Constants.ADD_USERS;
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void revokeRoleFromUser(AlterRoleRequest request) throws CatalogException {
        String url = Constants.getRoleUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getInput().getRoleName() + Constants.REMOVE_USERS;
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void grantPrivilegeToRole(AlterRoleRequest request) throws CatalogException {
        String url = Constants.getRoleUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getInput().getRoleName() + Constants.GRANT_PRIVILEGE;
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void grantPrivilegeToPrincipal(AlterPrivilegeRequest request) throws CatalogException {
        String urlPrefix = Constants.getPolicyUrlPrefix(catalogServerUrlPrefix, request)
             + Constants.GRANT_PRIVILEGE;
        Map<String, String> params = new HashMap<>();
        params.put(Constants.REQ_PARAM_PRINCIPAL_TYPE, request.getPrincipalType());
        params.put(Constants.REQ_PARAM_PRINCIPAL_SOURCE, request.getPrincipalSource());
        params.put(Constants.REQ_PARAM_PRINCIPAL_NAME, request.getPrincipalName());

        String url = makeReqParamsWithPrefix(urlPrefix, params);
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void revokePrivilegeFromPrincipal(AlterPrivilegeRequest request) throws CatalogException {
        String urlPrefix = Constants.getPolicyUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.REVOKE_PRIVILEGE;
        Map<String, String> params = new HashMap<>();
        params.put(Constants.REQ_PARAM_PRINCIPAL_TYPE, request.getPrincipalType());
        params.put(Constants.REQ_PARAM_PRINCIPAL_SOURCE, request.getPrincipalSource());
        params.put(Constants.REQ_PARAM_PRINCIPAL_NAME, request.getPrincipalName());

        String url = makeReqParamsWithPrefix(urlPrefix, params);
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public PagedList<Policy> showPoliciesOfPrincipal(ShowPoliciesOfPrincipalRequest request) throws CatalogException {
        String urlPrefix = Constants.getPolicyUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SHOW_PRIVILEGE;
        Map<String, String> params = new HashMap<>();
        params.put(Constants.REQ_PARAM_PRINCIPAL_TYPE, request.getPrincipalType());
        params.put(Constants.REQ_PARAM_PRINCIPAL_SOURCE, request.getPrincipalSource());
        params.put(Constants.REQ_PARAM_PRINCIPAL_NAME, request.getPrincipalName());
        String url = makeReqParamsWithPrefix(urlPrefix, params);
        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())),
          Policy.class);
    }

    @Override
    public PagedList<MetaPolicyHistory> getPolicyHistoryByTime(GetPolicyChangeRecordByTimeRequest request) throws CatalogException {
        String urlPrefix = Constants.getPolicyUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.LIST_POLICY_HISTORY;
        Map<String, String> params = new HashMap<>();
        params.put(Constants.REQ_PARAM_POLICY_UPDATE_TIME, String.valueOf(request.getUpdateTime()));
        String url = makeReqParamsWithPrefix(urlPrefix, params);
        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())),
            MetaPolicyHistory.class);
    }

    @Override
    public MetaPrivilegePolicyAggrData getAggrPolicyById(GetPrivilegesByIdRequest request) throws CatalogException{
        String prefixUrl =
            Constants.getPolicyUrlPrefix(catalogServerUrlPrefix, request) + Constants.LIST_PRIVILEGE;
        Map<String, String> params = new HashMap<>();
        params.put(Constants.REQ_PARAM_PRINCIPAL_TYPE, request.getPrincipalType());
        String url = makeReqParamsWithPrefix(prefixUrl, params);

        return withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())),
            MetaPrivilegePolicyAggrData.class);
    }

    @Override
    public void revokePrivilegeFromRole(AlterRoleRequest request) throws CatalogException {
        String url = Constants.getRoleUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getInput().getRoleName() + Constants.REVOKE_PRIVILEGE;
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void grantAllObjectPrivilegeToRole(AlterRoleRequest request) throws CatalogException {
        String url = Constants.getRoleUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getInput().getRoleName() + Constants.GRANT_ALL_OBJECT_PRIVILEGE;
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void revokeAllObjectPrivilegeFromRole(AlterRoleRequest request) throws CatalogException {
        String url = Constants.getRoleUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getInput().getRoleName() + Constants.REVOKE_ALL_OBJECT_PRIVILEGE;
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void createAccelerator(CreateAcceleratorRequest request) throws CatalogException {
        String url = Constants.getAcceleratorUrlPrefix(catalogServerUrlPrefix, request);
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public PagedList<AcceleratorObject> showAccelerator(ShowAcceleratorsRequest request) throws CatalogException {
        String url = Constants.getAcceleratorUrlPrefix(catalogServerUrlPrefix, request);
        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())),
            AcceleratorObject.class);
    }

    @Override
    public void dropAccelerator(DropAcceleratorRequest request) throws CatalogException {
        String url = Constants.getAcceleratorUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getAcceleratorName();
        withCatalogException(() -> deleteWithHeader(url, request.getHeader(getToken())));
    }

    @Override
    public void alterAccelerator(AcceleratorRequestBase<AcceleratorInput> request) {
        String url = Constants.getAcceleratorUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getAcceleratorName() + Constants.COMPILE;
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public Role getRole(GetRoleRequest request) throws CatalogException {
        String url = Constants.getRoleUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.QUESTION + Constants.REQ_PARAM_ROLE_NAME + Constants.EQUAL + request.getRoleName();
        return withCatalogException(() -> getWithHeader(url, request.getHeader(getToken())), Role.class);
    }

    @Override
    public PagedList<Role> showRoles(ShowRolesRequest request) throws CatalogException {
        StringBuilder sb = new StringBuilder(Constants.getRoleUrlPrefix(catalogServerUrlPrefix, request))
            .append(Constants.SHOW_ROLES);
        sb.append(Constants.QUESTION);
        if (StringUtils.isNotEmpty(request.getUserId())) {
            sb.append(Constants.REQ_PARAM_USER_ID)
                    .append(Constants.EQUAL)
                    .append(request.getUserId());
        }
        if (StringUtils.isNotEmpty(request.getPattern())) {
            appendPattern(sb, request.getPattern());
        }

        return CatalogClientHelper.listWithCatalogException(
                () -> getWithHeader(sb.toString(), request.getHeader(getToken())),
                Role.class);
    }

    @Override
    public PagedList<Role> showAllRoleName(ShowRoleNamesRequest request) throws CatalogException {
        StringBuilder sb = new StringBuilder(Constants.getRoleUrlPrefix(catalogServerUrlPrefix, request))
                .append(Constants.SHOW_ALL_ROLE_NAME);
        sb.append(Constants.QUESTION);
        appendUrlParam(sb, request.getKeyword(), Constants.REQ_PARAM_KEYWORD);
        return CatalogClientHelper.listWithCatalogException(
                () -> getWithHeader(sb.toString(), request.getHeader(getToken())),
                Role.class);
    }

    private void appendUrlParam(StringBuilder sb, String keyword, String reqParamKey) {
        if (StringUtils.isNotEmpty(keyword)) {
            sb.append(reqParamKey)
                    .append(Constants.EQUAL)
                    .append(urlParamEncode(keyword))
                    .append(Constants.AND);
        }
    }

    @Override
    public PagedList<String> showPermObjectsByUser(ShowPermObjectsRequest request) throws CatalogException {
        StringBuilder sb = new StringBuilder(Constants.getRoleUrlPrefix(catalogServerUrlPrefix, request))
                .append(Constants.SHOW_PERM_OBJECTS_BY_USER);
        sb.append(Constants.QUESTION);
        appendUrlParam(sb, request.getUserId(), Constants.REQ_PARAM_USER_ID);
        appendUrlParam(sb, request.getObjectType(), Constants.REQ_PARAM_OBJECT_TYPE);
        appendUrlParam(sb, request.getFilter(), Constants.REQ_PARAM_FILTER);
        return CatalogClientHelper.listWithCatalogException(
                () -> getWithHeader(sb.toString(), request.getHeader(getToken())),
                String.class);
    }

    @Override
    public Role showGrantsToRole(ShowGrantsToRoleRequest request) throws CatalogException {
        String url = Constants.getRoleUrlPrefix(catalogServerUrlPrefix, request)
                + Constants.QUESTION + Constants.REQ_PARAM_ROLE_NAME + Constants.EQUAL + request.getRoleName();
        return withCatalogException(() -> getWithHeader(url, request.getHeader(getToken())), Role.class);
    }

    @Override
    public AuthorizationResponse authenticate(AuthenticationRequest request) throws CatalogException {
        String url = catalogServerUrlPrefix + request.getProjectId() + Constants.AUTHENTICATION
            + Constants.QUESTION + Constants.REQ_PARAM_IGNORE_UNKNOWN_OBJ + Constants.EQUAL +
            request.getIgnoreUnknownObj();
        return withCatalogException(
            () -> postWithHeader(url, request.getAuthorizationInputList(), request.getHeader(getToken())),
            AuthorizationResponse.class);
    }

    /**
     * alter database
     */
    @Override
    public void alterDatabase(AlterDatabaseRequest request) throws CatalogException {
        String urlPrefix = Constants.getDatabaseUrlPrefix(catalogServerUrlPrefix, request);

        Map<String, String> params = new HashMap<>();
        params.put(Constants.REQ_PARAM_DATABASE_NAME, request.getDatabaseName());
        String url = makeReqParamsWithPrefix(urlPrefix, params);
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void alterCatalog(AlterCatalogRequest request) {
        RequestChecker.check(request);
        String url = Constants.getCatalogUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getCatalogName();
        withCatalogException(() -> putWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public Catalog getCatalog(GetCatalogRequest request) throws CatalogException {
        RequestChecker.check(request);
        String url = catalogServerUrlPrefix + request.getProjectId() + Constants.CATALOGS
            + Constants.SLASH + request.getCatalogName();
        return withCatalogException(() -> getWithHeader(url, request.getHeader(getToken())), Catalog.class);
    }

    @Override
    public Database getDatabase(GetDatabaseRequest request) throws CatalogException {
        String url = Constants.getDatabaseUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.GET_DATABASE_BY_NAME + Constants.QUESTION
            + Constants.REQ_PARAM_DATABASE_NAME + Constants.EQUAL + request.getDatabaseName();
        return withCatalogException(() -> getWithHeader(url, request.getHeader(getToken())), Database.class);
    }

    @Override
    public void dropCatalog(DropCatalogRequest request) throws CatalogException {
        RequestChecker.check(request);
        String url = Constants.getCatalogUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getCatalogName();

        withCatalogException(() -> deleteWithHeader(url, request.getHeader(getToken())));
    }

    @Override
    public PagedList<Catalog> listCatalogs(ListCatalogsRequest request) throws CatalogException {
        Map<String, String> params = request.getParams();
        RequestChecker.check(request, params);
        String url = makeReqParamsWithPrefix(Constants.getCatalogUrlPrefix(catalogServerUrlPrefix, request), params);
        return CatalogClientHelper.listWithCatalogException(
            () -> getWithHeader(url, request.getHeader(getToken())),
            Catalog.class);
    }

    @Override
    public Catalog createBranch(CreateBranchRequest request) {
        Catalog catalog = getCatalog(new GetCatalogRequest(request.getProjectId(), request.getCatalogName()));
        CreateCatalogRequest createCatalogRequest = new CreateCatalogRequest();
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setCatalogName(request.getBranchName());
        catalogInput.setParentName(catalog.getCatalogName());
        catalogInput.setOwner(request.getUserId());
        if (null != (request.getVersion())) {
            catalogInput.setParentVersion(request.getVersion());
        }
        createCatalogRequest.setProjectId(request.getProjectId());
        createCatalogRequest.setInput(catalogInput);
        return createCatalog(createCatalogRequest);
    }

    @Override
    public PagedList<Catalog> listBranch(ListBranchesRequest request) throws CatalogException {
        String url = Constants.getCatalogUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getCatalogName() + Constants.BRANCHS;
        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())),
            Catalog.class);
    }

    @Override
    public void mergeBranch(MergeBranchRequest request) {
        RequestChecker.check(request);
        String url = Constants.getCatalogUrlPrefix(catalogServerUrlPrefix, request) + Constants.MERGE;
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void deleteDatabase(DeleteDatabaseRequest request) throws CatalogException {
        String urlPrefix = Constants.getDatabaseUrlPrefix(catalogServerUrlPrefix, request);

        Map<String, String> params = new HashMap<>();
        params.put(Constants.REQ_PARAM_DATABASE_NAME, request.getDatabaseName());
        params.put(Constants.REQ_PARAM_IGNORE_UNKNOWN_OBJ, request.getIgnoreUnknownObj().toString());
        params.put(Constants.CASCADE, request.getCascade().toString());
        String url = makeReqParamsWithPrefix(urlPrefix, params);

        withCatalogException(() -> deleteWithHeader(url, request.getHeader(getToken())));
    }

    @Override
    public PagedList<Database> listDatabases(ListDatabasesRequest request) throws CatalogException {

        String urlPrefix = Constants.getDatabaseUrlPrefix(catalogServerUrlPrefix, request) + Constants.LIST;

        Map<String, String> params = new HashMap<>();
        if (request.isIncludeDrop()) {
            params.put(Constants.REQ_PARAM_INCLUDE_DROP, String.valueOf(request.isIncludeDrop()));
        }
        if (request.getMaxResults() != null) {
            params.put(Constants.REQ_PARAM_MAX_RESULTS, String.valueOf(request.getMaxResults()));
        }
        if (request.getPageToken() != null) {
            params.put(Constants.REQ_PARAM_PAGE_TOKEN, request.getPageToken());
        }
        if (request.getFilter() != null) {
            params.put(Constants.REQ_PARAM_FILTER, request.getFilter());
        }
        String url = makeReqParamsWithPrefix(urlPrefix, params);

        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())),
            Database.class);
    }

    @Override
    public PagedList<String> getDatabases(ListDatabasesRequest request) throws CatalogException {
        String urlPrefix = Constants.getDatabaseUrlPrefix(catalogServerUrlPrefix, request) + Constants.NAMES;

        Map<String, String> params = new HashMap<>();
        if (request.isIncludeDrop()) {
            params.put(Constants.REQ_PARAM_INCLUDE_DROP, String.valueOf(request.isIncludeDrop()));
        }
        if (request.getMaxResults() != null) {
            params.put(Constants.REQ_PARAM_MAX_RESULTS, String.valueOf(request.getMaxResults()));
        }
        if (request.getPageToken() != null) {
            params.put(Constants.REQ_PARAM_PAGE_TOKEN, request.getPageToken());
        }
        if (request.getFilter() != null) {
            params.put(Constants.REQ_PARAM_PATTERN, request.getFilter());
        }
        String url = makeReqParamsWithPrefix(urlPrefix, params);

        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())),
            String.class);
    }

    @Override
    public void undropCatalog(UndropCatalogRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PagedList<TableCommit> listTableCommits(ListTableCommitsRequest request)
        throws CatalogException {

        StringBuilder sb = new StringBuilder(Constants.getTableUrlPrefix(catalogServerUrlPrefix, request))
            .append(Constants.SLASH)
            .append(request.getTableName())
            .append(Constants.LIST_TABLE_COMMITS);

        if (Objects.nonNull(request.getPageToken())) {
            sb.append(Constants.AND)
                .append(Constants.REQ_PARAM_PAGE_TOKEN)
                .append(Constants.EQUAL)
                .append(request.getPageToken());
        }

        if (request.getLimits() > 0) {
            sb.append(Constants.AND)
                .append(Constants.REQ_PARAM_MAX_RESULTS)
                .append(Constants.EQUAL)
                .append(request.getLimits());
        }
        return CatalogClientHelper.listWithCatalogException(
            () -> getWithHeader(sb.toString(), request.getHeader(getToken())),
            TableCommit.class);
    }

    @Override
    public void undropDatabase(UndropDatabaseRequest undropDatabaseRequest) throws CatalogException {
        String urlPrefix = Constants.getDatabaseUrlPrefix(catalogServerUrlPrefix, undropDatabaseRequest)
            + Constants.SLASH + undropDatabaseRequest.getDatabaseName() + Constants.UNDROP;

        Map<String, String> params = new HashMap<>();
        if (undropDatabaseRequest.getDatabaseId() != null) {
            params.put(Constants.DATABASE_ID, undropDatabaseRequest.getDatabaseId());
        }
        if (undropDatabaseRequest.getRename() != null) {
            params.put(Constants.RENAME, undropDatabaseRequest.getRename());
        }
        String url = makeReqParamsWithPrefix(urlPrefix, params);
        withCatalogException(() -> putWithHeader(url, undropDatabaseRequest.getHeader(getToken())));
    }

    @Override
    public PagedList<TableBrief> getTableMeta(GetTableMetaRequest request) throws CatalogException {
        String urlPrefix = Constants.getCatalogUrlPrefix(catalogServerUrlPrefix, request);
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())),
            TableBrief.class);
    }

    @Override
    public void undropTable(UndropTableRequest request) throws CatalogException {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH
            + request.getTableName() + Constants.UNDROP;

        Map<String, String> params = new HashMap<>();
        if (request.getTableId() != null) {
            params.put(Constants.TABLE_ID, request.getTableId());
        }
        if (request.getRename() != null) {
            params.put(Constants.RENAME, request.getRename());
        }
        String url = makeReqParamsWithPrefix(urlPrefix, params);
        withCatalogException(() -> putWithHeader(url, request.getHeader(getToken())));
    }

    @Override
    public void alterColumn(AlterColumnRequest request) throws CatalogException {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH
            + request.getTableName() + Constants.ALTER_COLUMN;
        withCatalogException(() -> patchWithHeader(urlPrefix, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public Table createTable(CreateTableRequest request) throws CatalogException {
        String url = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request);
        return withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())),
            Table.class);
    }

    @Override
    public void createTableWithLocation(CreateTableWithInsertRequest insertSegmentRequest) throws CatalogException {
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setDatabaseName(insertSegmentRequest.getDatabaseName());
        createTableRequest.setProjectId(insertSegmentRequest.getProjectId());
        createTableRequest.setCatalogName(insertSegmentRequest.getCatalogName());
        createTableRequest.setInput(insertSegmentRequest.getInput().getTableInput());
        createTable(createTableRequest);
        AddPartitionRequest insertSegmentRequest1 = new AddPartitionRequest();
        insertSegmentRequest1.setInput(insertSegmentRequest.getInput().getPartitionInput());
        insertSegmentRequest1.setDatabaseName(insertSegmentRequest.getDatabaseName());
        insertSegmentRequest1.setProjectId(insertSegmentRequest.getProjectId());
        insertSegmentRequest1.setCatalogName(insertSegmentRequest.getCatalogName());
        addPartition(insertSegmentRequest1);
    }

    @Override
    public void batchDeleteTable(BatchDeleteTableRequest batchDeleteTableRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteTable(DeleteTableRequest deleteTableRequest) throws CatalogException {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, deleteTableRequest)
            + Constants.SLASH + deleteTableRequest.getTableName();
        Map<String, String> params = new HashMap<>();
        if (deleteTableRequest.isPurgeFlag()) {
            params.put(Constants.PURGE, Boolean.TRUE.toString());
        }

        if (deleteTableRequest.isDeleteData()) {
            params.put(Constants.DELETE_DATA, Boolean.TRUE.toString());
        }

        if (deleteTableRequest.isIgnoreUnknownObject()) {
            params.put(Constants.REQ_PARAM_IGNORE_UNKNOWN_OBJ, Boolean.TRUE.toString());
        }

        String url = makeReqParamsWithPrefix(urlPrefix, params);
        withCatalogException(() -> deleteWithHeader(url, deleteTableRequest.getHeader(getToken())));
    }

    @Override
    public void purgeTable(PurgeTableRequest request) throws CatalogException {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getTableName() + Constants.SLASH + Constants.PURGE;
        Map<String, String> params = new HashMap<>();
        if (request.getTableId() != null) {
            params.put(Constants.TABLE_ID, request.getTableId());
        }

        params.put(Constants.IF_EXIST, request.getExist().toString());

        String url = makeReqParamsWithPrefix(urlPrefix, params);
        withCatalogException(() -> deleteWithHeader(url, request.getHeader(getToken())));
    }

    @Override
    public Table getTable(GetTableRequest getTableRequest) throws CatalogException {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, getTableRequest)
            + Constants.SLASH + getTableRequest.getTableName();

        Map<String, String> params = new HashMap<>();
        params.put(Constants.REQ_PARAM_TABLE_NAME, getTableRequest.getTableName());
        if (getTableRequest.getShareName() != null) {
            params.put(Constants.REQ_PARAM_SHARE_NAME, getTableRequest.getShareName());
        }
        String url = makeReqParamsWithPrefix(urlPrefix, params);

        return withCatalogException(() -> getWithHeader(url, getTableRequest.getHeader(getToken())), Table.class);
    }

    @Override
    public TableCommit getTableVersions(GetTableVersionsRequest request) throws CatalogException {
        String url = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getTableName() + Constants.LATEST_VERSION;
        return withCatalogException(() -> getWithHeader(url, request.getHeader(getToken())), TableCommit.class);
    }

    @Override
    public PagedList<Table> listTables(ListTablesRequest request) throws CatalogException {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request);
        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(getListTableUrl(request, urlPrefix), request.getHeader(getToken())),
            Table.class);
    }

    private String getListTableUrl(ListTablesRequest request, String urlPrefix) {
        Map<String, String> params = new HashMap<>();
        if (request.getIncludeDrop() != null) {
            params.put(Constants.REQ_PARAM_INCLUDE_DROP, request.getIncludeDrop().toString());
        }
        if (request.getMaxResults() != null) {
            params.put(Constants.REQ_PARAM_MAX_RESULTS, request.getMaxResults().toString());
        }
        if (request.getNextToken() != null) {
            params.put(Constants.REQ_PARAM_PAGE_TOKEN, request.getNextToken());
        }
        if (request.getExpression() != null) {
            params.put(Constants.REQ_PARAM_FILTER, urlParamEncode(request.getExpression()));
        }
        return makeReqParamsWithPrefix(urlPrefix, params);
    }

    private String urlParamEncode(String value) {
        if (value == null) {
            return value;
        }
        try {
            return java.net.URLEncoder.encode(value, "utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return value;
    }

    @Override
    public PagedList<String> listTableNames(ListTablesRequest request) throws CatalogException {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request) + Constants.LIST_TABLE_NAMES;
        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(getListTableUrl(request, urlPrefix), request.getHeader(getToken())),
                String.class);
    }

    @Override
    public PagedList<String> getTableNames(GetTableNamesRequest request) {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request) + Constants.NAMES;
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParamsMap());
        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())),
            String.class);
    }

    private String makeReqParamsWithPrefix(String urlPrefix, Map<String, String> params) {
        if (params == null) {
            return urlPrefix;
        }
        StringBuilder urlSB = new StringBuilder(urlPrefix);
        urlSB.append(Constants.QUESTION);
        for (Map.Entry<String, String> entry : params.entrySet()) {
            urlSB.append(entry.getKey()).append(Constants.EQUAL).append(entry.getValue())
                .append(Constants.AND);
        }
        urlSB.deleteCharAt(urlSB.length() - 1);
        return urlSB.toString();
    }

    @Override
    public void alterTable(AlterTableRequest request) throws CatalogException {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getTableName();
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void addPartition(AddPartitionRequest request) throws CatalogException {
        // controller: addPartition()
        String url = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + "/add";
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public List<Partition> addPartitions(AddPartitionRequest request) throws CatalogException {
        // controller: addPartitions()  -- Unsupported
        String url = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + "?operate=addParts";
        Partition[] partitions = withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())),
            Partition[].class);
        return Objects.isNull(partitions) ? null : Arrays.asList(partitions);
    }

    @Override
    public boolean doesPartitionExist(DoesPartitionExistsRequest request) throws CatalogException {
        // controller:doesPartitionExists()
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + "/doesExists";
        return withCatalogException(() -> postWithHeader(urlPrefix, request.getInput(), request.getHeader(getToken())),
            Boolean.class);
    }

    @Override
    public void alterPartitions(AlterPartitionRequest request) throws CatalogException {
        //controller: alterPartitions()
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + "/alterParts";
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void dropPartition(DropPartitionRequest request) throws CatalogException {
        //controller: dropPartitionByName()
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + "/dropByName";
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void truncatePartition(TruncatePartitionRequest request) throws CatalogException {
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request);
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }



    @Override
    public PagedList<Partition> showTablePartitions(ListTablePartitionsRequest request)
        throws CatalogException {
        String url = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + Constants.SHOW_PARTITIONS;
        return CatalogClientHelper.listWithCatalogException(() ->
            postWithHeader(url, request.getInput(), request.getHeader(getToken())), Partition.class);
    }

    @Override
    public List<Partition> getPartitionsByFilter(ListPartitionByFilterRequest request)
        throws CatalogException {
        //controller: getPartitionsByFilter()
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + "/getByFilter";
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        Partition[] partitions = withCatalogException(() -> postWithHeader(url, request.getInput(),
            request.getHeader(getToken())), Partition[].class);
        return Arrays.asList(partitions);
    }

    @Override
    public List<Partition> getPartitionsByNames(GetTablePartitionsByNamesRequest request)
        throws CatalogException {
        //controller: getPartitionsByNames()
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + "/getPartitionsByNames";
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        Partition[] partitions = withCatalogException(() -> postWithHeader(url, request.getInput(),
            request.getHeader(getToken())), Partition[].class);
        return Arrays.asList(partitions);
    }

    @Override
    public Partition getPartitionWithAuth(GetPartitionWithAuthRequest request)
        throws CatalogException {
        //controller: getPartitionWithAuth()
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + "/getWithAuth";
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        return withCatalogException(() -> postWithHeader(url, request.getInput(),
            request.getHeader(getToken())), Partition.class);
    }

    @Override
    public List<String> listPartitionNames(ListTablePartitionsRequest request)
        throws CatalogException {
        //controller: listPartitionNames()
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + "/listNames";
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        String[] partitionNames = withCatalogException(() -> postWithHeader(url, request.getInput(),
            request.getHeader(getToken())), String[].class);
        return Arrays.asList(partitionNames);
    }

    @Override
    public List<String> listPartitionNamesByFilter(ListPartitionNamesByFilterRequest request)
        throws CatalogException {
        //controller: listPartitionNamesByFilter()
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + "/listNamesByFilter";
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        String[] partitionNames = withCatalogException(() -> postWithHeader(url, request.getInput(),
            request.getHeader(getToken())), String[].class);
        return Arrays.asList(partitionNames);
    }

    @Override
    public List<Partition> listPartitionsPsWithAuth(ListPartitionsWithAuthRequest request) throws CatalogException {
        //controller: listPartitionsPsWithAuth()
        String url = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) +
            Constants.LIST_PARTITIONS_PS_WITH_AUTH;
//        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request);
//        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        Partition[] partitions = withCatalogException(() -> postWithHeader(url, request.getInput(),
            request.getHeader(getToken())), Partition[].class);
        return Arrays.asList(partitions);
    }

    @Override
    public List<String> listPartitionNamesPs(ListPartitionNamesPsRequest request)
        throws CatalogException {
        //controller: listPartitionNamesPs()
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + "/listPartitionNamesPs";
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        String[] partitionNames = withCatalogException(() -> postWithHeader(url, request.getInput(),
            request.getHeader(getToken())), String[].class);
        return Arrays.asList(partitionNames);
    }

    @Override
    public List<Partition> listPartitions(ListFileRequest request) throws CatalogException {
        //controller: TableController.listPartitions() expressionGson不生效
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH
            + request.getTableName() + Constants.LIST_PARTITIONS;
        Map<String, String> params = new HashMap<>();
        if (request.getShareName() != null) {
            params.put(Constants.REQ_PARAM_SHARE_NAME, request.getShareName());
        }
        String url = makeReqParamsWithPrefix(urlPrefix, params);
        Partition[] partitions = withCatalogException(() -> postWithHeader(url, request.getInput(),
            request.getHeader(getToken())), Partition[].class);
        return Arrays.asList(partitions);
    }

    @Override
    public List<Partition> listPartitionPs(ListTablePartitionsRequest request) {
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request);
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        Partition[] partitions = withCatalogException(() -> postWithHeader(url, request.getInput(),
            request.getHeader(getToken())), Partition[].class);
        return Arrays.asList(partitions);
    }

    @Override
    public List<String> listFiles(ListFileRequest request) throws CatalogException {
        String url = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH
            + request.getTableName() + Constants.LIST_PARTITIONS;
        Partition[] partitions = withCatalogException(
            () -> postWithHeader(url, request.getInput(), request.getHeader(getToken())),
            Partition[].class);
        return Arrays.stream(partitions)
            .flatMap(x -> {
                if (StringUtils.isEmpty(x.getFileIndexUrl())) {
                    return x.getDataFiles().stream().map(DataFile::getFileName);
                } else {
                    return Stream.of(x.getFileIndexUrl());
                }
            })
            .collect(toList());
    }

    @Override
    public Partition getPartition(GetPartitionRequest request) throws CatalogException {
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + "/getByName";
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        return withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())),
            Partition.class);
    }

    @Override
    public Partition appendPartition(AppendPartitionRequest request) {
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request);
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        return withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())),
            Partition.class);
    }

    @Override
    public void renamePartition(RenamePartitionRequest request) {
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request);
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public Partition[] dropPartitionsByExpr(DropPartitionsRequest request) {
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request);
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        return withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())),
            Partition[].class);
    }

    @Override
    public TableStats getTableStats(GetTableStatRequest request) {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getTableName() + Constants.STAT;

        Map<String, String> params = new HashMap<>();
        if (request.getShareName() != null) {
            params.put(Constants.REQ_PARAM_SHARE_NAME, request.getShareName());
        }
        String url = makeReqParamsWithPrefix(urlPrefix, params);

        return withCatalogException(() -> getWithHeader(url, request.getHeader(getToken())), TableStats.class);
    }

    @Override
    public MetaObjectName getObjectFromNameMap(GetObjectMapRequest request) {
        String urlPrefix = catalogServerUrlPrefix + request.getProjectId()
            + Constants.OBJECT_NAME_MAP
            + Constants.GET_OBJECT;

        Map<String, String> params = new HashMap<>();
        params.put(Constants.REQ_PARAM_OBJECT_TYPE, String.valueOf(request.getObjectType()));
        params.put(Constants.REQ_PARAM_DATABASE_NAME, request.getDatabaseName());
        params.put(Constants.REQ_PARAM_OBJECT_NAME, request.getObjectName());

        String url = makeReqParamsWithPrefix(urlPrefix, params);

        return withCatalogException(() -> getWithHeader(url, request.getHeader(getToken())), MetaObjectName.class);
    }

    @Override
    public PagedList<Table> getTableObjectsByName(GetTableObjectsByNameRequest request) throws CatalogException {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request);
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())),
            Table.class);
    }

    @Override
    public void truncateTable(TruncateTableRequest request) {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getTableName();
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public PolyCatProfile getPolyCatProfile(GetPolyCatProfileRequest request) {
        String url = Constants.getPolyCatProfileUrlPrefix(catalogServerUrlPrefix, request);
        return withCatalogException(() -> getWithHeader(url, request.getHeader(getToken())), PolyCatProfile.class);
    }

    @Override
    public List<Partition> listPartitionNamesByExpr(ListPartitionsByExprRequest request) {
        String urlPrefix = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request);
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        return Arrays.asList(withCatalogException(
            () -> postWithHeader(url, request.getInput(), request.getHeader(getToken())), Partition[].class));
    }

    @Override
    public PagedList<CatalogCommit> listCatalogCommits(ListCatalogCommitsRequest request) {
        Map<String, String> params = request.getParams();
        RequestChecker.check(request, params);
        String url = makeReqParamsWithPrefix(Constants.getCatalogUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getCatalogName() + Constants.COMMIT_LOGS
            , params);
        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())),
            CatalogCommit.class);
    }

    @Override
    public void restoreCatalog(RestoreCatalogRequest restoreCatalogRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void restoreDatabase(RestoreDatabaseRequest restoreDatabaseRequest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void restoreTable(RestoreTableRequest restoreTableRequest) throws CatalogException {
        String url = Constants.getTableUrlPrefix(catalogServerUrlPrefix, restoreTableRequest)
            + Constants.SLASH + restoreTableRequest.getTableName() + Constants.RESTORE;
        if (!StringUtils.isBlank(restoreTableRequest.getVersion())) {
            url += Constants.QUESTION + Constants.REQ_PARAM_VERSION + Constants.EQUAL + restoreTableRequest
                .getVersion();
        }
        String finalUrl = url;
        withCatalogException(() -> putWithHeader(finalUrl, restoreTableRequest.getHeader(getToken())));
    }

    @Override
    public DelegateOutput createDelegate(CreateDelegateRequest request) throws CatalogException {
        String url = catalogServerUrlPrefix + request.getProjectId() + Constants.DELEGATES;
        return withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())),
            DelegateOutput.class);
    }

    @Override
    public DelegateOutput getDelegate(GetDelegateRequest request) throws CatalogException {
        String url = catalogServerUrlPrefix + request.getProjectId() + Constants.DELEGATES
            + Constants.QUESTION + Constants.REQ_PARAM_DELEGATE + Constants.EQUAL + request.getDelegateName();
        return withCatalogException(() -> getWithHeader(url, request.getHeader(getToken())), DelegateOutput.class);
    }

    @Override
    public PagedList<DelegateBriefInfo> listDelegates(ListDelegatesRequest request) throws CatalogException {
        String url = catalogServerUrlPrefix + request.getProjectId() + Constants.DELEGATES + Constants.LIST;
        if (request.getPattern() != null) {
            url += Constants.QUESTION + Constants.REQ_PARAM_PATTERN + Constants.EQUAL + request.getPattern();
        }
        String finalUrl = url;
        return CatalogClientHelper.listWithCatalogException(
            () -> getWithHeader(finalUrl, request.getHeader(getToken())),
            DelegateBriefInfo.class);
    }

    @Override
    public void deleteDelegate(DeleteDelegateRequest request) throws CatalogException {
        String url = catalogServerUrlPrefix + request.getProjectId() + Constants.DELEGATES
            + Constants.QUESTION + Constants.REQ_PARAM_DELEGATE + Constants.EQUAL + request.getDelegateName();
        withCatalogException(() -> deleteWithHeader(url, request.getHeader(getToken())));
    }

    @Override
    public void insertUsageProfile(InsertUsageProfileRequest request) throws CatalogException {
        String url = Constants.getUsageProfileUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.RECORD_TABLE_USAGE_PROFILE;
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public PagedList<TableUsageProfile> listCatalogUsageProfiles(ListCatalogUsageProfilesRequest request)
        throws CatalogException {
        String url =
            Constants.getUsageProfileUrlPrefix(catalogServerUrlPrefix, request)
                + Constants.GET_TOP_USAGE_PROFILES_BY_CATALOG
                + Constants.QUESTION + Constants.REQ_PARAM_CATALOG_NAME + Constants.EQUAL + request.getCatalogName()
                + Constants.AND + Constants.REQ_PARAM_OP_TYPES + Constants.EQUAL + request.getOpType()
                + Constants.AND + Constants.REQ_PARAM_TOP_TYPE + Constants.EQUAL + request.getUsageProfileType();
        if (request.getStartTimestamp() != 0) {
            url += Constants.AND + Constants.REQ_PARAM_START_TIME + Constants.EQUAL + request.getStartTimestamp();
        }
        if (request.getEndTimestamp() != 0) {
            url += Constants.AND + Constants.REQ_PARAM_END_TIME + Constants.EQUAL + request.getEndTimestamp();
        }
        if (request.getLimit() != -1) {
            url += Constants.AND + Constants.REQ_PARAM_TOP_NUM + Constants.EQUAL + request.getLimit();
        }
        if (StringUtils.isNotEmpty(request.getUserId())) {
            url += Constants.AND + Constants.REQ_PARAM_USER_ID + Constants.EQUAL + request.getUserId();
        }
        if (StringUtils.isNotEmpty(request.getTaskId())) {
            url += Constants.AND + Constants.REQ_PARAM_TASK_ID + Constants.EQUAL + request.getTaskId();
        }
        String finalUrl = url;
        return CatalogClientHelper.listWithCatalogException(
            () -> getWithHeader(finalUrl, request.getHeader(getToken())),
            TableUsageProfile.class);
    }

    @Override
    public PagedList<TableUsageProfile> getTableUsageProfile(GetTableUsageProfileRequest request)
        throws CatalogException {
        String url =
            Constants.getUsageProfileUrlPrefix(catalogServerUrlPrefix, request) + Constants.GET_USAGE_PROFILES_BY_TABLE
                + Constants.QUESTION + Constants.REQ_PARAM_CATALOG_NAME + Constants.EQUAL + request.getCatalogName()
                + Constants.AND + Constants.REQ_PARAM_DATABASE_NAME + Constants.EQUAL + request.getDatabaseName()
                + Constants.AND + Constants.REQ_PARAM_TABLE_NAME + Constants.EQUAL + request.getTableName();
        if (request.getStartTimestamp() != 0) {
            url += Constants.AND + Constants.REQ_PARAM_START_TIME + Constants.EQUAL + request.getStartTimestamp();
        }
        if (request.getEndTimestamp() != 0) {
            url += Constants.AND + Constants.REQ_PARAM_END_TIME + Constants.EQUAL + request.getEndTimestamp();
        }
        if (StringUtils.isNotEmpty(request.getUserId())) {
            url += Constants.AND + Constants.REQ_PARAM_USER_ID + Constants.EQUAL + request.getUserId();
        }
        if (StringUtils.isNotEmpty(request.getTaskId())) {
            url += Constants.AND + Constants.REQ_PARAM_TASK_ID + Constants.EQUAL + request.getTaskId();
        }
        String finalUrl = url;
        return CatalogClientHelper.listWithCatalogException(
            () -> getWithHeader(finalUrl, request.getHeader(getToken())),
            TableUsageProfile.class);
    }

    @Override
    public List<TableUsageProfile> getUsageProfileDetails(GetUsageProfileDetailsRequest request) throws CatalogException {
        String urlPrefix = Constants.getUsageProfileUrlPrefix(catalogServerUrlPrefix, request)
                + Constants.GET_USAGE_PROFILE_DETAILS;

        Map<String, String> params = new HashMap<>();
        TableSource tableSource = request.getTableSource();
        if (!Objects.isNull(tableSource) && StringUtils.isNotEmpty(tableSource.getCatalogName())) {
            params.put(Constants.REQ_PARAM_CATALOG_NAME, tableSource.getCatalogName());
        }
        if (!Objects.isNull(tableSource) && StringUtils.isNotEmpty(tableSource.getDatabaseName())) {
            params.put(Constants.REQ_PARAM_DATABASE_NAME, tableSource.getDatabaseName());
        }
        if (!Objects.isNull(tableSource) && StringUtils.isNotEmpty(tableSource.getTableName())) {
            params.put(Constants.REQ_PARAM_TABLE_NAME, tableSource.getTableName());
        }
        if (request.getRowCount() != 0) {
            params.put(Constants.REQ_PARAM_ROW_COUNT, String.valueOf(request.getRowCount()));
        }

        if (request.getStartTimestamp() != 0L) {
            params.put(Constants.REQ_PARAM_START_TIME, String.valueOf(request.getStartTimestamp()));
        }
        if (request.getEndTimestamp() != 0L) {
            params.put(Constants.REQ_PARAM_END_TIME, String.valueOf(request.getEndTimestamp()));
        }

        if (request.getTaskId() != null) {
            params.put(Constants.REQ_PARAM_TASK_ID, request.getTaskId());
        }
        if (StringUtils.isNotEmpty(request.getUserId())) {
            params.put(Constants.REQ_PARAM_USER_ID, request.getUserId());
        }
        String url = makeReqParamsWithPrefix(urlPrefix, params);

        TableUsageProfile[] tableUsageProfiles = withCatalogException(() -> getWithHeader(url, request.getHeader(getToken())), TableUsageProfile[].class);
        return Arrays.asList(tableUsageProfiles);
    }

    @Override
    public List<TableAccessUsers> getTableAccessUsers(GetTableAccessUsersRequest request) throws CatalogException {
        String url = Constants.getUsageProfileUrlPrefix(catalogServerUrlPrefix, request)
                + Constants.GET_TABLE_ACCESS_USERS;
        TableAccessUsers[] tableAccessUsersVOS = withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())), TableAccessUsers[].class);
        return Arrays.asList(tableAccessUsersVOS);
    }

    @Override
    public List<TableUsageProfile> getUsageProfileGroupByUser(GetUsageProfilesGroupByUserRequest request) throws CatalogException {
        String urlPrefix = Constants.getUsageProfileUrlPrefix(catalogServerUrlPrefix, request)
                + Constants.GET_USAGE_PROFILE_GROUP_BY_USER;

        Map<String, String> params = new HashMap<>();
        params.put(Constants.REQ_PARAM_CATALOG_NAME, request.getInput().getCatalogName());
        params.put(Constants.REQ_PARAM_DATABASE_NAME, request.getInput().getDatabaseName());
        params.put(Constants.REQ_PARAM_TABLE_NAME, request.getInput().getTableName());
        if (!StringUtils.isEmpty(request.getOpType())) {
            params.put(Constants.REQ_PARAM_OP_TYPES, request.getOpType());
        }
        if (request.getStartTimestamp() != 0L) {
            params.put(Constants.REQ_PARAM_START_TIME, String.valueOf(request.getStartTimestamp()));
        }
        if (request.getEndTimestamp() != 0L) {
            params.put(Constants.REQ_PARAM_END_TIME, String.valueOf(request.getEndTimestamp()));
        }
        String url = makeReqParamsWithPrefix(urlPrefix, params);

        TableUsageProfile[] tableUsageProfiles = withCatalogException(() -> getWithHeader(url, request.getHeader(getToken())), TableUsageProfile[].class);
        return Arrays.asList(tableUsageProfiles);
    }


    @Override
    public void insertDataLineage(InsertDataLineageRequest request) throws CatalogException {
        String url = Constants.getDataLineageUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.RECORD_DATA_LINEAGE;
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public PagedList<DataLineage> listDataLineages(ListDataLineageRequest request)
        throws CatalogException {
        String url =
            Constants.getDataLineageUrlPrefix(catalogServerUrlPrefix, request) + Constants.GET_DATA_LINEAGES_BY_TABLE
                + Constants.QUESTION + Constants.REQ_PARAM_CATALOG_NAME + Constants.EQUAL + request.getCatalogName()
                + Constants.AND + Constants.REQ_PARAM_DATABASE_NAME + Constants.EQUAL + request.getDatabaseName()
                + Constants.AND + Constants.REQ_PARAM_TABLE_NAME + Constants.EQUAL + request.getTableName()
                + Constants.AND + Constants.REQ_PARAM_LINEAGE_TYPE + Constants.EQUAL + request.getLineageType();
        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())),
            DataLineage.class);
    }

    @Override
    public void setTableProperty(SetTablePropertyRequest request) throws CatalogException {
        String url = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH
            + request.getTableName() + Constants.SET_PROPERTIES;
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void unsetTableProperty(UnsetTablePropertyRequest request) throws CatalogException {
        String url = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH
            + request.getTableName() + Constants.UNSET_PROPERTIES;
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void createFunction(CreateFunctionRequest request) throws CatalogException {
        String url = Constants.getFunctionUrlPrefix(catalogServerUrlPrefix, request);
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public FunctionInput getFunction(GetFunctionRequest request) throws CatalogException {
        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append(Constants.getFunctionUrlPrefix(catalogServerUrlPrefix, request))
            .append(Constants.SLASH).append(request.getFunctionName());
        return withCatalogException(() -> getWithHeader(urlBuilder.toString(),
            request.getHeader(getToken())), FunctionInput.class);
    }

    @Override
    public void alterFunction(AlterFunctionRequest request) {
        String url = Constants.getFunctionUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH
            + request.getFunctionName();
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public PagedList<String> listFunctions(ListFunctionRequest request) throws CatalogException {
        String urlPrefix = Constants.getFunctionUrlPrefix(catalogServerUrlPrefix, request) + Constants.LIST;
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())),
            String.class);
    }

    @Override
    public PagedList<FunctionInput> getAllFunctions(GetAllFunctionRequest request) throws CatalogException {
        String url = Constants.getFunctionUrlPrefix(catalogServerUrlPrefix, request);
        return CatalogClientHelper.listWithCatalogException(
            () -> getWithHeader(url, request.getHeader(getToken())), FunctionInput.class);
    }

    @Override
    public void dropFunction(FunctionRequestBase request) throws CatalogException {
        String url = Constants.getFunctionUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.QUESTION + Constants.REQ_PARAM_FUNCTION_NAME + Constants.EQUAL + request.getFunctionName();
        withCatalogException(() -> deleteWithHeader(url, request.getHeader(getToken())));
    }

    /**
     * Create materialized view and store metadata in LMS
     * @param request to create mv metadata and store in LMS
     */
    @Override
    public void createMaterializedView(CreateMaterializedViewRequest request)
        throws CatalogException {
        String url = Constants.getMVUrlPrefix(catalogServerUrlPrefix, request);
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    /**
     * Drop materialized view metadata present in LMS
     * @param request to drop the specified mv metadata by name in LMS
     */
    @Override
    public void dropMaterializedView(DropMaterializedViewRequest request) throws CatalogException {
        String urlPrefix =
            Constants.getMVUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH
                + request.getMaterializedViewName();

        Map<String, String> params = new HashMap<>();
        if (request.isHMSTable()) {
            params.put(Constants.REQ_PARAM_HMS_TAB, String.valueOf(request.isHMSTable()));
        }

        String url = makeReqParamsWithPrefix(urlPrefix, params);
        withCatalogException(() -> deleteWithHeader(url, request.getHeader(getToken())));
    }

    /**
     * List all the materialized view information stored in LMS
     * @param request to query materialized view based on base table or list all materialized
     *                views based on maximum results
     * @return list of index information
     */
    @Override
    public PagedList<IndexInfo> listIndexes(ListMaterializedViewsRequest request)
        throws CatalogException {

        String urlPrefix =
            Constants.getMVUrlPrefix(catalogServerUrlPrefix, request) + Constants.LIST;

        Map<String, String> params = new HashMap<>();
        if(request.getTableName() != null) {
            params.put(Constants.REQ_PARAM_TABLE_NAME, request.getTableName());
            if(request.getParentDbName() != null) {
                params.put(Constants.REQ_PARAM_PARENT_DATABASE_NAME, request.getParentDbName());
            } else {
                params.put(Constants.REQ_PARAM_PARENT_DATABASE_NAME, request.getDatabaseName());
            }
        }
        if (request.getIncludeDrop() != null) {
            params.put(Constants.REQ_PARAM_INCLUDE_DROP, String.valueOf(request.getIncludeDrop()));
        }
        if (request.getMaxResults() != null) {
            params.put(Constants.REQ_PARAM_MAX_RESULTS, String.valueOf(request.getMaxResults()));
        }
        if (request.getNextToken() != null) {
            params.put(Constants.REQ_PARAM_PAGE_TOKEN, request.getNextToken());
        }
        if (request.getIsHMSTable() != null) {
            params.put(Constants.REQ_PARAM_HMS_TAB, String.valueOf(request.getIsHMSTable()));
        }
        String url = makeReqParamsWithPrefix(urlPrefix, params);

        return CatalogClientHelper.listWithCatalogException(
            () -> getWithHeader(url, request.getHeader(getToken())), IndexInfo.class);
    }

    /**
     * Return the materialized view information based on name
      * @param getMaterializedViewRequest which has the details of mv name to be queried
     * @return index info
     */
    @Override
    public IndexInfo getMaterializedView(GetMaterializedViewRequest getMaterializedViewRequest)
        throws CatalogException {
        String urlPrefix = Constants.getMVUrlPrefix(catalogServerUrlPrefix, getMaterializedViewRequest)
                + Constants.GET_MV_BY_NAME;

        Map<String, String> params = new HashMap<>();
        params.put(Constants.REQ_PARAM_MV_NAME,
            getMaterializedViewRequest.getMaterializedViewName());
        if (getMaterializedViewRequest.getShareName() != null) {
            params.put(Constants.REQ_PARAM_SHARE_NAME, getMaterializedViewRequest.getShareName());
        }
        String url = makeReqParamsWithPrefix(urlPrefix, params);

        return withCatalogException(
            () -> getWithHeader(url, getMaterializedViewRequest.getHeader(getToken())),
            IndexInfo.class);
    }

    /**
     * Alter the materialized view metadata
     * @param alterMaterializedViewRequest to modify materialized view metadata
     */
    @Override
    public void alterMaterializedView(AlterMaterializedViewRequest alterMaterializedViewRequest)
        throws CatalogException {
        String url =
            Constants.getMVUrlPrefix(catalogServerUrlPrefix, alterMaterializedViewRequest)
                + Constants.SLASH  + alterMaterializedViewRequest.getMaterializedViewName();
        withCatalogException(() -> patchWithHeader(url, alterMaterializedViewRequest.getInput(),
            alterMaterializedViewRequest.getHeader(getToken())));
    }

    @Override
    public void deleteTableColumnStatistics(DeleteColumnStatisticsRequest request) throws CatalogException {
        String urlPrefix =
            Constants.getTableUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH + request.getTableName()
                + Constants.COLUMN_STATISTICS;
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        withCatalogException(() -> deleteWithHeader(url, request.getHeader(getToken())));
    }

    @Override
    public PagedList<ColumnStatisticsObj> getTableColumnsStatistics(GetTableColumnStatisticRequest request) {
        String prefixUrl =
            Constants.getTableUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH + request.getTableName()
                + Constants.COLUMN_STATISTICS;
        String url = makeReqParamsWithPrefix(prefixUrl, request.getParams());

        return CatalogClientHelper.listWithCatalogException(
            () -> getWithHeader(url, request.getHeader(getToken())), ColumnStatisticsObj.class);
    }

    @Override
    public void updateTableColumnStatistics(UpdateTableColumnStatisticRequest request) {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getTableName() + Constants.COLUMN_STATISTICS;
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void deletePartitionColumnStatistics(DeletePartitionColumnStatisticsRequest request) {
        String prefixUrl =
            Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + Constants.COLUMN_STATISTICS;
        String url = makeReqParamsWithPrefix(prefixUrl, request.getParams());
        withCatalogException(() -> deleteWithHeader(url, request.getHeader(getToken())));
    }

    @Override
    public AggrStatisticData getAggrColStats(GetAggregateColumnStatisticsRequest request) {
        String prefixUrl =
            Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + Constants.COLUMN_STATISTICS;
        String url = makeReqParamsWithPrefix(prefixUrl, request.getParams());

        return withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())),
            AggrStatisticData.class);
    }

    @Override
    public PartitionStatisticData getPartitionColumnStatistics(GetPartitionColumnStatisticsRequest request) {
        String prefixUrl =
            Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + Constants.COLUMN_STATISTICS;
        String url = makeReqParamsWithPrefix(prefixUrl, request.getParams());

        return withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())),
            PartitionStatisticData.class);
    }

    @Override
    public void setPartitionsColumnStatistics(SetPartitionColumnStatisticsRequest request) {
        String prefixUrl =
            Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + Constants.COLUMN_STATISTICS;
        String url = makeReqParamsWithPrefix(prefixUrl, request.getParams());

        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void updatePartitionColumnStatistics(UpdatePartitionColumnStatisticRequest request) {
        String prefixUrl =
            Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + Constants.COLUMN_STATISTICS;
        String url = makeReqParamsWithPrefix(prefixUrl, request.getParams());

        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public KerberosToken addToken(AddTokenRequest request) throws CatalogException {
        String prefixUrl = Constants.getTokenUrlPrefix(catalogServerUrlPrefix, request);
        String url = prefixUrl + Constants.SLASH + request.getTokenType();

        return withCatalogException(
            () -> postWithHeader(url, request.getInput(), request.getHeader(getToken())), KerberosToken.class);
    }

    @Override
    public KerberosToken alterToken(AlterKerberosTokenRequest request) throws CatalogException {
        String prefixUrl = Constants.getTokenUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH + request.getTokenType();
        String url = makeReqParamsWithPrefix(prefixUrl, request.getParams());

        return withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())),
            KerberosToken.class);
    }

    @Override
    public void deleteToken(DeleteTokenRequest request) throws CatalogException {
        String prefixUrl = Constants.getTokenUrlPrefix(catalogServerUrlPrefix, request);
        String url = prefixUrl + Constants.SLASH + request.getTokenType();

        withCatalogException(() -> deleteWithHeader(url, request.getHeader(getToken())));
    }

    @Override
    public PagedList<KerberosToken> ListToken(ListTokenRequest request) throws CatalogException {
        String prefixUrl =
            Constants.getTokenUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH + request.getTokenType()
                + Constants.LIST;
        String url = request.getPattern() == null ? prefixUrl :
            prefixUrl  + Constants.QUESTION + Constants.REQ_PARAM_PATTERN + Constants.EQUAL + request.getPattern();

        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())),
            KerberosToken.class);
    }

    @Override
    public KerberosToken getTokenWithRenewer(GetTokenWithRenewerRequest request) throws CatalogException {
        String prefixUrl = Constants.getTokenUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH + request.getTokenType() ;
        String url = makeReqParamsWithPrefix(prefixUrl, request.getParams());

        return withCatalogException(
            () -> getWithHeader(url, request.getHeader(getToken())), KerberosToken.class);
    }

    @Override
    public PagedList<PrimaryKey> getPrimaryKeys(GetPrimaryKeysRequest request) {
        String url = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getTableName() + Constants.PRIMARY_KEYS;
        return CatalogClientHelper.listWithCatalogException(
            () -> getWithHeader(url, request.getHeader(getToken())), PrimaryKey.class);
    }

    @Override
    public PagedList<ForeignKey> getForeignKeys(GetForeignKeysReq request) {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getTableName() + Constants.FOREIGN_KEYS;
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        return CatalogClientHelper.listWithCatalogException(
            () -> getWithHeader(url, request.getHeader(getToken())), ForeignKey.class);
    }

    @Override
    public PagedList<Constraint> getConstraints(GetConstraintsRequest request) {
        String urlPrefix = Constants.getConstraintUrlPrefix(catalogServerUrlPrefix, request);
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        return CatalogClientHelper.listWithCatalogException(
            () -> getWithHeader(url, request.getHeader(getToken())), Constraint.class);
    }

    @Override
    public void dropConstraint(DeleteConstraintRequest request) {
        String url = Constants.getConstraintUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH
            + request.getConstraintName();
        withCatalogException(() -> deleteWithHeader(url, request.getHeader(getToken())));
    }

    @Override
    public void addPrimaryKey(AddPrimaryKeysRequest request) {
        String url = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getTableName() + Constants.PRIMARY_KEYS;
        withCatalogException(()->postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void addForeignKey(AddForeignKeysRequest request) {
        String url = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request)
            + Constants.SLASH + request.getTableName() + Constants.FOREIGN_KEYS;
        withCatalogException(()->postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void addConstraint(AddConstraintsRequest request) {
        String url = Constants.getConstraintUrlPrefix(catalogServerUrlPrefix, request);
        withCatalogException(()->postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public KerberosToken getToken(GetTokenRequest request) throws CatalogException {
        String prefixUrl = Constants.getTokenUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH + request.getTokenType();
        String url = prefixUrl + Constants.SLASH + request.getTokenId();

        return withCatalogException(
            () -> getWithHeader(url, request.getHeader(getToken())), KerberosToken.class);
    }

}
