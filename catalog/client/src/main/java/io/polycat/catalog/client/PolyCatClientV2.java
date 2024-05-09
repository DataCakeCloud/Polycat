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

import io.polycat.catalog.client.authorization.CredentialsProvider;
import io.polycat.catalog.client.endpoint.EndpointProvider;
import io.polycat.catalog.client.util.RequestChecker;
import io.polycat.catalog.client.util.Constants;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.http.CatalogClientHelper;
import io.polycat.catalog.common.lineage.LineageFact;
import io.polycat.catalog.common.model.AuthorizationResponse;
import io.polycat.catalog.common.model.Catalog;
import io.polycat.catalog.common.model.Database;
import io.polycat.catalog.common.model.LineageInfo;
import io.polycat.catalog.common.model.PagedList;
import io.polycat.catalog.common.model.Partition;
import io.polycat.catalog.common.model.Role;
import io.polycat.catalog.common.model.Table;
import io.polycat.catalog.common.model.TableAccessUsers;
import io.polycat.catalog.common.model.TableBrief;
import io.polycat.catalog.common.model.TableSource;
import io.polycat.catalog.common.model.TableUsageProfile;
import io.polycat.catalog.common.model.discovery.CatalogTableCount;
import io.polycat.catalog.common.model.discovery.DatabaseSearch;
import io.polycat.catalog.common.model.discovery.DiscoverySearchBase;
import io.polycat.catalog.common.model.discovery.ObjectCount;
import io.polycat.catalog.common.model.discovery.TableCategories;
import io.polycat.catalog.common.model.discovery.TableSearch;
import io.polycat.catalog.common.model.glossary.Category;
import io.polycat.catalog.common.model.glossary.Glossary;
import io.polycat.catalog.common.model.lock.LockInfo;
import io.polycat.catalog.common.model.stats.AggrStatisticData;
import io.polycat.catalog.common.model.stats.ColumnStatisticsObj;
import io.polycat.catalog.common.model.stats.PartitionStatisticData;
import io.polycat.catalog.common.plugin.request.AddCategoryRelationRequest;
import io.polycat.catalog.common.plugin.request.AddPartitionRequest;
import io.polycat.catalog.common.plugin.request.AlterCatalogRequest;
import io.polycat.catalog.common.plugin.request.AlterCategoryRequest;
import io.polycat.catalog.common.plugin.request.AlterColumnRequest;
import io.polycat.catalog.common.plugin.request.AlterDatabaseRequest;
import io.polycat.catalog.common.plugin.request.AlterFunctionRequest;
import io.polycat.catalog.common.plugin.request.AlterGlossaryRequest;
import io.polycat.catalog.common.plugin.request.AlterPartitionRequest;
import io.polycat.catalog.common.plugin.request.AlterRoleRequest;
import io.polycat.catalog.common.plugin.request.AlterTableRequest;
import io.polycat.catalog.common.plugin.request.AppendPartitionRequest;
import io.polycat.catalog.common.plugin.request.AuthenticationRequest;
import io.polycat.catalog.common.plugin.request.CategoryRelationBaseRequest;
import io.polycat.catalog.common.plugin.request.CheckLockRequest;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.CreateCategoryRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.CreateFunctionRequest;
import io.polycat.catalog.common.plugin.request.CreateGlossaryRequest;
import io.polycat.catalog.common.plugin.request.CreateLockRequest;
import io.polycat.catalog.common.plugin.request.CreateRoleRequest;
import io.polycat.catalog.common.plugin.request.CreateTableRequest;
import io.polycat.catalog.common.plugin.request.DatabaseSearchRequest;
import io.polycat.catalog.common.plugin.request.DeleteCategoryRequest;
import io.polycat.catalog.common.plugin.request.DeleteColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.DeleteDatabaseRequest;
import io.polycat.catalog.common.plugin.request.DeleteGlossaryRequest;
import io.polycat.catalog.common.plugin.request.DeletePartitionColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.DeleteTableRequest;
import io.polycat.catalog.common.plugin.request.DoesPartitionExistsRequest;
import io.polycat.catalog.common.plugin.request.DropCatalogRequest;
import io.polycat.catalog.common.plugin.request.DropPartitionRequest;
import io.polycat.catalog.common.plugin.request.DropPartitionsRequest;
import io.polycat.catalog.common.plugin.request.DropRoleRequest;
import io.polycat.catalog.common.plugin.request.FunctionRequestBase;
import io.polycat.catalog.common.plugin.request.GetAggregateColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.GetAllFunctionRequest;
import io.polycat.catalog.common.plugin.request.GetCatalogRequest;
import io.polycat.catalog.common.plugin.request.GetCatalogTableCountRequest;
import io.polycat.catalog.common.plugin.request.GetCategoryRequest;
import io.polycat.catalog.common.plugin.request.GetDataLineageFactRequest;
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest;
import io.polycat.catalog.common.plugin.request.GetFunctionRequest;
import io.polycat.catalog.common.plugin.request.GetGlossaryRequest;
import io.polycat.catalog.common.plugin.request.GetLatestPartitionNameRequest;
import io.polycat.catalog.common.plugin.request.GetObjectCountRequest;
import io.polycat.catalog.common.plugin.request.GetPartitionColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.GetPartitionCountRequest;
import io.polycat.catalog.common.plugin.request.GetPartitionRequest;
import io.polycat.catalog.common.plugin.request.GetPartitionWithAuthRequest;
import io.polycat.catalog.common.plugin.request.GetRoleRequest;
import io.polycat.catalog.common.plugin.request.GetTableAccessUsersRequest;
import io.polycat.catalog.common.plugin.request.GetTableCategoriesRequest;
import io.polycat.catalog.common.plugin.request.GetTableColumnStatisticRequest;
import io.polycat.catalog.common.plugin.request.GetTableMetaRequest;
import io.polycat.catalog.common.plugin.request.GetTableNamesRequest;
import io.polycat.catalog.common.plugin.request.GetTableObjectsByNameRequest;
import io.polycat.catalog.common.plugin.request.GetTablePartitionsByNamesRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.GetTableUsageProfileRequest;
import io.polycat.catalog.common.plugin.request.GetUsageProfileDetailsRequest;
import io.polycat.catalog.common.plugin.request.GetUsageProfilesGroupByUserRequest;
import io.polycat.catalog.common.plugin.request.GlossaryBaseRequest;
import io.polycat.catalog.common.plugin.request.InsertUsageProfileRequest;
import io.polycat.catalog.common.plugin.request.ListCatalogUsageProfilesRequest;
import io.polycat.catalog.common.plugin.request.ListCatalogsRequest;
import io.polycat.catalog.common.plugin.request.ListDatabasesRequest;
import io.polycat.catalog.common.plugin.request.ListFunctionRequest;
import io.polycat.catalog.common.plugin.request.ListGlossaryRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionByFilterRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionNamesByFilterRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionNamesPsRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionsByExprRequest;
import io.polycat.catalog.common.plugin.request.ListPartitionsWithAuthRequest;
import io.polycat.catalog.common.plugin.request.ListTableObjectsRequest;
import io.polycat.catalog.common.plugin.request.ListTablePartitionsRequest;
import io.polycat.catalog.common.plugin.request.ListTablesRequest;
import io.polycat.catalog.common.plugin.request.LockBaseRequest;
import io.polycat.catalog.common.plugin.request.LockHeartbeatRequest;
import io.polycat.catalog.common.plugin.request.RemoveCategoryRelationRequest;
import io.polycat.catalog.common.plugin.request.RenamePartitionRequest;
import io.polycat.catalog.common.plugin.request.SearchBaseRequest;
import io.polycat.catalog.common.plugin.request.SearchDataLineageRequest;
import io.polycat.catalog.common.plugin.request.SearchDiscoveryNamesRequest;
import io.polycat.catalog.common.plugin.request.SetPartitionColumnStatisticsRequest;
import io.polycat.catalog.common.plugin.request.ShowGrantsToRoleRequest;
import io.polycat.catalog.common.plugin.request.ShowLocksRequest;
import io.polycat.catalog.common.plugin.request.ShowPermObjectsRequest;
import io.polycat.catalog.common.plugin.request.ShowRoleNamesRequest;
import io.polycat.catalog.common.plugin.request.ShowRolesRequest;
import io.polycat.catalog.common.plugin.request.TableSearchRequest;
import io.polycat.catalog.common.plugin.request.UnlockRequest;
import io.polycat.catalog.common.plugin.request.UpdateDataLineageRequest;
import io.polycat.catalog.common.plugin.request.UpdatePartitionColumnStatisticRequest;
import io.polycat.catalog.common.plugin.request.UpdateTableColumnStatisticRequest;
import io.polycat.catalog.common.plugin.request.input.FunctionInput;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.polycat.catalog.common.http.CatalogClientHelper.deleteWithHeader;
import static io.polycat.catalog.common.http.CatalogClientHelper.getWithHeader;
import static io.polycat.catalog.common.http.CatalogClientHelper.patchWithHeader;
import static io.polycat.catalog.common.http.CatalogClientHelper.postWithHeader;
import static io.polycat.catalog.common.http.CatalogClientHelper.putWithHeader;
import static io.polycat.catalog.common.http.CatalogClientHelper.withCatalogException;

/**
 * @author liangyouze
 * @date 2024/2/27
 */
public class PolyCatClientV2 implements Client{

    private String catalogServerUrlPrefix = "http://127.0.0.1:8082/v1/";
    private CredentialsProvider credentialsProvider;

    private EndpointProvider endpointProvider;

    private String getToken() {
        return credentialsProvider.getCredentials().getToken();
    }

    public PolyCatClientV2(EndpointProvider endpointProvider, CredentialsProvider credentialsProvider) {
        this.endpointProvider = endpointProvider;
        this.credentialsProvider = credentialsProvider;
        catalogServerUrlPrefix =  "http://" + getEndpoint() + "/v1/";
    }

    @Override
    public String getProjectId() {
        return credentialsProvider.getCredentials().getProjectId();
    }

    @Override
    public String getUserName() {
        return credentialsProvider.getCredentials().getUserName();
    }

    @Override
    public String getEndpoint() {
        return endpointProvider.getEndpoint();
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

    private void appendUrlParam(StringBuilder sb, String paramKey, Object value, boolean nonNull) {
        if (value == null && nonNull) {
            throw new NullPointerException("Request param: " + paramKey + " is null.");
        }
        if (value != null) {
            sb.append(paramKey)
                    .append(Constants.EQUAL)
                    .append(urlParamEncode(String.valueOf(value)))
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
        if (request.getProjectId() == null) {
            throw new NullPointerException("Request param: projectId is null.");
        }
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
    public void alterCatalog(AlterCatalogRequest request) throws CatalogException {
        RequestChecker.check(request);
        String url = Constants.getCatalogUrlPrefix(catalogServerUrlPrefix, request)
                + Constants.SLASH + request.getCatalogName();
        withCatalogException(() -> putWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public Catalog getCatalog(GetCatalogRequest request) throws CatalogException {
        RequestChecker.check(request);
        String url = catalogServerUrlPrefix + Constants.getProjectId(request) + Constants.CATALOGS
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
    public PagedList<TableBrief> getTableMeta(GetTableMetaRequest request) throws CatalogException {
        String urlPrefix = Constants.getCatalogUrlPrefix(catalogServerUrlPrefix, request);
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())),
                TableBrief.class);
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
    public PagedList<Table> listTables(ListTablesRequest request) throws CatalogException {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request);
        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(getListTableUrl(request, urlPrefix), request.getHeader(getToken())),
                Table.class);
    }

    @Override
    public PagedList<Table> listTables(ListTableObjectsRequest request) throws CatalogException {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request) + Constants.LIST_TABLE_OBJECTS;
        return CatalogClientHelper.listWithCatalogException(() -> postWithHeader(getListTableUrl(request, urlPrefix), request.getInput(), request.getHeader(getToken())),
                Table.class);
    }

    private String getListTableUrl(ListTableObjectsRequest request, String urlPrefix) {
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
        return makeReqParamsWithPrefix(urlPrefix, params);
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
        Map<String, String> params = request.getParams();
        params.put(Constants.REQ_PARAM_ESCAPE, String.valueOf(request.isEscape()));
        String url = makeReqParamsWithPrefix(urlPrefix, params);
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
    public List<Partition> listPartitionsByExpr(ListPartitionsByExprRequest request) throws CatalogException {
        //controller: listPartitionsByExpr()
        String url = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) +
                Constants.LIST_PARTITIONS_BY_EXPR;
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
    public List<Partition> listPartitions(ListTablePartitionsRequest request) throws CatalogException {
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
    public Integer getPartitionCount(GetPartitionCountRequest request) throws CatalogException {
        String url = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + Constants.GET_PARTITION_COUNT;
        return withCatalogException(() -> postWithHeader(url, request.getInput(),
                request.getHeader(getToken())), Integer.class);
    }

    @Override
    public String getLatestPartitionName(GetLatestPartitionNameRequest request) throws CatalogException {
        String url = Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + Constants.GET_LATEST_PARTITION_NAME;
        return withCatalogException(() -> postWithHeader(url, request.getInput(),
                request.getHeader(getToken())), String.class);
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
    public PagedList<Table> getTableObjectsByName(GetTableObjectsByNameRequest request) throws CatalogException {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request);
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())),
                Table.class);
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
    public PagedList<TableUsageProfile> getUsageProfileDetails(GetUsageProfileDetailsRequest request) throws CatalogException {
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
        if (StringUtils.isNotEmpty(request.getPageToken())) {
            params.put(Constants.REQ_PARAM_PAGE_TOKEN, request.getPageToken());
        }
        if (StringUtils.isNotEmpty(request.getTag())) {
            params.put(Constants.REQ_PARAM_TAG, request.getTag());
        }
        if (Objects.nonNull(request.getOperations()) && !request.getOperations().isEmpty()) {
            params.put(Constants.REQ_PARAM_OPERATIONS, String.join(",", request.getOperations()));
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

        return CatalogClientHelper.listWithCatalogException(() -> getWithHeader(url, request.getHeader(getToken())), TableUsageProfile.class);
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

    /**
     * update data lineage
     *
     * @param request request
     * @throws CatalogException
     */
    @Override
    public void updateDataLineage(UpdateDataLineageRequest request) throws CatalogException {
        String url = Constants.getDataLineageUrlPrefix(catalogServerUrlPrefix, request)
                + Constants.UPDATE_DATA_LINEAGE;
        withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())));
    }

    /**
     * search lineage graph
     *
     * @param request request
     * @return {@link LineageInfo}
     * @throws CatalogException
     */
    @Override
    public LineageInfo searchDataLineageGraph(SearchDataLineageRequest request) throws CatalogException {
        StringBuilder builder = new StringBuilder(Constants.getDataLineageUrlPrefix(catalogServerUrlPrefix, request))
                .append(Constants.SEARCH_DATA_LINEAGE)
                .append(Constants.QUESTION);
        appendUrlParam(builder, Constants.REQ_PARAM_DB_TYPE, request.getDbType(), true);
        appendUrlParam(builder, Constants.REQ_PARAM_OBJECT_TYPE, request.getObjectType(), true);
        appendUrlParam(builder, Constants.REQ_PARAM_QUALIFIED_NAME, request.getQualifiedName(), true);
        appendUrlParam(builder, Constants.REQ_PARAM_DEPTH, request.getDepth(), false);
        appendUrlParam(builder, Constants.REQ_PARAM_DIRECTION, request.getDirection(), false);
        appendUrlParam(builder, Constants.REQ_PARAM_LINEAGE_TYPE, request.getLineageType(), false);
        appendUrlParam(builder, Constants.REQ_PARAM_START_TIME, request.getStartTime(), false);
        return withCatalogException(() -> getWithHeader(builder.toString(),
                request.getHeader(getToken())), LineageInfo.class);
    }

    /**
     * lineage job fact by job fact id.
     *
     * @param request request
     * @return {@link LineageFact}
     * @throws CatalogException
     */
    @Override
    public LineageFact getDataLineageFact(GetDataLineageFactRequest request) throws CatalogException {
        StringBuilder builder = new StringBuilder(Constants.getDataLineageUrlPrefix(catalogServerUrlPrefix, request))
                .append(Constants.GET_DATA_LINEAGE_FACT)
                .append(Constants.QUESTION);
        appendUrlParam(builder, Constants.REQ_PARAM_FACT_ID, request.getFactId(), true);
        return withCatalogException(() -> getWithHeader(builder.toString(),
                request.getHeader(getToken())), LineageFact.class);
    }

/*

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
*/

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

    @Override
    public void deleteTableColumnStatistics(DeleteColumnStatisticsRequest request) throws CatalogException {
        String urlPrefix =
                Constants.getTableUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH + request.getTableName()
                        + Constants.COLUMN_STATISTICS;
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        withCatalogException(() -> deleteWithHeader(url, request.getHeader(getToken())));
    }

    @Override
    public ColumnStatisticsObj[] getTableColumnsStatistics(GetTableColumnStatisticRequest request) {
        String prefixUrl =
                Constants.getTableUrlPrefix(catalogServerUrlPrefix, request) + Constants.SLASH + request.getTableName()
                        + Constants.COLUMN_STATISTICS;
        String url = makeReqParamsWithPrefix(prefixUrl, request.getParams());

        return withCatalogException(
                () -> getWithHeader(url, request.getHeader(getToken())), ColumnStatisticsObj[].class);
    }

    @Override
    public boolean updateTableColumnStatistics(UpdateTableColumnStatisticRequest request) {
        String urlPrefix = Constants.getTableUrlPrefix(catalogServerUrlPrefix, request)
                + Constants.SLASH + request.getTableName() + Constants.COLUMN_STATISTICS;
        String url = makeReqParamsWithPrefix(urlPrefix, request.getParams());
        return withCatalogException(() -> patchWithHeader(url, request.getInput(), request.getHeader(getToken())),
                Boolean.class);
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
    public boolean updatePartitionColumnStatistics(UpdatePartitionColumnStatisticRequest request) {
        String prefixUrl =
                Constants.getPartitionUrlPrefix(catalogServerUrlPrefix, request) + Constants.COLUMN_STATISTICS;
        String url = makeReqParamsWithPrefix(prefixUrl, request.getParams());

        return withCatalogException(() -> postWithHeader(url, request.getInput(), request.getHeader(getToken())), Boolean.class);
    }

    /**
     * discovery fulltext
     *
     * @param request
     * @return
     * @throws CatalogException
     */
    @Override
    public PagedList<DiscoverySearchBase> search(SearchBaseRequest request) throws CatalogException {
        return CatalogClientHelper.listWithCatalogException(
                () -> postWithHeader(getDiscoveryUrl(request, Constants.SEARCH), request.getInput(), request.getHeader(getToken())), DiscoverySearchBase.class);
    }

    private String getDiscoveryUrl(SearchBaseRequest request, String urlSuffix) {
        StringBuilder sb = new StringBuilder(Constants.getDiscoveryUrlPrefix(catalogServerUrlPrefix, request))
                .append(urlSuffix);
        sb.append(Constants.QUESTION);
        appendUrlParam(sb, request.getPageToken(), Constants.REQ_PARAM_PAGE_TOKEN);
        appendUrlParam(sb, request.getCatalogName(), Constants.REQ_PARAM_CATALOG_NAME);
        appendUrlParam(sb, request.getKeyword(), Constants.REQ_PARAM_KEYWORD);
        appendUrlParam(sb, request.getOwner(), Constants.REQ_PARAM_OWNER);
        if (request.getCategoryId() != null) {
            appendUrlParam(sb, request.getCategoryId().toString(), Constants.REQ_CATEGORY_ID);
        }
        if (request.getObjectType() != null) {
            appendUrlParam(sb, request.getObjectType().name(), Constants.REQ_PARAM_OBJECT_TYPE);
        }
        appendUrlParam(sb, request.getLogicalOperator().name(), Constants.REQ_PARAM_LOGICAL_OPERATOR);
        sb.append(Constants.REQ_PARAM_EXACT_MATCH).append(Constants.EQUAL).append(request.isExactMatch()).append(Constants.AND);
        sb.append(Constants.REQ_PARAM_WITH_CATEGORIES).append(Constants.EQUAL).append(request.isWithCategories()).append(Constants.AND);
        sb.append(Constants.LIMIT).append(Constants.EQUAL).append(request.getLimit());
        return sb.toString();
    }

    private String getCategoryRelationUrl(CategoryRelationBaseRequest request, String urlSuffix) {
        StringBuilder sb = new StringBuilder(Constants.getDiscoveryUrlPrefix(catalogServerUrlPrefix, request))
                .append(urlSuffix);
        sb.append(Constants.QUESTION);
        appendUrlParam(sb, request.getCategoryId().toString(), Constants.REQ_CATEGORY_ID);
        appendUrlParam(sb, request.getQualifiedName(), Constants.REQ_PARAM_QUALIFIED_NAME);
        return sb.toString();
    }

    /**
     * table fulltext
     *
     * @param request
     * @return
     * @throws CatalogException
     */
    @Override
    public PagedList<TableSearch> searchTable(TableSearchRequest request) throws CatalogException {
        return CatalogClientHelper.listWithCatalogException(
                () -> postWithHeader(getDiscoveryUrl(request, Constants.SEARCH), request.getInput(), request.getHeader(getToken())), TableSearch.class);
    }

    @Override
    public PagedList<TableCategories> searchTableWithCategories(TableSearchRequest request) throws CatalogException {
        return CatalogClientHelper.listWithCatalogException(
                () -> postWithHeader(getDiscoveryUrl(request, Constants.SEARCH), request.getInput(), request.getHeader(getToken())), TableCategories.class);
    }

    @Override
    public PagedList<DatabaseSearch> searchDatabase(DatabaseSearchRequest request) throws CatalogException {
        return CatalogClientHelper.listWithCatalogException(
                () -> postWithHeader(getDiscoveryUrl(request, Constants.SEARCH), request.getInput(), request.getHeader(getToken())), DatabaseSearch.class);
    }

    @Override
    public PagedList<String> searchDiscoveryNames(SearchDiscoveryNamesRequest request) throws CatalogException {
        return CatalogClientHelper.listWithCatalogException(
                () -> postWithHeader(getDiscoveryUrl(request, Constants.MATCH_LIST_NAMES), request.getInput(), request.getHeader(getToken())), String.class);
    }

    @Override
    public void addCategoryRelation(AddCategoryRelationRequest request) throws CatalogException {
        withCatalogException(()->patchWithHeader(getCategoryRelationUrl(request, Constants.ADD_CATEGORY_RELATION), request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void removeCategoryRelation(RemoveCategoryRelationRequest request) throws CatalogException {
        withCatalogException(()->deleteWithHeader(getCategoryRelationUrl(request, Constants.REMOVE_CATEGORY_RELATION), request.getHeader(getToken())));
    }

    @Override
    public ObjectCount getObjectCountByCategory(GetObjectCountRequest request) throws CatalogException {
        return withCatalogException(
                () -> postWithHeader(getDiscoveryUrl(request, Constants.GET_OBJECT_COUNT_BY_CATEGORY), request.getInput(), request.getHeader(getToken())),
                ObjectCount.class);
    }

    @Override
    public List<CatalogTableCount> getTableCountByCatalog(GetCatalogTableCountRequest request) throws CatalogException {
        final CatalogTableCount[] catalogTableCounts = withCatalogException(
                () -> postWithHeader(getDiscoveryUrl(request, Constants.GET_TABLE_COUNT_BY_CATALOG), request.getInput(), request.getHeader(getToken())),
                CatalogTableCount[].class);
        return Arrays.asList(catalogTableCounts);
    }

    @Override
    public TableCategories getTableCategories(GetTableCategoriesRequest request) throws CatalogException {
        final StringBuilder url = new StringBuilder(Constants.getDiscoveryUrlPrefix(catalogServerUrlPrefix, request));
        url.append(Constants.GET_TABLE_CATEGORIES).append(Constants.QUESTION);
        appendUrlParam(url, request.getQualifiedName(), Constants.REQ_PARAM_QUALIFIED_NAME);
        return withCatalogException(
                () -> getWithHeader(url.toString(), request.getHeader(getToken())), TableCategories.class
        );
    }

    private String getGlossaryUrl(GlossaryBaseRequest request, String urlSuffix) {
        StringBuilder sb = new StringBuilder(Constants.getGlossaryUrlPrefix(catalogServerUrlPrefix, request))
                .append(urlSuffix);
        sb.append(Constants.QUESTION);
        if (request.getId() != null) {
            appendUrlParam(sb, request.getId().toString(), Constants.REQ_ID);
        }
        return sb.toString();
    }

    @Override
    public Glossary createGlossary(CreateGlossaryRequest request) throws CatalogException {
        return withCatalogException(()->postWithHeader(getGlossaryUrl(request, Constants.CREATE_GLOSSARY), request.getInput(), request.getHeader(getToken())), Glossary.class);
    }

    @Override
    public void alterGlossary(AlterGlossaryRequest request) throws CatalogException {
        withCatalogException(()->postWithHeader(getGlossaryUrl(request, Constants.ALTER_GLOSSARY), request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void deleteGlossary(DeleteGlossaryRequest request) throws CatalogException {
        withCatalogException(()->deleteWithHeader(getGlossaryUrl(request, Constants.DELETE_GLOSSARY), request.getHeader(getToken())));
    }

    @Override
    public Glossary getGlossary(GetGlossaryRequest request) throws CatalogException {
        StringBuilder url = new StringBuilder(getGlossaryUrl(request, Constants.GET_GLOSSARY));
        if (request.getGlossaryName() != null && !request.getGlossaryName().isEmpty()) {
            appendUrlParam(url, request.getGlossaryName(),  Constants.REQ_NAME);
        }
        return withCatalogException(()->getWithHeader(url.toString(), request.getHeader(getToken())), Glossary.class);
    }

    @Override
    public PagedList<Glossary> listGlossaryWithoutCategory(ListGlossaryRequest request) throws CatalogException {
        return CatalogClientHelper.listWithCatalogException(()->getWithHeader(getGlossaryUrl(request, Constants.LIST_GLOSSARY_WITHOUT_CATEGORY), request.getHeader(getToken())), Glossary.class);
    }

    @Override
    public Category createCategory(CreateCategoryRequest request) throws CatalogException {
        return withCatalogException(()->postWithHeader(getGlossaryUrl(request, Constants.CREATE_CATEGORY), request.getInput(), request.getHeader(getToken())), Category.class);
    }

    @Override
    public void alterCategory(AlterCategoryRequest request) throws CatalogException {
        withCatalogException(()->postWithHeader(getGlossaryUrl(request, Constants.ALTER_CATEGORY), request.getInput(), request.getHeader(getToken())));
    }

    @Override
    public void deleteCategory(DeleteCategoryRequest request) throws CatalogException {
        withCatalogException(()->deleteWithHeader(getGlossaryUrl(request, Constants.DELETE_CATEGORY), request.getHeader(getToken())));
    }

    @Override
    public Category getCategory(GetCategoryRequest request) throws CatalogException {
        return withCatalogException(()->getWithHeader(getGlossaryUrl(request, Constants.GET_CATEGORY), request.getHeader(getToken())), Category.class);
    }

    private String getLockUrl(LockBaseRequest request, String urlSuffix) {
        StringBuilder sb = new StringBuilder(Constants.getLockUrlPrefix(catalogServerUrlPrefix, request))
                .append(urlSuffix);
        sb.append(Constants.QUESTION);
        if (request.getLockId() != null) {
            appendUrlParam(sb, request.getLockId().toString(), Constants.REQ_LOCK_ID);
        }
        return sb.toString();
    }

    @Override
    public LockInfo lock(CreateLockRequest request) throws CatalogException {
        return withCatalogException(() -> postWithHeader(getLockUrl(request, Constants.CREATE_LOCK), request.getInput(), request.getHeader(getToken())), LockInfo.class);
    }

    @Override
    public void heartbeat(LockHeartbeatRequest request) throws CatalogException {
        withCatalogException(() -> getWithHeader(getLockUrl(request, Constants.HEART_BEAT), request.getHeader(getToken())));
    }

    @Override
    public LockInfo checkLock(CheckLockRequest request) throws CatalogException {
        return withCatalogException(() -> getWithHeader(getLockUrl(request, Constants.CHECK_LOCK), request.getHeader(getToken())), LockInfo.class);
    }

    @Override
    public void unlock(UnlockRequest request) throws CatalogException {
        withCatalogException(() -> deleteWithHeader(getLockUrl(request, Constants.UNLOCK), request.getHeader(getToken())));
    }

    @Override
    public List<LockInfo> showLocks(ShowLocksRequest request) throws CatalogException {
        final LockInfo[] lockInfos = withCatalogException(() -> getWithHeader(getLockUrl(request, Constants.SHOW_LOCKS), request.getHeader(getToken())), LockInfo[].class);
        return Arrays.asList(lockInfos);
    }
}
