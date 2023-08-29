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
package io.polycat.common.sql;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.polycat.catalog.common.ColType;
import io.polycat.catalog.common.DataLineageType;
import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.exception.CarbonSqlException;
import io.polycat.catalog.common.model.SerDeInfo;
import io.polycat.catalog.common.model.StorageDescriptor;
import io.polycat.catalog.common.model.TopTableUsageProfileType;
import io.polycat.catalog.common.model.record.DateWritable;
import io.polycat.catalog.common.model.record.TimestampWritable;
import io.polycat.catalog.common.plugin.request.AlterColumnRequest;
import io.polycat.catalog.common.plugin.request.AlterDatabaseRequest;
import io.polycat.catalog.common.plugin.request.AlterRoleRequest;
import io.polycat.catalog.common.plugin.request.AlterShareRequest;
import io.polycat.catalog.common.plugin.request.AlterTableRequest;
import io.polycat.catalog.common.plugin.request.CreateAcceleratorRequest;
import io.polycat.catalog.common.plugin.request.CreateBranchRequest;
import io.polycat.catalog.common.plugin.request.CreateCatalogRequest;
import io.polycat.catalog.common.plugin.request.CreateDatabaseRequest;
import io.polycat.catalog.common.plugin.request.CreateDelegateRequest;
import io.polycat.catalog.common.plugin.request.CreateRoleRequest;
import io.polycat.catalog.common.plugin.request.CreateShareRequest;
import io.polycat.catalog.common.plugin.request.CreateTableRequest;
import io.polycat.catalog.common.plugin.request.DropCatalogRequest;
import io.polycat.catalog.common.plugin.request.DeleteDatabaseRequest;
import io.polycat.catalog.common.plugin.request.DeleteDelegateRequest;
import io.polycat.catalog.common.plugin.request.DeleteTableRequest;
import io.polycat.catalog.common.plugin.request.DescCatalogRequest;
import io.polycat.catalog.common.plugin.request.DropAcceleratorRequest;
import io.polycat.catalog.common.plugin.request.DropMaterializedViewRequest;
import io.polycat.catalog.common.plugin.request.DropPartitionRequest;
import io.polycat.catalog.common.plugin.request.DropRoleRequest;
import io.polycat.catalog.common.plugin.request.DropShareRequest;
import io.polycat.catalog.common.plugin.request.GetCatalogRequest;
import io.polycat.catalog.common.plugin.request.GetDatabaseRequest;
import io.polycat.catalog.common.plugin.request.GetDelegateRequest;
import io.polycat.catalog.common.plugin.request.GetRoleRequest;
import io.polycat.catalog.common.plugin.request.GetShareRequest;
import io.polycat.catalog.common.plugin.request.GetTableRequest;
import io.polycat.catalog.common.plugin.request.GetTableUsageProfileRequest;
import io.polycat.catalog.common.plugin.request.ListBranchesRequest;
import io.polycat.catalog.common.plugin.request.ListCatalogCommitsRequest;
import io.polycat.catalog.common.plugin.request.ListCatalogUsageProfilesRequest;
import io.polycat.catalog.common.plugin.request.ListCatalogsRequest;
import io.polycat.catalog.common.plugin.request.ListDataLineageRequest;
import io.polycat.catalog.common.plugin.request.ListDatabasesRequest;
import io.polycat.catalog.common.plugin.request.ListDelegatesRequest;
import io.polycat.catalog.common.plugin.request.ListMaterializedViewsRequest;
import io.polycat.catalog.common.plugin.request.ListTableCommitsRequest;
import io.polycat.catalog.common.plugin.request.ListTablePartitionsRequest;
import io.polycat.catalog.common.plugin.request.ListTablesRequest;
import io.polycat.catalog.common.plugin.request.MergeBranchRequest;
import io.polycat.catalog.common.plugin.request.PurgeTableRequest;
import io.polycat.catalog.common.plugin.request.RefreshMaterializedViewRequest;
import io.polycat.catalog.common.plugin.request.RestoreTableRequest;
import io.polycat.catalog.common.plugin.request.SetTablePropertyRequest;
import io.polycat.catalog.common.plugin.request.ShowGrantsToRoleRequest;
import io.polycat.catalog.common.plugin.request.ShowRolesRequest;
import io.polycat.catalog.common.plugin.request.ShowSharesRequest;
import io.polycat.catalog.common.plugin.request.UndropDatabaseRequest;
import io.polycat.catalog.common.plugin.request.UndropTableRequest;
import io.polycat.catalog.common.plugin.request.UnsetTablePropertyRequest;
import io.polycat.catalog.common.plugin.request.input.AcceleratorInput;
import io.polycat.catalog.common.plugin.request.input.AlterTableInput;
import io.polycat.catalog.common.plugin.request.input.CatalogInput;
import io.polycat.catalog.common.plugin.request.input.ColumnChangeInput;
import io.polycat.catalog.common.plugin.request.CreateMaterializedViewRequest;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.DatabaseNameInput;
import io.polycat.catalog.common.plugin.request.input.DelegateInput;
import io.polycat.catalog.common.plugin.request.input.DropPartitionInput;
import io.polycat.catalog.common.plugin.request.input.IndexInput;
import io.polycat.catalog.common.plugin.request.input.MaterializedViewNameInput;
import io.polycat.catalog.common.plugin.request.input.MergeBranchInput;
import io.polycat.catalog.common.plugin.request.input.PartitionFilterInput;
import io.polycat.catalog.common.plugin.request.input.RoleInput;
import io.polycat.catalog.common.plugin.request.input.SetTablePropertyInput;
import io.polycat.catalog.common.plugin.request.input.ShareInput;
import io.polycat.catalog.common.plugin.request.input.ShareTableNameInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;
import io.polycat.catalog.common.plugin.request.input.TableNameInput;
import io.polycat.catalog.common.plugin.request.input.TableTypeInput;
import io.polycat.catalog.common.plugin.request.input.UnsetTablePropertyInput;
import io.polycat.catalog.common.model.Column;
import io.polycat.common.expression.interval.IntervalUnit;
import io.polycat.common.expression.interval.TimeInterval;
import io.polycat.sql.PolyCatSQLParser;
import io.polycat.sql.PolyCatSQLParser.AddColumnsContext;
import io.polycat.sql.PolyCatSQLParser.AddPartitionContext;
import io.polycat.sql.PolyCatSQLParser.AlterColumnContext;
import io.polycat.sql.PolyCatSQLParser.AlterDbLocationContext;
import io.polycat.sql.PolyCatSQLParser.AlterDbPropertiesContext;
import io.polycat.sql.PolyCatSQLParser.AlterShareAddAccountsContext;
import io.polycat.sql.PolyCatSQLParser.AlterShareRemoveAccountsContext;
import io.polycat.sql.PolyCatSQLParser.BackQuotedConfigContext;
import io.polycat.sql.PolyCatSQLParser.BooleanValueContext;
import io.polycat.sql.PolyCatSQLParser.ChangeColumnsContext;
import io.polycat.sql.PolyCatSQLParser.ColumnActionContext;
import io.polycat.sql.PolyCatSQLParser.ColumnAliasesContext;
import io.polycat.sql.PolyCatSQLParser.ColumnConstraintContext;
import io.polycat.sql.PolyCatSQLParser.ColumnDefinitionContext;
import io.polycat.sql.PolyCatSQLParser.ConfigEntryContext;
import io.polycat.sql.PolyCatSQLParser.CreateAcceleratorContext;
import io.polycat.sql.PolyCatSQLParser.CreateBranchContext;
import io.polycat.sql.PolyCatSQLParser.CreateCatalogContext;
import io.polycat.sql.PolyCatSQLParser.CreateDatabaseContext;
import io.polycat.sql.PolyCatSQLParser.CreateDelegateContext;
import io.polycat.sql.PolyCatSQLParser.CreateRoleContext;
import io.polycat.sql.PolyCatSQLParser.CreateShareContext;
import io.polycat.sql.PolyCatSQLParser.CreateTableAsSelectContext;
import io.polycat.sql.PolyCatSQLParser.CreateTableContext;
import io.polycat.sql.PolyCatSQLParser.DatabaseNameContext;
import io.polycat.sql.PolyCatSQLParser.DescAccessStatsForTableContext;
import io.polycat.sql.PolyCatSQLParser.DescCatalogContext;
import io.polycat.sql.PolyCatSQLParser.DescDatabaseContext;
import io.polycat.sql.PolyCatSQLParser.DescDelegateContext;
import io.polycat.sql.PolyCatSQLParser.DescRoleContext;
import io.polycat.sql.PolyCatSQLParser.DescShareContext;
import io.polycat.sql.PolyCatSQLParser.DescTableContext;
import io.polycat.sql.PolyCatSQLParser.DropPartitionContext;
import io.polycat.sql.PolyCatSQLParser.ShowCreateTableContext;
import io.polycat.sql.PolyCatSQLParser.DoubleQuotedConfigContext;
import io.polycat.sql.PolyCatSQLParser.DropAcceleratorContext;
import io.polycat.sql.PolyCatSQLParser.DropCatalogContext;
import io.polycat.sql.PolyCatSQLParser.DropColumnsContext;
import io.polycat.sql.PolyCatSQLParser.DropDatabaseContext;
import io.polycat.sql.PolyCatSQLParser.DropDelegateContext;
import io.polycat.sql.PolyCatSQLParser.DropRoleContext;
import io.polycat.sql.PolyCatSQLParser.DropShareContext;
import io.polycat.sql.PolyCatSQLParser.DropStreamContext;
import io.polycat.sql.PolyCatSQLParser.DropTableContext;
import io.polycat.sql.PolyCatSQLParser.GrantAllObjectPrivilegeToRoleContext;
import io.polycat.sql.PolyCatSQLParser.GrantPrivilegeToRoleContext;
import io.polycat.sql.PolyCatSQLParser.GrantPrivilegeToShareContext;
import io.polycat.sql.PolyCatSQLParser.GrantRoleToUserContext;
import io.polycat.sql.PolyCatSQLParser.GrantShareToUserContext;
import io.polycat.sql.PolyCatSQLParser.IdentifierContext;
import io.polycat.sql.PolyCatSQLParser.MergeBranchContext;
import io.polycat.sql.PolyCatSQLParser.ObjectTypeContext;
import io.polycat.sql.PolyCatSQLParser.PartitionDefinitionContext;
import io.polycat.sql.PolyCatSQLParser.PrincipalContext;
import io.polycat.sql.PolyCatSQLParser.PrivilegeContext;
import io.polycat.sql.PolyCatSQLParser.PropertiesContext;
import io.polycat.sql.PolyCatSQLParser.PropertyContext;
import io.polycat.sql.PolyCatSQLParser.PurgeTableContext;
import io.polycat.sql.PolyCatSQLParser.QualifiedNameContext;
import io.polycat.sql.PolyCatSQLParser.QueryNoWithContext;
import io.polycat.sql.PolyCatSQLParser.QuotedConfigContext;
import io.polycat.sql.PolyCatSQLParser.RenameColumnsContext;
import io.polycat.sql.PolyCatSQLParser.RenameTableContext;
import io.polycat.sql.PolyCatSQLParser.ReplaceColumnsContext;
import io.polycat.sql.PolyCatSQLParser.RestoreTableWithVerContext;
import io.polycat.sql.PolyCatSQLParser.RevokeAllObjectPrivilegeFromRoleContext;
import io.polycat.sql.PolyCatSQLParser.RevokeAllPrivilegeFromRoleContext;
import io.polycat.sql.PolyCatSQLParser.RevokePrivilegeFromRoleContext;
import io.polycat.sql.PolyCatSQLParser.RevokePrivilegeFromShareContext;
import io.polycat.sql.PolyCatSQLParser.RevokeRoleFromUserContext;
import io.polycat.sql.PolyCatSQLParser.RevokeShareFromUserContext;
import io.polycat.sql.PolyCatSQLParser.SetTablePropertiesContext;
import io.polycat.sql.PolyCatSQLParser.ShareFullNameContext;
import io.polycat.sql.PolyCatSQLParser.ShareTableNameContext;
import io.polycat.sql.PolyCatSQLParser.ShowAccessStatsForCatalogContext;
import io.polycat.sql.PolyCatSQLParser.ShowBranchesContext;
import io.polycat.sql.PolyCatSQLParser.ShowCatalogHistoryContext;
import io.polycat.sql.PolyCatSQLParser.ShowCatalogsContext;
import io.polycat.sql.PolyCatSQLParser.ShowDataLineageForTableContext;
import io.polycat.sql.PolyCatSQLParser.ShowDatabasesContext;
import io.polycat.sql.PolyCatSQLParser.ShowDelegatesContext;
import io.polycat.sql.PolyCatSQLParser.ShowGrantsToRoleContext;
import io.polycat.sql.PolyCatSQLParser.ShowRolesContext;
import io.polycat.sql.PolyCatSQLParser.ShowSharesContext;
import io.polycat.sql.PolyCatSQLParser.ShowTableHistoryContext;
import io.polycat.sql.PolyCatSQLParser.ShowTablePartitionsContext;
import io.polycat.sql.PolyCatSQLParser.ShowTablesContext;
import io.polycat.sql.PolyCatSQLParser.SourceShareTableNameContext;
import io.polycat.sql.PolyCatSQLParser.SourceTableNameContext;
import io.polycat.sql.PolyCatSQLParser.StringContext;
import io.polycat.sql.PolyCatSQLParser.TableElementContext;
import io.polycat.sql.PolyCatSQLParser.TableNameContext;
import io.polycat.sql.PolyCatSQLParser.TypeContext;
import io.polycat.sql.PolyCatSQLParser.TypeParameterContext;
import io.polycat.sql.PolyCatSQLParser.UndropDatabaseContext;
import io.polycat.sql.PolyCatSQLParser.UndropTableContext;
import io.polycat.sql.PolyCatSQLParser.UnsetTablePropertiesContext;
import io.polycat.sql.PolyCatSQLParser.UseBranchContext;
import io.polycat.sql.PolyCatSQLParser.UseCatalogContext;
import io.polycat.sql.PolyCatSQLParser.UseDatabaseContext;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.internal.HiveSerDe;
import scala.Option;

public class ParserUtil {

    /**
     * currently support the following pattern: yyyy-[M]M-[d]d
     */
    public static DateWritable stringToDate(String dateString) {
        return new DateWritable(Date.valueOf(dateString));
    }

    /**
     * currently support the following pattern: yyyy-[m]m-[d]d hh:mm:ss[.f...]
     */
    public static TimestampWritable stringToTimestamp(String timestampString) {
        return new TimestampWritable(Timestamp.valueOf(timestampString));
    }

    public static TimeInterval unitInterval(String value, IntervalUnit from) {
        try {
            long number = Long.parseLong(value);
            TimeInterval timeInterval = new TimeInterval();
            timeInterval.add(number, from);
            return timeInterval;
        } catch (NumberFormatException e) {
            throw new CarbonSqlException("Unsupported interval integer value: " + value);
        }
    }

    public static TimeInterval unitToUnitInterval(String value, IntervalUnit from, IntervalUnit to) {
        boolean isNegative = false;
        if (value.startsWith("-")) {
            isNegative = true;
            value = value.substring(1);
        }
        String[] split = value.split("-", -1);
        if (split.length != 2) {
            throw new CarbonSqlException("Unsupported interval from-to value " + value);
        }
        TimeInterval timeInterval = new TimeInterval();
        try {
            long fromValue = Long.parseLong(split[0]);
            long toValue = Long.parseLong(split[1]);
            if (isNegative) {
                timeInterval.subtract(fromValue, from);
                timeInterval.subtract(toValue, from);
            } else {
                timeInterval.add(fromValue, from);
                timeInterval.add(toValue, from);
            }
        } catch (NumberFormatException e) {
            throw new CarbonSqlException("Unsupported interval from-to value " + value);
        }

        return null;
    }

    public static String parseIdentifier(IdentifierContext ctx) {
        if (ctx == null) {
            return null;
        }
        String identifier = ctx.getText().toLowerCase();
        identifier = StringUtils.unwrap(identifier, "`");
        return StringUtils.unwrap(identifier, "\"");
    }

    public static String parseString(StringContext ctx) {
        if (ctx == null) {
            return null;
        }
        return StringUtils.unwrap(ctx.getText(), "'");
    }

    public static Boolean parseBooleanDefaultFalse(BooleanValueContext ctx) {
        if (ctx == null) {
            return false;
        }
        return Boolean.valueOf(StringUtils.unwrap(ctx.getText(), "\""));
    }


    public static DatabaseNameInput parseDatabaseName(DatabaseNameContext ctx) {
        if (ctx == null) {
            return new DatabaseNameInput();
        }
        DatabaseNameInput databaseNameInput = new DatabaseNameInput();
        databaseNameInput.setDatabaseName(parseIdentifier(ctx.database));
        databaseNameInput.setCatalogName(parseIdentifier(ctx.catalog));
        return databaseNameInput;
    }

    public static TableNameInput parseTableName(TableNameContext ctx) {
        if (ctx == null) {
            return new TableNameInput();
        }
        TableNameInput tableName = new TableNameInput();

        tableName.setTableName(parseIdentifier(ctx.table));
        tableName.setDatabaseNameInput(parseDatabaseName(ctx.databaseName()));
        return tableName;
    }

    public static List<String> parseStringListContext(List<StringContext> stringListCtx) {
        List<String> stringList = new ArrayList<>();
        stringListCtx.forEach(sc -> stringList.add(ParserUtil.parseString(sc)));
        return stringList;
    }

    public static CreateBranchRequest buildCreateBranchRequest(CreateBranchContext ctx) {
        CreateBranchRequest request = new CreateBranchRequest();
        request.setBranchName(ctx.qualifiedName(0).getText());
        if (Objects.nonNull(ctx.qualifiedName(1))) {
            request.setCatalogName(ctx.qualifiedName(1).getText());
        }
        request.setVersion(parseString(ctx.string()));
        return request;
    }

    public static CreateCatalogRequest buildCreateCatalogRequest(CreateCatalogContext ctx) {
        CatalogInput catalogInput = new CatalogInput();
        catalogInput.setCatalogName(parseIdentifier(ctx.identifier()));
        catalogInput.setDescription(parseString(ctx.string()));
        CreateCatalogRequest request = new CreateCatalogRequest();
        request.setInput(catalogInput);
        return request;
    }

    public static ListBranchesRequest buildListBranchesRequest(ShowBranchesContext ctx) {
        ListBranchesRequest request = new ListBranchesRequest();
        request.setCatalogName(parseIdentifier(ctx.identifier()));
        return request;
    }

    public static GetCatalogRequest buildGetCatalogRequest(UseBranchContext ctx) {
        return new GetCatalogRequest(null, parseIdentifier(ctx.identifier()));
    }

    public static MergeBranchRequest buildMergeBranchRequest(MergeBranchContext ctx) {
        MergeBranchRequest request = new MergeBranchRequest();

        MergeBranchInput mergeBranchInput = new MergeBranchInput();
        mergeBranchInput.setSrcBranchName(parseIdentifier(ctx.srcBranch().identifier()));
        if (Objects.nonNull(ctx.destBranch())) {
            mergeBranchInput.setDestBranchName(parseIdentifier(ctx.destBranch().identifier()));
        }
        request.setInput(mergeBranchInput);
        return request;
    }

    public static GetCatalogRequest buildGetCatalogRequest(UseCatalogContext ctx) {
        return new GetCatalogRequest(null, parseIdentifier(ctx.identifier()));
    }

    public static DescCatalogRequest buildDescCatalogRequest(DescCatalogContext ctx) {
        DescCatalogRequest request = new DescCatalogRequest();
        request.setCatalogName(parseIdentifier(ctx.identifier()));
        return request;
    }

    public static ListCatalogsRequest buildListCatalogsRequest(ShowCatalogsContext ctx) {
        ListCatalogsRequest request = new ListCatalogsRequest();
        if (Objects.nonNull(ctx.pattern)) {
            request.setPattern(parseString(ctx.pattern));
        }
        return request;
    }

    public static DropCatalogRequest buildDeleteCatalogRequest(DropCatalogContext ctx) {
        DropCatalogRequest request = new DropCatalogRequest();
        request.setCatalogName(parseIdentifier(ctx.identifier()));
        return request;
    }

    public static GetDatabaseRequest buildGetDatabaseRequest(DescDatabaseContext ctx) {
        DatabaseNameInput databaseName = parseDatabaseName(ctx.databaseName());
        Boolean extended = ctx.EXTENDED() != null;

        GetDatabaseRequest request = new GetDatabaseRequest();
        request.setCatalogName(databaseName.getCatalogName());
        request.setDatabaseName(databaseName.getDatabaseName());
        request.setExtended(extended);
        return request;
    }

    public static CreateDatabaseRequest buildCreateDatabaseRequest(CreateDatabaseContext ctx) {
        DatabaseNameInput databaseName = parseDatabaseName(ctx.databaseName());
        DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setDatabaseName(databaseName.getDatabaseName());
        databaseInput.setDescription(ctx.comment == null ? null : parseString(ctx.comment));
        databaseInput.setLocationUri(ctx.location == null ? null : parseString(ctx.location));
        databaseInput.setParameters(parseProperties(ctx.properties()));

        CreateDatabaseRequest request = new CreateDatabaseRequest();
        request.setInput(databaseInput);
        request.setCatalogName(databaseName.getCatalogName());
        return request;
    }

    public static DeleteDatabaseRequest buildDeleteDatabaseRequest(DropDatabaseContext ctx) {
        DatabaseNameInput databaseName = parseDatabaseName(ctx.databaseName());
        DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setDatabaseName(databaseName.getDatabaseName());

        DeleteDatabaseRequest request = new DeleteDatabaseRequest();
        request.setInput(databaseInput);
        request.setCatalogName(databaseName.getCatalogName());
        request.setDatabaseName(databaseName.getDatabaseName());
        if (ctx.IF() != null && ctx.EXISTS() != null) {
            request.setIgnoreUnknownObj(true);
        }
        return request;
    }

    public static UndropDatabaseRequest buildUndropDatabaseRequest(UndropDatabaseContext ctx) {
        DatabaseNameInput databaseName = parseDatabaseName(ctx.databaseName());

        UndropDatabaseRequest request = new UndropDatabaseRequest();
        request.setCatalogName(databaseName.getCatalogName());
        request.setDatabaseName(databaseName.getDatabaseName());
        if (ctx.idProperty() != null) {
            request.setDatabaseId(parseString(ctx.idProperty().string()));
        }
        return request;
    }

    public static ListDatabasesRequest buildListDatabasesRequest(ShowDatabasesContext ctx) {
        ListDatabasesRequest request = new ListDatabasesRequest();
        request.setIncludeDrop(ctx.ALL() != null);
        if (ctx.catalogName() != null) {
            request.setCatalogName(parseIdentifier(ctx.catalogName().catalog));
        }
        if (Objects.nonNull(ctx.pattern)) {
            request.setFilter(parseString(ctx.pattern));
        }
        if (ctx.maximumToScan != null) {
            request.setMaxResults(Integer.parseInt(ctx.maximumToScan.getText()));
        }
        return request;
    }

    public static GetDatabaseRequest buildGetDatabaseRequest(UseDatabaseContext ctx) {
        DatabaseNameInput databaseName = parseDatabaseName(ctx.databaseName());
        GetDatabaseRequest request = new GetDatabaseRequest();
        request.setCatalogName(databaseName.getCatalogName());
        request.setDatabaseName(databaseName.getDatabaseName());
        return request;
    }

    public static ListTablesRequest buildListTablesRequest(ShowTablesContext ctx) {
        ListTablesRequest request = new ListTablesRequest();
        request.setIncludeDrop(ctx.ALL() != null);
        if (ctx.databaseName() != null) {
            request.setDatabaseName(ctx.databaseName().database.getText().toLowerCase());
            if (ctx.databaseName().catalog != null) {
                request.setCatalogName(ctx.databaseName().catalog.getText().toLowerCase());
            }
        }
        if (ctx.pattern != null) {
            request.setExpression(ctx.pattern.getText());
        }
        if (ctx.maximumToScan != null) {
            request.setMaxResults(Integer.parseInt(ctx.maximumToScan.getText()));
        }
        return request;
    }

    public static GetTableRequest buildGetTableRequest(DescTableContext ctx) {
        TableNameInput tableNameInput = parseTableName(ctx.tableName());
        GetTableRequest request = new GetTableRequest(tableNameInput.getTableName());
        request.setCatalogName(tableNameInput.getCatalogName());
        request.setDatabaseName(tableNameInput.getDatabaseName());
        if (ctx.EXTENDED() != null) {
            request.setExtended(true);
        }
        return request;
    }

    public static GetTableRequest buildShowCreateTableRequest(ShowCreateTableContext ctx) {
        TableNameInput tableNameInput = parseTableName(ctx.tableName());
        GetTableRequest request = new GetTableRequest(tableNameInput.getTableName());
        request.setCatalogName(tableNameInput.getCatalogName());
        request.setDatabaseName(tableNameInput.getDatabaseName());
        request.setExtended(true);
        return request;
    }

    public static List<String> parseColumnAliases(ColumnAliasesContext ctx) {
        if (ctx == null) {
            return Collections.emptyList();
        }
        return ctx.identifier().stream().map(ParserUtil::parseIdentifier).collect(Collectors.toList());
    }

    public static boolean parseTransaction(PolyCatSQLParser.BooleanValueContext ctx) {
        String transaction = ctx.getText().toLowerCase();
        if (transaction.equals("true")) {
            return true;
        } else if (transaction.equals("false")) {
            return false;
        } else {
            throw new CarbonSqlException("Transaction flag is wrong");
        }
    }

    public static CreateTableRequest buildCreateTableRequest(CreateTableAsSelectContext ctx) {
        List<Column> columns = parseColumnAliases(ctx.columnAliases()).stream()
            .map(x -> new Column(x, null)).collect(Collectors.toList());
        boolean transaction = parseBooleanDefaultFalse(ctx.booleanValue());
        TableNameInput tableName = parseTableName(ctx.createTableHeader().tableName());
        TableTypeInput dataType = parseTableType(ctx.createTableHeader().EXTERNAL(), ctx.location);
        return buildCreateTableRequest(tableName, columns, transaction, dataType, ctx.comment, ctx.source,
            ctx.properties());
    }

    private static TableTypeInput parseTableType(TerminalNode external, StringContext location) {
        if (external != null || location != null) {
            return TableTypeInput.EXTERNAL_TABLE;
        }
        return TableTypeInput.MANAGED_TABLE;
    }

    protected static CreateTableRequest buildCreateTableRequest(TableNameInput tableName,
        List<Column> columns, boolean transaction, TableTypeInput tableType,
        StringContext comment, IdentifierContext source, PropertiesContext properties) {
        return buildCreateTableRequest(tableName, columns, null, transaction, tableType, comment, source, properties);
    }

    public static CreateTableRequest buildCreateTableRequest(TableNameInput tableName,
        List<Column> columns, List<Column> partitions, boolean transaction, TableTypeInput tableType,
        StringContext comment, IdentifierContext source, PropertiesContext properties) {

        TableInput tableInput = new TableInput();
        tableInput.setTableName(tableName.getTableName());
        tableInput.setLmsMvcc(transaction);
        tableInput.setDescription(comment == null ? null : comment.getText());
        if (partitions != null) {
            tableInput.setPartitionKeys(partitions);
        }
        tableInput.setParameters(parseProperties(properties));
        tableInput.setTableType(tableType.name());
        tableInput.setStorageDescriptor(fillInStorageDescriptor(source, columns));
        CreateTableRequest request = new CreateTableRequest();
        request.setInput(tableInput);
        request.setCatalogName(tableName.getCatalogName());
        request.setDatabaseName(tableName.getDatabaseName());
        return request;
    }

    private static StorageDescriptor fillInStorageDescriptor(IdentifierContext source, List<Column> columns) {
        StorageDescriptor storageInput = new StorageDescriptor();
        if (source != null) {
            storageInput.setSourceShortName(source.getText());
            storageInput.setFileFormat(storageInput.getSourceShortName());
            HiveSerDe hiveSerDe = HiveSerDe.serdeMap()
                    .get(source.getText().toLowerCase())
                    .getOrElse(() -> new HiveSerDe(Option.empty(), Option.empty(), Option.empty()));
            storageInput.setInputFormat(hiveSerDe.inputFormat().getOrElse(() -> ""));
            storageInput.setOutputFormat(hiveSerDe.outputFormat().getOrElse(() -> ""));
            storageInput.setSerdeInfo(fillInSerDeInfo(hiveSerDe));
        }
        storageInput.setColumns(columns);
        return storageInput;
    }

    private static SerDeInfo fillInSerDeInfo(HiveSerDe hiveSerDe) {
        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setSerializationLibrary(hiveSerDe.serde().getOrElse(() -> ""));
        return serDeInfo;
    }

    protected static String parsePrivilege(PrivilegeContext ctx) {
        if (ctx == null) {
            return null;
        }
        return ctx.getText().toLowerCase();
    }

    protected static String parseObjectType(ObjectTypeContext ctx) {
        if (ctx == null) {
            return null;
        }
        return ctx.getText().toLowerCase();
    }

    public static String parseSparkPartition(PolyCatSQLParser.SparkPartitionContext ctx) {
        if (ctx == null) {
            return null;
        }
        return parseIdentifier(ctx.identifier());
    }

    public static Map<String, String> parseProperties(PropertiesContext ctx) {
        if (ctx == null) {
            return null;
        }
        Map<String, String> result = new HashMap<>();
        List<PropertyContext> properties = ctx.property();
        for (PropertyContext context : properties) {
            String key = parseString(context.key);
            String value = parseString(context.value);
            result.put(key, value);
        }
        return result;
    }

    public static String parseType(TypeContext ctx) {
        if (ctx == null) {
            return null;
        }
        StringBuilder type = new StringBuilder();
        for (ParseTree node : ctx.children) {
            type.append(node.getText());
        }
        return type.toString().toUpperCase();
    }

    public static Column parseColumnDefinition(ColumnDefinitionContext context) {
        String colName = parseIdentifier(context.identifier());
        if (colName.equals("commit")) {
            throw new CarbonSqlException("Cannot define a column named \"commit\""
                + " because it is the reserved key of CarbonSql");
        }
        Column columnInput = new Column();
        columnInput.setColumnName(colName);
        columnInput.setColType(parseType(context.type()));
        if ("decimal".equals(columnInput.getColType())) {
            List<TypeParameterContext> typeParameter = context.type().typeParameter();
            if (typeParameter != null && typeParameter.size() == 2) {
//                columnInput.setPrecision(parseTypeParameter(typeParameter.get(0)));
//                columnInput.setScale(parseTypeParameter(typeParameter.get(1)));
            } else {
                throw new CarbonSqlException("Invalid column data type: " + columnInput.getColType());
            }
        }
        parseColumnConstraint(columnInput, context.columnConstraint());
        String comment = parseString(context.string());
        comment = comment == null ? "" : comment;
        columnInput.setComment(comment);

        return columnInput;
    }

    private static int parseTypeParameter(TypeParameterContext ctx) {
        return Integer.parseInt(ctx.getText());
    }

    protected static void parseColumnConstraint(Column columnInput, ColumnConstraintContext constraint) {
        if (constraint == null) {
            return;
        }

        if (constraint.NOT() != null && constraint.NULL() != null) {
//            columnInput.setNotNull(true);
        } else if (constraint.DEFAULT() != null) {
            byte[] defaultValue = parseDefaultValue(constraint);
//            columnInput.setDefaultValue(defaultValue);
        }
    }

    protected static byte[] parseDefaultValue(ColumnConstraintContext defaultValContext) {
        if (defaultValContext.string() != null) {
            return parseString(defaultValContext.string()).getBytes();
        } else if (defaultValContext.number() != null) {
            return defaultValContext.number().children.get(0).toString().getBytes();
        } else if (defaultValContext.booleanValue() != null) {
            return defaultValContext.booleanValue().children.get(0).toString().getBytes();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public static String sourceTextForContext(ParserRuleContext context) {
        Token start = context.start;
        Token stop = context.stop;
        CharStream cs = start.getTokenSource().getInputStream();
        int stopIndex = stop != null ? stop.getStopIndex() : -1;
        return cs.getText(new Interval(start.getStartIndex(), stopIndex));
    }

    public static TableNameInput parseSourceTableName(SourceTableNameContext ctx) {
        return parseTableName(ctx.tableName());
    }

    public static ShareTableNameInput parseSourceShareTableName(SourceShareTableNameContext ctx) {
        return parseShareTableName(ctx.shareTableName());
    }

    protected static ShareTableNameInput parseShareTableName(ShareTableNameContext ctx) {
        ShareTableNameInput shareTableName = new ShareTableNameInput();
        shareTableName.setTableName(ctx.sharetable.getText().toLowerCase());
        shareTableName.setDatabaseName(ctx.sharedatabase.getText().toLowerCase());
        shareTableName.setShareName(ctx.sharecatalog.getText().toLowerCase());
        shareTableName.setProjectId(ctx.shareproject.getText());
        return shareTableName;
    }

    public static Column parseTableElement(TableElementContext ctx) {
        if (ctx.columnDefinition() != null) {
            return ParserUtil.parseColumnDefinition(ctx.columnDefinition());
        } else {
            throw new UnsupportedOperationException();
        }
    }

    protected static Column defaultPartition() {
        // add dash default partition column: commit-partition, "commit" is the reserved key,
        // user cannot make non-partition column named "commit"
        Column defaultPartitionCol = new Column();
        defaultPartitionCol.setColumnName("commit");
        defaultPartitionCol.setColType("STRING");
        return defaultPartitionCol;
    }

    public static List<String> parseQualifiedName(QualifiedNameContext qualifiedName) {
        if (qualifiedName == null) {
            return Collections.emptyList();
        }
        return qualifiedName.identifier().stream().map(ParserUtil::parseIdentifier).collect(Collectors.toList());
    }

    public static CreateShareRequest buildCreateShareRequest(CreateShareContext ctx) {
        ShareInput shareInput = new ShareInput();
        shareInput.setShareName(parseIdentifier(ctx.identifier()));
        CreateShareRequest request = new CreateShareRequest();
        request.setInput(shareInput);
        return request;
    }

    public static DropShareRequest buildDropShareRequest(DropShareContext ctx) {
        DropShareRequest request = new DropShareRequest();
        request.setShareName(parseIdentifier(ctx.identifier()));
        return request;
    }

    public static GetShareRequest buildGetShareRequest(DescShareContext ctx) {
        GetShareRequest request = new GetShareRequest();
        request.setShareName(parseIdentifier(ctx.identifier()));
        return request;
    }

    public static ShowSharesRequest buildShowSharesRequest(ShowSharesContext ctx) {
        ShowSharesRequest request = new ShowSharesRequest();
        if (Objects.nonNull(ctx.pattern)) {
            request.setPattern(parseString(ctx.pattern));
        }
        return request;
    }

    public static AlterShareRequest buildAlterShareRequest(AlterShareAddAccountsContext ctx) {
        ShareInput shareInput = new ShareInput();
        shareInput.setShareName(parseIdentifier(ctx.identifier()));
        String[] accountInfos = parseString(ctx.string()).split(",");
        List<String> accounts = new ArrayList<>();
        List<String> users = new ArrayList<>();
        for (String accountInfo : accountInfos) {
            String[] infos = accountInfo.split(":");
            if (infos.length != 2) {
                throw new CarbonSqlException("input account info error");
            }
            accounts.add(infos[0]);
            users.add(infos[1]);
        }
        shareInput.setAccountIds(accounts.toArray(new String[accounts.size()]));
        shareInput.setUsers(users.toArray(new String[users.size()]));
        AlterShareRequest request = new AlterShareRequest();
        request.setInput(shareInput);
        return request;
    }

    public static AlterShareRequest buildAlterShareRequest(AlterShareRemoveAccountsContext ctx) {
        ShareInput shareInput = new ShareInput();
        shareInput.setShareName(parseIdentifier(ctx.identifier()));
        String[] accounts = parseString(ctx.string()).split(",");
        shareInput.setAccountIds(accounts);
        AlterShareRequest request = new AlterShareRequest();
        request.setInput(shareInput);
        return request;
    }

    public static ShareInput buildShareInput(ShareFullNameContext share, IdentifierContext user) {
        ShareInput shareInput = new ShareInput();
        shareInput.setProjectId(parseIdentifier(share.shareproject));
        shareInput.setShareName(parseIdentifier(share.sharecatalog));
        String[] users = new String[1];
        users[0] = parseIdentifier(user);
        shareInput.setUsers(users);
        return shareInput;
    }

    public static AlterShareRequest buildAlterShareRequest(RevokeShareFromUserContext ctx) {
        ShareInput shareInput = buildShareInput(ctx.share, ctx.user);
        AlterShareRequest request = new AlterShareRequest();
        request.setInput(shareInput);
        return request;
    }

    public static AlterShareRequest buildAlterShareRequest(GrantPrivilegeToShareContext ctx) {
        ShareInput shareInput = buildShareInput(ctx.identifier(), ctx.qualifiedName());
        AlterShareRequest request = new AlterShareRequest();
        request.setInput(shareInput);
        return request;
    }

    private static ShareInput buildShareInput(IdentifierContext identifier, QualifiedNameContext qualifiedNameContext) {
        ShareInput shareInput = new ShareInput();
        shareInput.setShareName(parseIdentifier(identifier));
        shareInput.setOperation(Operation.SELECT_TABLE);
        shareInput.setObjectName(buildObjectName(qualifiedNameContext));
        return shareInput;
    }

    public static String buildObjectName(QualifiedNameContext qualifiedNameContext) {
        List<String> objectNameList = parseQualifiedName(qualifiedNameContext);
        return String.join(".", objectNameList);
    }

    public static AlterShareRequest buildAlterShareRequest(RevokePrivilegeFromShareContext ctx) {
        ShareInput shareInput = buildShareInput(ctx.identifier(), ctx.qualifiedName());
        AlterShareRequest request = new AlterShareRequest();
        request.setInput(shareInput);
        return request;
    }

    public static CreateRoleRequest buildCreateRoleRequest(CreateRoleContext ctx) {
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(parseIdentifier(ctx.identifier()));
        CreateRoleRequest request = new CreateRoleRequest();
        request.setInput(roleInput);
        return request;
    }

    public static DropRoleRequest buildDropRoleRequest(DropRoleContext ctx) {
        DropRoleRequest request = new DropRoleRequest();
        request.setRoleName(parseIdentifier(ctx.identifier()));
        return request;
    }

    public static GetRoleRequest buildGetRoleRequest(DescRoleContext ctx) {
        GetRoleRequest request = new GetRoleRequest();
        request.setRoleName(parseIdentifier(ctx.identifier()));
        return request;
    }

    public static ShowRolesRequest buildShowRolesRequest(ShowRolesContext ctx) {
        ShowRolesRequest request = new ShowRolesRequest();
        if (Objects.nonNull(ctx.pattern)) {
            request.setPattern(ParserUtil.parseString(ctx.pattern));
        }
        return request;
    }
    public static RoleInput buildRoleInput(IdentifierContext role, IdentifierContext user) {
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(parseIdentifier(role));
        String[] userId = new String[1];
        userId[0] = parseIdentifier(user);
        roleInput.setUserId(userId);
        return roleInput;
    }

    public static RoleInput buildRoleInput(PrincipalContext principal, PrivilegeContext privilege,
        ObjectTypeContext objectTypeContext, QualifiedNameContext qualifiedNameContext) {
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(principal.getChild(1).getText().toLowerCase());
        Operation operation = getObjectOperation(parsePrivilege(privilege), parseObjectType(objectTypeContext));
        roleInput.setOperation(operation);
        roleInput.setObjectName(buildObjectName(qualifiedNameContext));
        return roleInput;
    }

    public static AlterRoleRequest buildAlterRoleRequest(RevokeAllPrivilegeFromRoleContext ctx) {
        String roleName = parseIdentifier(ctx.identifier());
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(roleName);
        roleInput.setOperation(Operation.REVOKE_ALL_OPERATION_FROM_ROLE);
        AlterRoleRequest alterRoleRequest = new AlterRoleRequest();
        alterRoleRequest.setInput(roleInput);
        return alterRoleRequest;
    }

    public static ShowGrantsToRoleRequest buildShowGrantsToRoleRequest(ShowGrantsToRoleContext ctx) {
        ShowGrantsToRoleRequest request = new ShowGrantsToRoleRequest();
        request.setRoleName(ParserUtil.parseIdentifier(ctx.identifier()));
        return request;
    }

    public static AlterRoleRequest buildAlterRoleRequest(GrantAllObjectPrivilegeToRoleContext ctx) {
        String roleName = ctx.principal().getChild(1).getText().toLowerCase();
        RoleInput roleInput = buildRoleInput(roleName, Operation.ADD_ALL_OPERATION, ctx.objectType(),
            ctx.qualifiedName());
        AlterRoleRequest alterRoleRequest = new AlterRoleRequest();
        alterRoleRequest.setInput(roleInput);
        return alterRoleRequest;
    }

    public static AlterRoleRequest buildAlterRoleRequest(RevokeAllObjectPrivilegeFromRoleContext ctx) {
        String roleName = ctx.principal().getChild(1).getText().toLowerCase();
        RoleInput roleInput = buildRoleInput(roleName, Operation.REVOKE_ALL_OPERATION, ctx.objectType(),
            ctx.qualifiedName());
        AlterRoleRequest alterRoleRequest = new AlterRoleRequest();
        alterRoleRequest.setInput(roleInput);
        return alterRoleRequest;
    }


    public static AlterRoleRequest buildAlterRoleRequest(RevokeRoleFromUserContext ctx) {
        RoleInput roleInput = ParserUtil.buildRoleInput(ctx.role, ctx.user);
        AlterRoleRequest alterRoleRequest = new AlterRoleRequest();
        alterRoleRequest.setInput(roleInput);
        return alterRoleRequest;
    }

    public static AlterRoleRequest buildAlterRoleRequest(RevokePrivilegeFromRoleContext ctx) {
        RoleInput roleInput = ParserUtil.buildRoleInput(ctx.principal(), ctx.privilege(), ctx.objectType(), ctx.qualifiedName());
        AlterRoleRequest alterRoleRequest = new AlterRoleRequest();
        alterRoleRequest.setInput(roleInput);
        return alterRoleRequest;
    }

    private static RoleInput buildRoleInput(String roleName, Operation operation, ObjectTypeContext objectTypeContext,
        QualifiedNameContext qualifiedNameContext) {
        RoleInput roleInput = new RoleInput();
        roleInput.setRoleName(roleName);
        roleInput.setOperation(operation);
        if (parseObjectType(objectTypeContext).equals("branch")) {
            roleInput.setObjectType("catalog");
        } else {
            roleInput.setObjectType(parseObjectType(objectTypeContext));
        }
        roleInput.setObjectName(buildObjectName(qualifiedNameContext));
        return roleInput;
    }

    public static String parseConfigEntry(ConfigEntryContext key) {
        if (key == null) {
            return null;
        }
        if (key instanceof QuotedConfigContext) {
            return StringUtils.unwrap(key.getText(), "'");
        }
        if (key instanceof DoubleQuotedConfigContext) {
            return StringUtils.unwrap(key.getText(), "\"");
        }
        if (key instanceof BackQuotedConfigContext) {
            return StringUtils.unwrap(key.getText(), "`");
        }
        return key.getText();
    }

    public static Operation getObjectOperation(String opera, String objectType) {
        Operation operation;
        if (!checkOperationGrammar(opera, objectType)) {
            throw new CarbonSqlException("not supported privilege operation");
        }

        if (opera.equalsIgnoreCase("create") || opera.equalsIgnoreCase("show")) {
            if (objectType.equalsIgnoreCase("catalog")) {
                operation = Operation.valueOf((opera + "_" + "database").toUpperCase());
            } else if (objectType.equalsIgnoreCase("database")) {
                operation = Operation.valueOf((opera + "_" + "table").toUpperCase());
            } else if (objectType.equalsIgnoreCase("branch")) {
                operation = Operation.valueOf((opera + "_" + "branch").toUpperCase());
            } else {
                throw new CarbonSqlException("not supported granted privilege");
            }
        } else if (opera.equalsIgnoreCase("createbranch")) {
            operation = Operation.valueOf("CREATE_BRANCH");
        } else if (opera.equalsIgnoreCase("createtable")) {
            operation = Operation.valueOf("CREATE_TABLE");
        } else if (opera.equalsIgnoreCase("createview")) {
            operation = Operation.valueOf("CREATE_VIEW");
        } else {
            operation = Operation.valueOf((opera + "_" + objectType).toUpperCase());
        }

        return operation;
    }

    private static boolean checkOperationGrammar(String opera, String objectType) {
        for (Operation operation : Operation.values()) {
            if (operation.getPrintName().equalsIgnoreCase(opera + " " + objectType)
                || operation.getPrintName().replaceAll(" ", "").equalsIgnoreCase(opera)) {
                return true;
            }
        }
        return false;
    }

    public static String parseStreamName(QualifiedNameContext qualifiedNameContext) {
        List<String> streamNames = parseQualifiedName(qualifiedNameContext);
        if (streamNames.size() != 1) {
            throw new CarbonSqlException("stream name must be a single identifier");
        }
        return streamNames.get(0);
    }

    public static String parseDropStream(DropStreamContext ctx) {
        return parseStreamName(ctx.qualifiedName());
    }

    public static CreateDelegateRequest buildCreateDelegateRequest(CreateDelegateContext ctx) {
        DelegateInput delegateInput = new DelegateInput();
        delegateInput.setDelegateName(ctx.delegateName.getText());
        delegateInput.setStorageProvider(ctx.provider.getText());
        delegateInput.setProviderDomainName(ctx.domainName.getText());
        delegateInput.setAgencyName(ctx.agencyName.getText());
        if (ctx.allowedLocations != null) {
            delegateInput.setAllowedLocationList(parseStringListContext(ctx.allowedLocations.string()));
        } else {
            delegateInput.setAllowedLocationList(Collections.emptyList());
        }
        if (ctx.blockedLocations != null) {
            delegateInput.setBlockedLocationList(parseStringListContext(ctx.blockedLocations.string()));
        } else {
            delegateInput.setBlockedLocationList(new ArrayList<>());
        }
        CreateDelegateRequest request = new CreateDelegateRequest();
        request.setInput(delegateInput);
        return request;
    }

    public static GetDelegateRequest buildGetDelegateRequest(DescDelegateContext ctx) {
        return new GetDelegateRequest(parseIdentifier(ctx.delegateName));
    }

    public static DeleteDelegateRequest buildDeleteDelegateRequest(DropDelegateContext ctx) {
        return new DeleteDelegateRequest(parseIdentifier(ctx.delegateName));
    }

    public static AlterColumnRequest buildAlterColumnRequest(AlterColumnContext ctx) {
        TableNameInput tableName = parseTableName(ctx.tableName());
        ColumnActionContext colAction = ctx.columnAction();
        if (colAction instanceof AddColumnsContext) {
            ColumnChangeInput columnChangeInput = new ColumnChangeInput();
            columnChangeInput.setChangeType(Operation.ADD_COLUMN);
            AddColumnsContext context = (AddColumnsContext) colAction;

            List<Column> columns = context.columnDefinition().stream()
                .map(ParserUtil::parseColumnDefinition).collect(Collectors.toList());
            columnChangeInput.setColumnList(columns);

            return new AlterColumnRequest(tableName, columnChangeInput);
        } else if (colAction instanceof ReplaceColumnsContext) {
            throw new UnsupportedOperationException();
        } else if (colAction instanceof ChangeColumnsContext) {
            ColumnChangeInput columnChangeInput = new ColumnChangeInput();
            columnChangeInput.setChangeType(Operation.CHANGE_COLUMN);
            ChangeColumnsContext context = (ChangeColumnsContext) colAction;

            HashMap<String, Column> changeMap = new HashMap<>();
            for (int i = 0, eleNum = context.identifier().size(); i < eleNum; i++) {
                String oldColName = parseIdentifier(context.identifier(i));
                Column newColInput = ParserUtil.parseColumnDefinition(context.columnDefinition(i));
                changeMap.put(oldColName, newColInput);
            }
            columnChangeInput.setChangeColumnMap(changeMap);
            return new AlterColumnRequest(tableName, columnChangeInput);
        } else if (colAction instanceof RenameColumnsContext) {
            ColumnChangeInput columnChangeInput = new ColumnChangeInput();
            columnChangeInput.setChangeType(Operation.RENAME_COLUMN);
            RenameColumnsContext context = (RenameColumnsContext) colAction;

            HashMap<String, String> renameMap = new HashMap<>();
            for (int i = 0, eleNum = context.identifier().size(); i < eleNum; i += 2) {
                String oldCol = parseIdentifier(context.identifier(i));
                String newCol = parseIdentifier(context.identifier(i + 1));
                renameMap.put(oldCol, newCol);
            }
            columnChangeInput.setRenameColumnMap(renameMap);

            return new AlterColumnRequest(tableName, columnChangeInput);
        } else if (colAction instanceof DropColumnsContext) {
            ColumnChangeInput columnChangeInput = new ColumnChangeInput();
            columnChangeInput.setChangeType(Operation.DROP_COLUMN);
            DropColumnsContext context = (DropColumnsContext) colAction;

            List<String> dropList = new ArrayList<>(context.identifier().size());
            for (int i = 0, eleNum = context.identifier().size(); i < eleNum; i++) {
                String dropCol = ParserUtil.parseIdentifier(context.identifier(i));
                dropList.add(dropCol);
            }
            columnChangeInput.setDropColumnList(dropList);

            return new AlterColumnRequest(tableName, columnChangeInput);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public static GetTableUsageProfileRequest buildGetTableUsageProfileRequest(DescAccessStatsForTableContext ctx) {
        GetTableUsageProfileRequest request = new GetTableUsageProfileRequest();
        TableNameInput nameInput = parseTableName(ctx.tableName());
        request.setCatalogName(nameInput.getCatalogName());
        request.setDatabaseName(nameInput.getDatabaseName());
        request.setTableName(nameInput.getTableName());
        try {
            List<Long> timestamps = parseTimeStamp(ctx.startTimestamp, ctx.endTimestamp);
            if (timestamps.size() == 2) {
                request.setStartTimestamp(timestamps.get(0));
                request.setEndTimestamp(timestamps.get(1));
            }
        } catch (CarbonSqlException e) {
            throw new CarbonSqlException("unsupported startTimestamp|endTimestamp input");
        }
        return request;
    }

    public static List<Long> parseTimeStamp(StringContext startTimestamp,
        StringContext endTimestamp) {
        List<Long> timestamps = new ArrayList<>();
        if (Objects.nonNull(startTimestamp) && Objects.nonNull(endTimestamp)) {
            String start = parseString(startTimestamp);
            String end = parseString(endTimestamp);
            timestamps.add(Timestamp.valueOf(start).getTime());
            timestamps.add(Timestamp.valueOf(end).getTime());
        }
        return timestamps;
    }

    public static ListDataLineageRequest buildListDataLineageRequest(ShowDataLineageForTableContext ctx) {
        ListDataLineageRequest request = new ListDataLineageRequest();
        TableNameInput nameInput = parseTableName(ctx.tableName());
        request.setCatalogName(nameInput.getCatalogName());
        request.setDatabaseName(nameInput.getDatabaseName());
        request.setTableName(nameInput.getTableName());
        if (Objects.nonNull(ctx.lineageType)) {
            request.setLineageType(ctx.lineageType.getText());
        } else {
            request.setLineageType(DataLineageType.UPSTREAM.toString());
        }
        return request;
    }

    public static SetTablePropertyRequest buildSetTablePropertyRequest(SetTablePropertiesContext ctx) {
        TableNameInput tableName = parseTableName(ctx.tableName());

        SetTablePropertyInput input = new SetTablePropertyInput();
        input.setSetProperties(parseProperties(ctx.properties()));

        SetTablePropertyRequest request = new SetTablePropertyRequest();
        request.setTableName(tableName.getTableName());
        request.setCatalogName(tableName.getCatalogName());
        request.setDatabaseName(tableName.getDatabaseName());
        request.setInput(input);
        return request;
    }

    public static UnsetTablePropertyRequest buildUnsetTablePropertyRequest(UnsetTablePropertiesContext ctx) {
        TableNameInput tableName = parseTableName(ctx.tableName());

        UnsetTablePropertyInput input = new UnsetTablePropertyInput();
        input.setUnsetProperties(parseStringListContext(ctx.keys().string()));

        UnsetTablePropertyRequest request = new UnsetTablePropertyRequest();
        request.setTableName(tableName.getTableName());
        request.setCatalogName(tableName.getCatalogName());
        request.setDatabaseName(tableName.getDatabaseName());
        request.setInput(input);
        return request;
    }

    public static List<String> buildPartitionColumns(CreateTableAsSelectContext ctx) {
        List<String> ptColNames = new ArrayList<>();
        if (ctx.sparkPartition() != null) {
            ptColNames.addAll(
                ctx.sparkPartition().stream().map(ParserUtil::parseSparkPartition).collect(Collectors.toList()));
        }
        return ptColNames;
    }

    public static GetTableRequest buildGetTableRequest(PolyCatSQLParser.CreateTableLikeContext ctx) {
        TableNameInput tableNameInput = parseTableName(ctx.table);
        GetTableRequest request = new GetTableRequest(tableNameInput.getTableName());
        request.setCatalogName(tableNameInput.getCatalogName());
        request.setDatabaseName(tableNameInput.getDatabaseName());
        return request;
    }

    public static CreateTableRequest buildCreateTableRequest(PolyCatSQLParser.CreateTableLikeContext ctx) {
        TableNameInput tableNameInput = parseTableName(ctx.tableName(0));
        CreateTableRequest request = buildCreateTableRequest(tableNameInput, null, true,
            TableTypeInput.EXTERNAL_TABLE, null, ctx.source, ctx.properties());
        if (ctx.location != null) {
            request.getInput().getStorageDescriptor().setLocation(parseString(ctx.location));
        }
        return request;
    }

    public static CreateTableRequest buildCreateTableRequest(CreateTableContext ctx) {
        List<Column> normalColumns = ctx.tableElement().stream()
            .map(ParserUtil::parseTableElement).collect(Collectors.toList());
        List<Column> partColumns = parsePartitionColumns(ctx.partitionDefinition(), normalColumns);
        Boolean transaction = parseBooleanDefaultFalse(ctx.booleanValue());
        TableNameInput tableName = parseTableName(ctx.createTableHeader().tableName());
        TableTypeInput tableType = parseTableType(ctx.createTableHeader().EXTERNAL(), ctx.location);

        CreateTableRequest request = buildCreateTableRequest(tableName, normalColumns, partColumns, transaction,
            tableType, ctx.comment, ctx.source, ctx.properties());
        if (ctx.location != null) {
            request.getInput().getStorageDescriptor().setLocation(parseString(ctx.location));
        }
        return request;
    }

    private static List<Column> parsePartitionColumns(List<PartitionDefinitionContext> partDefCtx,
        List<Column> normalColumns) {
        ColType partStyle = validatePartitionDef(partDefCtx);
        List<Column> partColumns;
        if (partStyle == ColType.HIVE_STYLE_PARTITION) {
            partColumns = parseHivePartitions(partDefCtx);
        } else if (partStyle == ColType.SPARK_STYLE_PARTITION) {
            partColumns = parseSparkPartitions(normalColumns, partDefCtx);
            if (normalColumns.size() == 0) {
                throw new CarbonSqlException("Setting all columns as partition columns is FORBIDDEN");
            }
        } else if (partStyle == ColType.NON_PARTITIONED_TABLE) {
            partColumns = null;
        } else {
            throw new UnsupportedOperationException();
        }
        return partColumns;
    }

    private static ColType validatePartitionDef(List<PartitionDefinitionContext> partitionDefinition) {
        ColType partStyle = ColType.NON_PARTITIONED_TABLE;
        boolean hasHivePartDef = false;
        boolean hasSparkPartDef = false;
        for (PartitionDefinitionContext ctx : partitionDefinition) {
            if (ctx.hivePartition() != null) {
                hasHivePartDef = true;
                partStyle = ColType.HIVE_STYLE_PARTITION;
            } else if (ctx.sparkPartition() != null) {
                hasSparkPartDef = true;
                partStyle = ColType.SPARK_STYLE_PARTITION;
            }
            if (hasHivePartDef && hasSparkPartDef) {
                throw new CarbonSqlException("Mix styles of hive and spark partition definition is not allowed");
            }
        }
        return partStyle;
    }

    private static List<Column> parseHivePartitions(List<PartitionDefinitionContext> partitionCtx) {
        List<Column> partColList = new ArrayList<>();
        if (partitionCtx != null) {
            partitionCtx.forEach(x -> {
                Column col = new Column();
                col.setColumnName(parseIdentifier(x.hivePartition().identifier()));
                col.setColType(parseType(x.hivePartition().type()));
                col.setComment(parseString(x.hivePartition().string()));
                partColList.add(col);
            });
        }
        return partColList;
    }

    private static List<Column> parseSparkPartitions(List<Column> normalColumns,
        List<PartitionDefinitionContext> partitionCtx) {
        // split partition columns in partList and normal data columns in normalColumns
        List<Column> partList = new ArrayList<>();
        HashMap<String, Column> columnMap = new HashMap<>();
        normalColumns.forEach(x -> {
            if (columnMap.containsKey(x.getColumnName())) {
                throw new CarbonSqlException("The table columns exist duplicate column name");
            }
            columnMap.put(x.getColumnName(), x);
        });
        if (partitionCtx != null) {
            partitionCtx.forEach(x -> {
                Column col = new Column();
                col.setColumnName(parseIdentifier(x.sparkPartition().identifier()));
                if (!columnMap.containsKey(col.getColumnName())) {
                    throw new CarbonSqlException("The table columns don't contain partition column");
                }
                Column tableColumn = columnMap.get(col.getColumnName());
                col.setColType(tableColumn.getColType());
                partList.add(col);
                normalColumns.remove(tableColumn);
            });
        }
        return partList;
    }

    public static DeleteTableRequest buildDeleteTableRequest(DropTableContext ctx) {
        DeleteTableRequest request = new DeleteTableRequest();
        TableNameInput tableName = parseTableName(ctx.tableName());

        request.setTableName(tableName.getTableName());
        request.setCatalogName(tableName.getCatalogName());
        request.setDatabaseName(tableName.getDatabaseName());
        request.setPurgeFlag(ctx.PURGE() != null);
        return request;
    }

    public static UndropTableRequest buildUndropTableRequest(UndropTableContext ctx) {
        UndropTableRequest request = new UndropTableRequest();
        TableNameInput tableName = parseTableName(ctx.tableName());

        request.setTableName(tableName.getTableName());
        request.setCatalogName(tableName.getCatalogName());
        request.setDatabaseName(tableName.getDatabaseName());
        if (ctx.idProperty() != null) {
            request.setTableId(parseString(ctx.idProperty().string()));
        }
        return request;
    }

    public static PurgeTableRequest buildPurgeTableRequest(PurgeTableContext ctx) {
        PurgeTableRequest request = new PurgeTableRequest();
        TableNameInput tableName = parseTableName(ctx.tableName());

        request.setTableName(tableName.getTableName());
        request.setCatalogName(tableName.getCatalogName());
        request.setDatabaseName(tableName.getDatabaseName());
        if (ctx.idProperty() != null) {
            request.setTableId(parseString(ctx.idProperty().string()));
        }

        if (ctx.IF() != null && ctx.EXISTS() != null) {
            request.setExist(true);
        } else {
            request.setExist(false);
        }
        return request;
    }

    public static RestoreTableRequest buildRestoreTableRequest(RestoreTableWithVerContext ctx) {
        RestoreTableRequest request = new RestoreTableRequest();
        TableNameInput tableName = parseTableName(ctx.tableName());

        request.setTableName(tableName.getTableName());
        request.setCatalogName(tableName.getCatalogName());
        request.setDatabaseName(tableName.getDatabaseName());
        request.setVersion(parseString(ctx.string()));
        return request;
    }

    public static ListTableCommitsRequest buildListTableCommitsRequest(ShowTableHistoryContext ctx) {
        ListTableCommitsRequest request = new ListTableCommitsRequest();
        TableNameInput tableNameInput = parseTableName(ctx.tableName());

        request.setCatalogName(tableNameInput.getCatalogName());
        request.setDatabaseName(tableNameInput.getDatabaseName());
        request.setTableName(tableNameInput.getTableName());
        return request;
    }

    public static ListTablePartitionsRequest buildListTablePartitionsRequest(ShowTablePartitionsContext ctx) {
        ListTablePartitionsRequest request = new ListTablePartitionsRequest();
        PartitionFilterInput filterInput = new PartitionFilterInput();
        filterInput.setFilter("");
        request.setInput(filterInput);

        TableNameInput tableNameInput = parseTableName(ctx.tableName());

        request.setCatalogName(tableNameInput.getCatalogName());
        request.setDatabaseName(tableNameInput.getDatabaseName());
        request.setTableName(tableNameInput.getTableName());
        return request;
    }

    public static AlterTableRequest buildAlterTableRequest(RenameTableContext ctx) {
        AlterTableRequest request = new AlterTableRequest();
        TableInput tableInput = new TableInput();
        tableInput.setTableName(ctx.tableName(1).table.getText().toLowerCase());
        request.setInput(new AlterTableInput(tableInput, null));

        TableNameInput tableNameInput = parseTableName(ctx.tableName(0));
        request.setCatalogName(tableNameInput.getCatalogName());
        request.setDatabaseName(tableNameInput.getDatabaseName());
        request.setTableName(tableNameInput.getTableName());
        return request;
    }

    public static ListCatalogCommitsRequest buildListCatalogCommitsRequest(ShowCatalogHistoryContext ctx) {
        ListCatalogCommitsRequest request = new ListCatalogCommitsRequest();
        request.setCatalogName(parseIdentifier(ctx.identifier()));
        return request;
    }

    public static ListCatalogUsageProfilesRequest buldListCatalogUsageProfilesRequest(
        ShowAccessStatsForCatalogContext ctx) {
        ListCatalogUsageProfilesRequest request = new ListCatalogUsageProfilesRequest();

        int limit = 10;
        try {
            if (Objects.nonNull(ctx.maximumToScan)) {
                limit = Integer.parseInt(ctx.maximumToScan.getText());
            }
        } catch (NumberFormatException e) {
            throw new CarbonSqlException("unsupported limit input");
        }

        request.setCatalogName(parseIdentifier(ctx.identifier()));
        request.setOpType(ctx.opType.getText());
        request.setLimit(limit);
        request.setUsageProfileType(Objects.nonNull(ctx.DESC())
            ? TopTableUsageProfileType.TOP_HOT_TABLES.getTopTypeId()
            : TopTableUsageProfileType.TOP_COLD_TABLES
                .getTopTypeId());
        try {
            List<Long> timestamps = parseTimeStamp(ctx.startTimestamp, ctx.endTimestamp);
            if (timestamps.size() == 2) {
                request.setStartTimestamp(timestamps.get(0));
                request.setEndTimestamp(timestamps.get(1));
            }
        } catch (CarbonSqlException e) {
            throw new CarbonSqlException("unsupported startTimestamp|endTimestamp input");
        }
        return request;
    }

    public static CreateAcceleratorRequest buildCreateAcceleratorRequest(CreateAcceleratorContext ctx) {
        TableNameInput tableName = parseTableName(ctx.accName().tableName());
        // build input
        AcceleratorInput input = new AcceleratorInput();
        input.setName(tableName.getTableName());
        String useLib = parseIdentifier(ctx.lib);
        input.setLib(useLib);
        QueryNoWithContext queryNoWithContext = ctx.queryNoWith();
        input.setSqlStatement(sourceTextForContext(queryNoWithContext));
        // build request
        CreateAcceleratorRequest request = new CreateAcceleratorRequest();
        request.setInput(input);
        request.setCatalogName(tableName.getDatabaseNameInput().getCatalogName());
        request.setDatabaseName(tableName.getDatabaseNameInput().getDatabaseName());
        return request;
    }

    public static DropAcceleratorRequest buildDropAcceleratorRequest(DropAcceleratorContext ctx) {
        TableNameInput tableName = parseTableName(ctx.accName().tableName());
        DropAcceleratorRequest request = new DropAcceleratorRequest();
        request.setCatalogName(tableName.getDatabaseNameInput().getCatalogName());
        request.setDatabaseName(tableName.getDatabaseNameInput().getDatabaseName());
        request.setAcceleratorName(tableName.getTableName());
        return request;
    }

    public static ListDelegatesRequest buildListDelegatesRequest(ShowDelegatesContext ctx) {
        return new ListDelegatesRequest(parseString(ctx.pattern));
    }

    public static AlterRoleRequest buildAlterRoleRequest(GrantRoleToUserContext ctx) {
        RoleInput roleInput = buildRoleInput(ctx.role, ctx.user);
        AlterRoleRequest alterRoleRequest = new AlterRoleRequest();
        alterRoleRequest.setInput(roleInput);
        return alterRoleRequest;
    }

    public static AlterRoleRequest buildAlterRoleRequest(GrantPrivilegeToRoleContext ctx) {
        RoleInput roleInput = buildRoleInput(ctx.principal(), ctx.privilege(), ctx.objectType(), ctx.qualifiedName());
        AlterRoleRequest alterRoleRequest = new AlterRoleRequest();
        alterRoleRequest.setInput(roleInput);
        return alterRoleRequest;
    }

    public static AlterShareRequest buildAlterShareRequest(GrantShareToUserContext ctx) {
        ShareInput shareInput = ParserUtil.buildShareInput(ctx.share, ctx.user);
        AlterShareRequest request = new AlterShareRequest();
        request.setInput(shareInput);
        return request;
    }

    public static Boolean parseIfNotExists(CreateDatabaseContext ctx) {
        return ctx.IF() != null && ctx.NOT() != null && ctx.EXISTS() != null;
    }

    public static AlterDatabaseRequest buildAlterDbPropertiesRequest(AlterDbPropertiesContext ctx) {
        AlterDatabaseRequest request = new AlterDatabaseRequest();
        DatabaseNameInput databaseName = parseDatabaseName(ctx.databaseName());
        DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setDatabaseName(databaseName.getDatabaseName());
        databaseInput.setParameters(parseProperties(ctx.properties()));

        request.setInput(databaseInput);
        request.setCatalogName(databaseName.getCatalogName());
        request.setDatabaseName(databaseName.getDatabaseName());
        return request;
    }


    public static AlterDatabaseRequest buildAlterDbLocationRequest(AlterDbLocationContext ctx) {
        AlterDatabaseRequest request = new AlterDatabaseRequest();
        DatabaseNameInput databaseName = parseDatabaseName(ctx.databaseName());
        DatabaseInput databaseInput = new DatabaseInput();
        databaseInput.setDatabaseName(databaseName.getDatabaseName());
        databaseInput.setLocationUri(parseString(ctx.location));

        request.setInput(databaseInput);
        request.setCatalogName(databaseName.getCatalogName());
        request.setDatabaseName(databaseName.getDatabaseName());
        return request;
    }

    public static DropPartitionRequest buildDropPartitionRequest(DropPartitionContext ctx, DropPartitionInput input) {
        TableNameInput tableName = parseTableName(ctx.tableName());
        DropPartitionRequest request = new DropPartitionRequest();
        request.setInput(input);
        request.setCatalogName(tableName.getCatalogName());
        request.setDatabaseName(tableName.getDatabaseName());
        request.setTableName(tableName.getTableName());

        if (ctx.IF() != null && ctx.EXISTS() != null) {
            request.setIgnoreUnknownPart(true);
        }
        if (ctx.PURGE() != null) {
            throw new UnsupportedOperationException("syntax [DROP PARTITION PURGE] does not supported yet");
        }
        return request;
    }

    public static Boolean parseIfNotExists(AddPartitionContext ctx) {
        return ctx.IF() != null && ctx.NOT() != null && ctx.EXISTS() != null;
    }

    public static MaterializedViewNameInput parseMaterializedViewName(
        PolyCatSQLParser.MaterializedViewNameContext ctx) {
        if (ctx == null) {
            return new MaterializedViewNameInput();
        }
        MaterializedViewNameInput materializedViewInput = new MaterializedViewNameInput();
        materializedViewInput.setMaterializedViewName(parseIdentifier(ctx.materializedView));
        materializedViewInput.setDatabaseNameInput(parseDatabaseName(ctx.databaseName()));
        return materializedViewInput;
    }

    public static CreateMaterializedViewRequest buildCreateMaterializedViewRequest(
        PolyCatSQLParser.CreateMaterializedViewContext ctx) {
        MaterializedViewNameInput materializedViewInput =
            parseMaterializedViewName(ctx.materializedViewName());
        return buildCreateMaterializedViewRequest(materializedViewInput, ctx.properties());
    }

    protected static CreateMaterializedViewRequest buildCreateMaterializedViewRequest(
        MaterializedViewNameInput mvInput, PropertiesContext properties) {
        CreateMaterializedViewRequest createMaterializedViewRequest =
            new CreateMaterializedViewRequest();
        IndexInput materializedViewInput = new IndexInput();
        materializedViewInput.setName(mvInput.getMaterializedViewName());
        Map<String, String> mvProperties = parseProperties(properties);
        if (null == mvProperties) {
            mvProperties = new HashMap<>();
        }
        materializedViewInput.setProperties(mvProperties);
        createMaterializedViewRequest.setDatabaseName(
            createMaterializedViewRequest.getDatabaseName());
        createMaterializedViewRequest.setCatalogName(
            createMaterializedViewRequest.getCatalogName());
        createMaterializedViewRequest.setInput(materializedViewInput);
        return createMaterializedViewRequest;
    }

    public static DropMaterializedViewRequest buildDropMaterializedViewRequest(
        PolyCatSQLParser.DropMaterializedViewContext ctx) {
        MaterializedViewNameInput materializedViewInput =
            parseMaterializedViewName(ctx.materializedViewName());

        return new DropMaterializedViewRequest(materializedViewInput.getCatalogName(),
            materializedViewInput.getDatabaseName(),
            materializedViewInput.getMaterializedViewName());
    }

    public static RefreshMaterializedViewRequest buildRefreshMaterializedViewRequest(
        PolyCatSQLParser.RefreshMaterializedViewContext ctx) {
        MaterializedViewNameInput materializedViewInput =
            parseMaterializedViewName(ctx.materializedViewName());

        return new RefreshMaterializedViewRequest(materializedViewInput.getCatalogName(),
            materializedViewInput.getDatabaseName(),
            materializedViewInput.getMaterializedViewName());
    }

    public static ListMaterializedViewsRequest buildShowMaterializedViewRequest(
        PolyCatSQLParser.ShowMaterializedViewContext ctx) {
        ListMaterializedViewsRequest request = new ListMaterializedViewsRequest();
        request.setIncludeDrop(ctx.ALL() != null);
        if (ctx.tableName() != null) {
           request.setTableName(ctx.tableName().table.getText());
            if(ctx.tableName().databaseName() != null) {
                request.setDatabaseName(ctx.tableName().databaseName().database.getText());
            }
        }
        if (ctx.maximumToScan != null) {
            request.setMaxResults(Integer.parseInt(ctx.maximumToScan.getText()));
        }
        return request;
    }
}
