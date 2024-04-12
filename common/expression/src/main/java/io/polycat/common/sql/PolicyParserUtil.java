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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.polycat.catalog.common.Operation;
import io.polycat.catalog.common.exception.CarbonSqlException;
import io.polycat.catalog.common.model.PrincipalSource;
import io.polycat.catalog.common.model.PrincipalType;
import io.polycat.catalog.common.plugin.request.AlterPrivilegeRequest;
import io.polycat.catalog.common.plugin.request.AlterRoleRequest;
import io.polycat.catalog.common.plugin.request.AlterShareRequest;
import io.polycat.catalog.common.plugin.request.CreateShareRequest;
import io.polycat.catalog.common.plugin.request.ShowPoliciesOfPrincipalRequest;
import io.polycat.catalog.common.plugin.request.input.PolicyInput;
import io.polycat.catalog.common.plugin.request.input.RoleInput;
import io.polycat.catalog.common.plugin.request.input.ShareInput;
import io.polycat.sql.PolyCatSQLParser.BooleanExpressionContext;
import io.polycat.sql.PolyCatSQLParser.BooleanValueContext;
import io.polycat.sql.PolyCatSQLParser.ColumnFilterContext;
import io.polycat.sql.PolyCatSQLParser.CreateShareOnCatalogContext;
import io.polycat.sql.PolyCatSQLParser.DataMaskContext;
import io.polycat.sql.PolyCatSQLParser.GrantPrivilegeToPrincipalContext;
import io.polycat.sql.PolyCatSQLParser.GrantRsPrincipalToUgPrincipalContext;
import io.polycat.sql.PolyCatSQLParser.IdentifierContext;
import io.polycat.sql.PolyCatSQLParser.PrincipalInfoContext;
import io.polycat.sql.PolyCatSQLParser.PrincipalSourceContext;
import io.polycat.sql.PolyCatSQLParser.PrivilegeContext;
import io.polycat.sql.PolyCatSQLParser.PropertiesContext;
import io.polycat.sql.PolyCatSQLParser.PropertyContext;
import io.polycat.sql.PolyCatSQLParser.QualifiedNameContext;
import io.polycat.sql.PolyCatSQLParser.RevokePrivilegeFromPrincipalContext;
import io.polycat.sql.PolyCatSQLParser.RevokeRsPrincipalFromUgPrincipalContext;
import io.polycat.sql.PolyCatSQLParser.RsPrincipalInfoContext;
import io.polycat.sql.PolyCatSQLParser.ShareFullNameContext;
import io.polycat.sql.PolyCatSQLParser.ShowPoliciesOfPrincipalContext;
import io.polycat.sql.PolyCatSQLParser.UgPrincipalInfoContext;

import org.antlr.v4.runtime.tree.TerminalNode;

public class PolicyParserUtil {

    private static String parseUgPrincipalType(UgPrincipalInfoContext principalTypeContext) {
        String principalType;
        switch (principalTypeContext.getChild(0).getText().toUpperCase()) {
            case "USER":
                principalType = PrincipalType.USER.name();
                break;
            case "GROUP":
                principalType = PrincipalType.GROUP.name();
                break;
            default:
                throw new CarbonSqlException("not supported principal type");
        }
        return principalType;
    }

    private static String parseRsPrincipalType(RsPrincipalInfoContext principalTypeContext) {
        String principalType;
        switch (principalTypeContext.getChild(0).getText().toUpperCase()) {
            case "ROLE":
                principalType = PrincipalType.ROLE.name();
                break;
            case "SHARE":
                principalType = PrincipalType.SHARE.name();
                break;
            default:
                throw new CarbonSqlException("not supported principal type");
        }
        return principalType;
    }

    private static String parsePrincipalSource(PrincipalSourceContext principalSourceContext) {
        String principalSource;
        switch (principalSourceContext.getChild(0).getText().toUpperCase()) {
            case "IAM":
                principalSource = PrincipalSource.IAM.name();
                break;
            case "SAML":
                principalSource = PrincipalSource.SAML.name();
                break;
            case "LDAP":
                principalSource = PrincipalSource.LDAP.name();
                break;
            default:
                throw new CarbonSqlException("not supported principal source");
        }
        return principalSource;
    }

    private static String parseConditions(PropertiesContext ctx) {
        if (ctx == null) {
            return null;
        }
        List<String> conditionList = new ArrayList<>();
        List<PropertyContext> properties = ctx.property();
        for (PropertyContext context : properties) {
            String key = ParserUtil.parseString(context.key);
            String value = ParserUtil.parseString(context.value);
            String condition = key+"="+value;
            conditionList.add(condition);
        }
        String result = conditionList.stream().collect(Collectors.joining(","));
        return result;
    }

    private static String[] parseColMask(DataMaskContext ctx) {
        if (ctx == null) {
            return null;
        }
        String dataMask = ParserUtil.sourceTextForContext(ctx);
        String[] colFilterArr= dataMask.split(";");
        return colFilterArr;
    }

    private static String generateObligation(String rowFilter, String colFilter, String[] dataMask) {
        List<String> obligations = new ArrayList<>();
        if (rowFilter != null) {
            String rowFilterStr = "ROWFILTER:"+ rowFilter;
            obligations.add(rowFilterStr);
        }
        if (colFilter != null) {
            String colFilterStr = "COLFILTER:"+ colFilter;
            obligations.add(colFilterStr);
        }
        if (dataMask != null) {
            for (String colMask : dataMask) {
                String colMaskStr = "DATAMASK:" + colMask;
                obligations.add(colMaskStr);
            }
        }
        if (obligations.size() > 0) {
            return obligations.stream().collect(Collectors.joining(";"));
        }
        return null;
    }

    private static PolicyInput buildPolicyInput(PrincipalInfoContext principalInfoCtx,
        List<Operation> operationList, String objectType,
        QualifiedNameContext objectNameCtx, BooleanValueContext effectCtx,
        PropertiesContext conditionsCtx, BooleanExpressionContext rowFilterCtx,
        ColumnFilterContext colFilterCtx, DataMaskContext dataMaskCtx,
        TerminalNode with_grant) {

        PolicyInput policyInput = new PolicyInput();
        String principalType;
        String principalSource;
        String principalName;
        if (principalInfoCtx.ugPrincipalInfo() != null) {
            principalType = parseUgPrincipalType(principalInfoCtx.ugPrincipalInfo());
            principalSource = parsePrincipalSource(principalInfoCtx.ugPrincipalInfo().principalSource());
            principalName = ParserUtil.parseIdentifier(principalInfoCtx.ugPrincipalInfo().identifier());
        } else {
            principalType = parseRsPrincipalType(principalInfoCtx.rsPrincipalInfo());
            principalSource = "IAM";
            principalName = ParserUtil.parseIdentifier(principalInfoCtx.rsPrincipalInfo().identifier());
        }

        String objectName = ParserUtil.buildObjectName(objectNameCtx);
        boolean effect = ParserUtil.parseBooleanDefaultFalse(effectCtx);

        String conditions = null;
        String rowFilter = null;
        String colFilter = null;
        String[] colMask = null;
        if (conditionsCtx != null) {
            conditions = parseConditions(conditionsCtx);
        }
        if (rowFilterCtx != null) {
            rowFilter = ParserUtil.sourceTextForContext(rowFilterCtx);
        }
        if (colFilterCtx != null) {
            colFilter = ParserUtil.sourceTextForContext(colFilterCtx);
        }
        if (dataMaskCtx != null) {
            colMask = parseColMask(dataMaskCtx);
        }
        String obligations = generateObligation(rowFilter,colFilter,colMask);
        boolean grantAble = false;
        if (with_grant != null) {
            grantAble = true;
        }
        policyInput.setPrincipalType(principalType);
        policyInput.setPrincipalSource(principalSource);
        policyInput.setPrincipalName(principalName);
        policyInput.setOperationList(operationList);
        policyInput.setObjectType(objectType);
        policyInput.setObjectName(objectName);
        policyInput.setEffect(effect);
        policyInput.setOwner(false);
        policyInput.setCondition(conditions);
        policyInput.setObligation(obligations);
        policyInput.setGrantAble(grantAble);

        return policyInput;
    }

    public static AlterPrivilegeRequest buildAlterPrivilegeRequest(GrantPrivilegeToPrincipalContext ctx) {
        String objectType;
        if (ParserUtil.parseObjectType(ctx.objectType()).equals("branch")) {
            objectType = "catalog";
        } else {
            objectType = ParserUtil.parseObjectType(ctx.objectType());
        }
        List<Operation> operationList = new ArrayList<>();
        List<PrivilegeContext> privileges = ctx.privileges().privilege();
        for (PrivilegeContext privilege : privileges) {
            Operation operation = ParserUtil.getObjectOperation(ParserUtil.parsePrivilege(privilege), objectType);
            operationList.add(operation);
        }
        PolicyInput policyInput  = buildPolicyInput(ctx.principalInfo(), operationList, objectType,
            ctx.qualifiedName(), ctx.booleanValue(),
            ctx.conditions, ctx.rowFilter, ctx.colFilter, ctx.colMask, ctx.WITH_GRANT());

        AlterPrivilegeRequest alterPrivilegeRequest = new AlterPrivilegeRequest();
        alterPrivilegeRequest.setInput(policyInput);
        return alterPrivilegeRequest;
    }


    public static AlterPrivilegeRequest buildAlterPrivilegeRequest(RevokePrivilegeFromPrincipalContext ctx) {
        String objectType;
        if (ParserUtil.parseObjectType(ctx.objectType()).equals("branch")) {
            objectType = "catalog";
        } else {
            objectType = ParserUtil.parseObjectType(ctx.objectType());
        }
        List<Operation> operationList = new ArrayList<>();
        List<PrivilegeContext> privileges = ctx.privileges().privilege();
        for (PrivilegeContext privilege : privileges) {
            Operation operation = ParserUtil.getObjectOperation(ParserUtil.parsePrivilege(privilege), objectType);
            operationList.add(operation);
        }
        PolicyInput policyInput  = buildPolicyInput(ctx.principalInfo(), operationList, objectType,
            ctx.qualifiedName(), ctx.booleanValue(),
            ctx.conditions, ctx.rowFilter, ctx.colFilter, ctx.colMask, ctx.WITH_GRANT());

        AlterPrivilegeRequest alterPrivilegeRequest = new AlterPrivilegeRequest();
        alterPrivilegeRequest.setInput(policyInput);
        return alterPrivilegeRequest;
    }

    public static RoleInput buildNewRoleInput(IdentifierContext role, UgPrincipalInfoContext UgPrincipal) {

        RoleInput roleInput = new RoleInput();
        String principalType = parseUgPrincipalType(UgPrincipal);
        String principalSource = parsePrincipalSource(UgPrincipal.principalSource());
        String principalName = ParserUtil.parseIdentifier(UgPrincipal.identifier());
        String[] userIds = new String[1];
        String userId = principalType + ":" + principalSource + ":" + principalName;

        roleInput.setRoleName(ParserUtil.parseIdentifier(role));
        userIds[0] = userId;
        roleInput.setUserId(userIds);
        return roleInput;
    }


    public static ShareInput buildGlobalShareInput(ShareFullNameContext share, UgPrincipalInfoContext UgPrincipal) {
        ShareInput shareInput = new ShareInput();
        String principalType = parseUgPrincipalType(UgPrincipal);
        String principalSource = parsePrincipalSource(UgPrincipal.principalSource());
        String principalName = ParserUtil.parseIdentifier(UgPrincipal.identifier());
        String[] users = new String[1];
        String user = principalType + ":" + principalSource + ":" + principalName;

        shareInput.setProjectId(ParserUtil.parseIdentifier(share.shareproject));
        shareInput.setShareName(ParserUtil.parseIdentifier(share.sharecatalog));
        users[0] = user;
        shareInput.setUsers(users);
        return shareInput;
    }


    public static AlterRoleRequest buildAlterRoleRequest(GrantRsPrincipalToUgPrincipalContext ctx) {
        RoleInput roleInput = buildNewRoleInput(ctx.rsPrincipalInfo().identifier(), ctx.ugPrincipalInfo());
        AlterRoleRequest request = new AlterRoleRequest();
        request.setInput(roleInput);
        return request;
    }

    public static AlterShareRequest buildAlterShareRequest(GrantRsPrincipalToUgPrincipalContext ctx) {
        ShareInput shareInput = buildGlobalShareInput(ctx.rsPrincipalInfo().shareFullName(),
            ctx.ugPrincipalInfo());
        AlterShareRequest request = new AlterShareRequest();
        request.setInput(shareInput);
        return request;
    }

    public static AlterRoleRequest buildAlterRoleRequest(RevokeRsPrincipalFromUgPrincipalContext ctx) {
        RoleInput roleInput = buildNewRoleInput(ctx.rsPrincipalInfo().identifier(), ctx.ugPrincipalInfo());
        AlterRoleRequest request = new AlterRoleRequest();
        request.setInput(roleInput);
        return request;
    }

    public static AlterShareRequest buildAlterShareRequest(RevokeRsPrincipalFromUgPrincipalContext ctx) {
        ShareInput shareInput = buildGlobalShareInput(ctx.rsPrincipalInfo().shareFullName(),
            ctx.ugPrincipalInfo());
        AlterShareRequest request = new AlterShareRequest();
        request.setInput(shareInput);
        return request;
    }


    public static ShowPoliciesOfPrincipalRequest buildShowPoliciesOfPrincipalRequest(ShowPoliciesOfPrincipalContext ctx) {
        ShowPoliciesOfPrincipalRequest request = new ShowPoliciesOfPrincipalRequest();
        String principalType;
        String principalSource;
        String principalName;
        if (ctx.principalInfo().ugPrincipalInfo() != null) {
            principalType = parseUgPrincipalType(ctx.principalInfo().ugPrincipalInfo());
            principalSource = parsePrincipalSource(ctx.principalInfo().ugPrincipalInfo().principalSource());
            principalName = ParserUtil.parseIdentifier(ctx.principalInfo().ugPrincipalInfo().identifier());
        } else {
            principalType = parseRsPrincipalType(ctx.principalInfo().rsPrincipalInfo());
            principalSource = "IAM";
            principalName = ParserUtil.parseIdentifier(ctx.principalInfo().rsPrincipalInfo().identifier());
        }
        request.setPrincipalType(principalType);
        request.setPrincipalSource(principalSource);
        request.setPrincipalName(principalName);
        return request;
    }

    public static CreateShareRequest buildCreateShareRequest(CreateShareOnCatalogContext ctx) {
        ShareInput shareInput = new ShareInput();
        shareInput.setShareName(ParserUtil.parseIdentifier(ctx.identifier()));
        shareInput.setObjectName(ParserUtil.parseIdentifier(ctx.catalogName().identifier()));
        CreateShareRequest request = new CreateShareRequest();
        request.setInput(shareInput);
        return request;
    }

}
