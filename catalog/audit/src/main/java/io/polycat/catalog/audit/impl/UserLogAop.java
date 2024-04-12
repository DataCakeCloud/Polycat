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
package io.polycat.catalog.audit.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import io.polycat.catalog.audit.api.AuditLogService;
import io.polycat.catalog.audit.api.UserLog;
import io.polycat.catalog.common.model.AuditLog;

import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.validation.BindingResult;
import org.springframework.web.servlet.HandlerMapping;


/**
 * Audit the logging facet, Add this comment(@UserLog) to the controller method, and logs are automatically logged in
 * aop based on the configured parameters.
 */
@Aspect
@Order(0)
@Component
public class UserLogAop {

    private static final ThreadLocal<LogDetail> OPT_DETAIL = new ThreadLocal<>();

    @Resource
    protected HttpServletRequest request;
    @Autowired
    private AuditLogService auditLogService;
    /**
     * addAuditLog
     *
     * @param point point
     * @param log   log
     */
    @Around("execution(* io.polycat.catalog.server.controller.*Controller.*(..)) && @annotation(log)")
    public Object writeUserAuditLog(ProceedingJoinPoint point, UserLog log) throws Throwable {
        //Construct an audit log
        AuditLog builder = AuditLogHelper.getAuditLog(request);
        builder.setOperation(log.operation());
        builder.setObjectType(log.objectType());

        LogDetail logDetail = new LogDetail();
        OPT_DETAIL.set(logDetail);

        try {
            Object obj = point.proceed();
            if (obj instanceof ResponseEntity) {
                ResponseEntity result = (ResponseEntity) obj;
                builder.setState(result.getStatusCode().series().toString());
            } else {
                builder.setState(HttpStatus.OK.series().toString());
            }
            return obj;
        } catch (Throwable t) {
            builder.setState(HttpStatus.INTERNAL_SERVER_ERROR.toString());
            throw t;
        } finally {
            builder.setObjectId(OPT_DETAIL.get().objectId);
            builder.setObjectName(OPT_DETAIL.get().objectName);
            builder.setDetail(logDetail.detail == null ? getRequestParam(request) : logDetail.detail);
            OPT_DETAIL.remove();
            auditLogService.addAuditLog(builder);
        }
    }

    private String args2str(Object[] args) {
        if (args == null) {
            return StringUtils.EMPTY;
        }
        args = Stream.of(args).filter(arg -> !(arg instanceof BindingResult)).toArray();
        if (args.length == 0) {
            return StringUtils.EMPTY;
        }
        return Arrays.toString(args);
    }

    private String getRequestParam(HttpServletRequest request) {
        Map<String, String[]> paramVariables = (Map) request.getParameterMap();
        String requestParams = "";
        for (String key : paramVariables.keySet()) {
            List<String> values = Arrays.asList(paramVariables.get(key));
            requestParams = requestParams + key + ":" + values + ";";
        }
        return requestParams;
    }

    /**
     * getRequestUserId
     *
     * @param request request
     */
    private String getRequestUserId(HttpServletRequest request) throws IOException, ClassNotFoundException {
        String userId = request.getHeader("userId");
        if (StringUtils.isNotEmpty(userId)) {
            return userId;
        }
        return "default";
    }

    /**
     * getRequestProjectId
     *
     * @param request request
     */
    private String getRequestProjectId(HttpServletRequest request) {
        Map pathVariables = (Map) request.getAttribute(HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE);
        String projectId = (String) pathVariables.get("projectId");
        return projectId;
    }

    /**
     * setAuditLogDetail
     *
     * @param detail detail
     */
    public static void setAuditLogDetail(String detail) {
        OPT_DETAIL.get().detail = detail;
    }

    /**
     * setAuditLogObjectId
     *
     * @param objectId
     */
    public static void setAuditLogObjectId(String objectId) {
        OPT_DETAIL.get().objectId = objectId;
    }

    /**
     * setAuditLogObjectName
     *
     * @param objectName
     */
    public static void setAuditLogObjectName(String objectName) {
        OPT_DETAIL.get().objectName = objectName;
    }

    /**
     * AuditLog detail
     */
    private static class LogDetail {

        //auditlog detail
        private String detail;
        //audilog objectId
        private String objectId;
        //auditlog objectName
        private String objectName;
    }
}
