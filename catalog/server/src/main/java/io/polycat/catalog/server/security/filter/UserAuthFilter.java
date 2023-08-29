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
package io.polycat.catalog.server.security.filter;

import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.model.CatalogResponse;
import io.polycat.catalog.common.utils.GsonUtil;
import io.polycat.catalog.server.util.ResponseUtil;

import org.springframework.http.HttpStatus;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

public class UserAuthFilter extends UsernamePasswordAuthenticationFilter {

    private boolean validateToken(String token) {
        return true;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse =  (HttpServletResponse) response;

        // ���Ի�ȡ����ͷ�� token
        String token = httpRequest.getHeader("Authorization");
        System.out.println("token " + token);

        if (SecurityContextHolder.getContext().getAuthentication() == null) {

            Boolean isValidate = validateToken(token);

            if (!isValidate) {
                httpResponse.setCharacterEncoding("UTF-8");
                httpResponse.setContentType("application/json");

                CatalogResponse responseEntity = ResponseUtil.responseFromErrorCode(ErrorCode.AUTHORIZATION_TOKEN_ERROR);
                httpResponse.getWriter().println(GsonUtil.toJson(responseEntity.getData()));
                httpResponse.setStatus(HttpStatus.UNAUTHORIZED.value());
                return;
            }
            chain.doFilter(request, response);
        }
    }
}
