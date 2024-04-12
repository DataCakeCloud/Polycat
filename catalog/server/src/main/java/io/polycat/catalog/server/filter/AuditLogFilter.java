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
package io.polycat.catalog.server.filter;

import io.polycat.catalog.audit.api.AuditLogService;
import io.polycat.catalog.audit.impl.AuditLogHelper;
import io.polycat.catalog.server.util.SpringContextUtils;
import io.polycat.catalog.common.CatalogConstants;
import io.polycat.catalog.common.ObjectType;
import io.polycat.catalog.common.model.AuditLog;
import io.polycat.catalog.common.plugin.request.input.DatabaseInput;
import io.polycat.catalog.common.plugin.request.input.TableInput;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;

import javax.servlet.Filter;
import javax.servlet.FilterConfig;
import javax.servlet.FilterChain;
import javax.servlet.ReadListener;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletInputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Configuration
public class AuditLogFilter implements Filter {
    private static final String COMMON_CHAR_PREFIX = "\"";
    private static final String COMMON_CHAR_SUFIX = "\":\"****\"";
    private static final int MAX_QUOTES_NUM = 4;
    private static Set<String> notLogUrlSet = new HashSet<>();

    private String[] sensitiveWords;

    private static Set<ObjectType> auditLogRouteKey;
    private static Boolean auditEnable = true;

    @Value("${audit.log.routing.keys}")
    public void setAuditLogRoutingKeys(String keys) {
        if (StringUtils.isNotEmpty(keys)) {
            auditLogRouteKey = Arrays.stream(keys.split(",")).map(String::trim).map(String::toUpperCase).map(ObjectType::valueOf).collect(Collectors.toSet());
        }
    }

    @Value("${audit.enable:true}")
    public void setAuditLogRoutingKeys(Boolean enable) {
        auditEnable = enable;
    }

    /*
     * @param filterConfig
     * @throws ServletException
     */
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        if (notLogUrlSet.isEmpty()) {
            final String notLogUrls = filterConfig.getInitParameter("notLogUrls");
            //final String includeRegex = filterConfig.getInitParameter("includeRegex");
            if (StringUtils.isNotBlank(notLogUrls)) {
                String[] paths = notLogUrls.replace(" ", "").split(",");
                notLogUrlSet.addAll(Arrays.asList(paths));
            }
        }

        if (sensitiveWords == null) {
            String words = filterConfig.getInitParameter("sensitiveWords");
            if (StringUtils.isNotBlank(words)) {
                words = words.replaceAll(" ", "");
                sensitiveWords = words.split(",");
            }
        }
        AuditLogHelper.initUriPath(FilterConfigurations.getBaseVersion());
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
        if (!(request instanceof HttpServletRequest) || !(response instanceof HttpServletResponse)) {
            return;
        }

        final HttpServletRequest httpRequest = (HttpServletRequest) request;
        final HttpServletResponse httpResponse = (HttpServletResponse) response;
        if (!auditEnable) {
            filterChain.doFilter(httpRequest, httpResponse);
            return;
        }
        long requestTime = System.currentTimeMillis();
        if (AuditLogHelper.skipRegexUriPath(httpRequest.getRequestURI())
                || AuditLogHelper.matchIgnoreMethod(CatalogConstants.AUDIT_IGNORE_METHOD, httpRequest)) {
            filterChain.doFilter(httpRequest, httpResponse);
            return;
        }

        BufferedRequestWrapper bufferedRequest = new BufferedRequestWrapper(httpRequest);
        //writeLogBeforeRequest(httpRequest, bufferedRequest);

        BufferedResponseWrapper wrappedResponse = new BufferedResponseWrapper(httpResponse);
        filterChain.doFilter(bufferedRequest, wrappedResponse);
        byte[] bytes = wrappedResponse.getBytes();
        String respMsg = AuditLogHelper.getString(bytes);
        writeMsgBackToResponse(httpResponse, respMsg);
        try {
            AuditLog auditLog = getAuditLog(bufferedRequest, httpResponse, requestTime, respMsg);
            addAuditLog(auditLog);
        } catch (Exception e) {
            log.warn("Audit log add error: {}", e.getMessage());
        }
    }

    private void addAuditLog(AuditLog auditLog) {
        ObjectType objectType = auditLog.getObjectType();
        if (objectType != null) {
            if (!CollectionUtils.isEmpty(auditLogRouteKey) && auditLogRouteKey.contains(objectType)) {
                getAuditLogService().addAuditLogRoute(auditLog, objectType.toString());
                return;
            }
        }
        getAuditLogService().addAuditLog(auditLog);
    }

    private void writeMsgBackToResponse(HttpServletResponse httpResponse, String respMsg) {
        PrintWriter writer = null;
        try {
            httpResponse.addHeader(CatalogConstants.HTTP_HEADER_CONTENT_TYPE_KEY, CatalogConstants.HTTP_HEADER_CONTENT_TYPE_VALUE);
            writer = httpResponse.getWriter();
            writer.print(respMsg);
        } catch (IOException e) {
            log.debug("io exception" + e);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    private boolean isNeedToLog(HttpServletRequest request) {
        if (null == notLogUrlSet || notLogUrlSet.isEmpty()) {
            return true;
        }
        if (notLogUrlSet.contains(request.getPathInfo())) {
            return false;
        }
        return true;
    }

    private void writeLogBeforeRequest(HttpServletRequest request, BufferedRequestWrapper bufferedRequest) {
        String ip = StringUtils.isBlank(request.getRemoteAddr()) ? "Unknown" : request.getRemoteAddr();
        String user = "unknown";
        user = AuditLogHelper.getUser(request);
        String uri = request.getPathInfo();
        String method = request.getMethod();
        String contentType = request.getHeader("Content-Type");
        String msg = "";
        if (null == contentType) {
            msg = "****";
        } else {
            msg = replacePwdAndUserInfo(bufferedRequest.getBuffer());
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[Request] ");
        sb.append("[user=").append(user).append("] ");
        sb.append("[ip=").append(ip).append("] ");
        sb.append("[url=").append(uri).append("; ");
        sb.append("method=").append(method).append("; ");
        sb.append("msg=").append(msg).append("] ");
        log.info(sb.toString());
        System.out.println(sb.toString());
        return;
    }

    public static String getTableLocationFromContent(HttpServletRequest request) throws IOException {

        String location = "Unkown";
        BufferedRequestWrapper bufferedRequest = new BufferedRequestWrapper(request);

        byte[]  bytes = bufferedRequest.getBuffer();
        String reqBody;
        try {
            reqBody = new String(bytes,"utf-8");
        } catch (UnsupportedEncodingException e) {
            log.warn("failed to trans to string:" + e.getMessage());
            return location;
        }
        if (StringUtils.isBlank(reqBody)) {
            return location;
        }

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            TableInput tableInput = objectMapper.readValue(reqBody,TableInput.class);
            if (null != tableInput && tableInput.getStorageDescriptor() != null
                    && tableInput.getStorageDescriptor().getLocation() != null
                    && !tableInput.getStorageDescriptor().getLocation().isEmpty()) {
                return tableInput.getStorageDescriptor().getLocation();
            }
        } catch (JsonParseException e) {
            log.warn(e.getMessage());
        } catch (IOException e) {
            log.warn(e.getMessage());
        }
        return location;
    }

    public static String getDatabaseLocationFromContent(HttpServletRequest request) throws IOException {

        String location = "Unkown";
        BufferedRequestWrapper bufferedRequest = new BufferedRequestWrapper(request);

        byte[]  bytes = bufferedRequest.getBuffer();
        String reqBody;
        try {
            reqBody = new String(bytes,"utf-8");
        } catch (UnsupportedEncodingException e) {
            log.warn("failed to trans to string:" + e.getMessage());
            return location;
        }
        if (StringUtils.isBlank(reqBody)) {
            return location;
        }

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            DatabaseInput databaseInput = objectMapper.readValue(reqBody, DatabaseInput.class);
            if (null != databaseInput
                    && null != databaseInput.getLocationUri()
                    && !databaseInput.getLocationUri().isEmpty()) {
                return databaseInput.getLocationUri();
            }
        } catch (JsonParseException e) {
            log.warn(e.getMessage());
        } catch (IOException e) {
            log.warn(e.getMessage());
        }
        return location;
    }

    private AuditLog getAuditLog(BufferedRequestWrapper request, HttpServletResponse response, long requestTime, String data) {
        AuditLog auditLog = AuditLogHelper.getAuditLog(request, request.getBuffer());
        AuditLogHelper.setAuditLogResponseAttr(response, auditLog, data);
        auditLog.setTimeConsuming(System.currentTimeMillis() - requestTime);
        //String msg = msgEncode(request, bufferedRequest);
        return auditLog;
    }

    private String msgEncode(HttpServletRequest request, BufferedRequestWrapper bufferedRequest) {
        String contentType = request.getHeader(CatalogConstants.HTTP_HEADER_CONTENT_TYPE_KEY);
        String msg = "";
        if (null == contentType) {
            msg = "****";
        } else {
            msg = replacePwdAndUserInfo(bufferedRequest.getBuffer());
        }
        return msg;
    }

    private AuditLogService getAuditLogService() {
        return SpringContextUtils.getBean("auditLogService", AuditLogService.class);
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

    public static byte[] getBufferedBytes(HttpServletRequest request) throws IOException {
        return new BufferedRequestWrapper(request).getBuffer();
    }

    private String replacePwdAndUserInfo(byte[] bytes) {
        String msg;
        msg = new String(bytes, StandardCharsets.UTF_8);
        if (StringUtils.isBlank(msg) || sensitiveWords == null || sensitiveWords.length == 0) {
            return "";
        }

        for (String word : sensitiveWords) {
            int startIndex = msg.indexOf(COMMON_CHAR_PREFIX + word);
            if (startIndex < 0) {
                continue;
            }

            int num = 0;
            int endIndex = 0;
            for (int i = startIndex; i < msg.length(); i++) {
                char iChar = msg.charAt(i);
                if (iChar == '\"') {
                    if (++num >= MAX_QUOTES_NUM && msg.charAt(i - 1) != '\\') {
                        endIndex = i;
                        break;
                    }
                }
            }

            String tempRegex = COMMON_CHAR_PREFIX + word + COMMON_CHAR_SUFIX;
            String finalMsg = msg.substring(0, startIndex);
            finalMsg += tempRegex;
            finalMsg += msg.substring(endIndex + 1 > msg.length() ? msg.length() : (endIndex + 1), msg.length());
            msg = finalMsg;
        }

        return msg;
    }

    @Override
    public void destroy() {

    }

    /* jie shuzu shuchu liu  */
    private static class ByteArrayServletStream extends ServletOutputStream {
        private ByteArrayOutputStream baos;

        ByteArrayServletStream(ByteArrayOutputStream baos) {this.baos = baos;}

        @Override
        public void write(int param) throws IOException {
            baos.write(param);
        }

        @Override
        public boolean isReady() {
            return false;
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {

        }
    }

    private  static  class ByteArrayPrintWriter {
        private ByteArrayOutputStream baos = new ByteArrayOutputStream();
        private ServletOutputStream sos = new ByteArrayServletStream(baos);
        private OutputStreamWriter osw;
        private PrintWriter pw;

        public ByteArrayPrintWriter() {
            osw = new OutputStreamWriter(baos, StandardCharsets.UTF_8);
            pw = new PrintWriter(osw);
        }

        public PrintWriter getWriter() {return pw;}

        public ServletOutputStream getStream() {return sos;}

        public void close() {
            if (osw != null) {
                try {
                    osw.close();
                } catch (IOException e) {
                    log.warn("failed to close osw" + e);
                }
            }
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException e) {
                    log.warn("failed to close baos" + e);
                }
            }
        }

        byte[] toByteArray() {return baos.toByteArray();}
    }


    private static class BufferedServletInputStream extends ServletInputStream {
        private ByteArrayInputStream bais;

        public BufferedServletInputStream(ByteArrayInputStream bais) {this.bais = bais;}

        @Override
        public int available() { return bais.available(); }

        @Override
        public int read() { return bais.read(); }

        @Override
        public int read(byte[] buf, int off, int len) { return bais.read(buf,off,len); }

        @Override
        public boolean isFinished() {
            return false;
        }

        @Override
        public boolean isReady() {
            return false;
        }

        @Override
        public void setReadListener(ReadListener readListener) {

        }
    }

    private static class BufferedResponseWrapper extends HttpServletResponseWrapper implements Closeable {
        private ByteArrayPrintWriter pw;

        public BufferedResponseWrapper(HttpServletResponse httpResponse) {
            super(httpResponse);
            this.pw = new ByteArrayPrintWriter();
        }

        public byte[] getBytes() {
            return pw.toByteArray();
        }

        @Override
        public PrintWriter getWriter() {
            return pw.getWriter();
        }

        @Override
        public ServletOutputStream getOutputStream() {
            return pw.getStream();
        }

        @Override
        public void close() throws IOException {
            pw.close();
        }
    }

    private static  class BufferedRequestWrapper extends HttpServletRequestWrapper {
        private ByteArrayInputStream bais;
        private ByteArrayOutputStream baos;
        private BufferedServletInputStream bsis;

        private byte[] buffer;
        private static final int BUFFER_LENGTH = 1024;

        public BufferedRequestWrapper(HttpServletRequest req) throws IOException {
            super(req);
            init(req);
        }

        private void init(HttpServletRequest req) throws IOException {
            InputStream is = req.getInputStream();
            baos = new ByteArrayOutputStream();
            byte buf[] = new byte[BUFFER_LENGTH];
            int length;
            while ( (length = is.read(buf)) > 0) {
                baos.write(buf,0,length);
            }
            buffer = baos.toByteArray();
        }

        @Override
        public ServletInputStream getInputStream() {
            try {
                bais = new ByteArrayInputStream(buffer);
                bsis = new BufferedServletInputStream(bais);
            } catch (Exception e) {
                log.debug("init error", e);
            }
            return bsis;
        }

        public byte[] getBuffer() { return buffer; }
    }
}
