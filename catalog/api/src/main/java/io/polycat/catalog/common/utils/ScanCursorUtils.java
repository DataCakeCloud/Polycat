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
package io.polycat.catalog.common.utils;

import io.polycat.catalog.common.model.ScanRecordCursorResult;
import io.polycat.catalog.common.model.TraverseCursorResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ScanCursorUtils {

    /**
     * Token key for default strategy
     */
    private static final String TOK_DEFAULT_STRATEGY = "defaultProcessorStrategy";
    public interface CursorProcessor<T> {
        List<T> processor(int batchNum, long batchOffset);
    }

    private static <T> ScanRecordCursorResult<List<T>> getScanCursorResult(List<T> result, long batchOffset, int batchNum) {
        if (result.size() < batchNum) {
            return new ScanRecordCursorResult<List<T>>(result, 0);
        } else {
            return new ScanRecordCursorResult<List<T>>(result, batchOffset + batchNum);
        }
    }

    public static <T> TraverseCursorResult<List<T>> scan(String readVersion, String tokenKey, String pageToken,
        Integer limit, Integer maxBatchRowNum, CursorProcessor<T> cursorProcessor) {
        return scan(readVersion, tokenKey, pageToken, getCallMethodName(), limit, maxBatchRowNum, cursorProcessor);
    }

    public static <T> TraverseCursorResult<List<T>> scan(String readVersion, String tokenKey, String pageToken, String method,
            Integer limit, Integer maxBatchRowNum, CursorProcessor<T> cursorProcessor) {
        int remainNum = limit;
        CatalogToken catalogToken = CatalogToken.parseTokenOrCreate(pageToken, tokenKey,
                method, readVersion, "0");
        long tokenOffset = getTokenOffset(catalogToken, method);
        CatalogToken previousToken = speculatePreviousToken(tokenKey, method, tokenOffset, limit, catalogToken.getReadVersion());
        List<T> result = new ArrayList<>();
        ScanRecordCursorResult<List<T>> scanRecordCursorResult;
        while (true) {
            int batchNum = Math.min(remainNum, maxBatchRowNum);
            long batchOffset = tokenOffset;
            scanRecordCursorResult = getScanCursorResult(cursorProcessor.processor(batchNum, batchOffset), batchOffset, batchNum);
            result.addAll(scanRecordCursorResult.getResult());
            remainNum = remainNum - scanRecordCursorResult.getResult().size();
            tokenOffset = scanRecordCursorResult.getNextOffset();
            if ((tokenOffset == 0) || (remainNum == 0)) {
                break;
            }
        }

        if (tokenOffset == 0) {
            return new TraverseCursorResult<>(result, null, previousToken);
        }

        CatalogToken catalogTokenNew = CatalogToken.buildCatalogToken(tokenKey, method,
                String.valueOf(tokenOffset), catalogToken.getReadVersion());
        return new TraverseCursorResult<>(result, catalogTokenNew, previousToken);
    }

    private static String getCallMethodName() {
        return Thread.currentThread().getStackTrace()[3].getMethodName();
    }

    public static <T> TraverseCursorResult<List<T>> scanMultiStrategy(String readVersion, String tokenKey, String pageToken,
        Integer limit, Integer maxBatchRowNum, CursorProcessor<T> cursorProcessor, CursorProcessor<T> strategyCursorProcessor2) {
        return scanMultiStrategy(readVersion, tokenKey, pageToken, getCallMethodName(), limit, maxBatchRowNum, cursorProcessor, strategyCursorProcessor2);
    }

        public static <T> TraverseCursorResult<List<T>> scanMultiStrategy(String readVersion, String tokenKey, String pageToken, String method,
            Integer limit, Integer maxBatchRowNum, CursorProcessor<T> cursorProcessor, CursorProcessor<T> strategyCursorProcessor2) {
        int remainNum = limit;
        CatalogToken catalogToken = CatalogToken.parseTokenOrCreate(pageToken, tokenKey,
                method, readVersion, "0");
        boolean defaultStrategy = getTokenDefaultProcessorStrategy(catalogToken, TOK_DEFAULT_STRATEGY);
        long tokenOffset = getTokenOffset(catalogToken, method);
        // Speculate the token of the previous page.
        CatalogToken previousToken = speculatePreviousToken(tokenKey, method, tokenOffset, limit, catalogToken.getReadVersion());
        List<T> result = new ArrayList<>();
        ScanRecordCursorResult<List<T>> scanRecordCursorResult = new ScanRecordCursorResult<>(Collections.emptyList(), 0);
        while (true) {
            int batchNum = Math.min(remainNum, maxBatchRowNum);
            long batchOffset = tokenOffset;
            if (defaultStrategy) {
                scanRecordCursorResult = getScanCursorResult(cursorProcessor.processor(batchNum, batchOffset), batchOffset, batchNum);
            }
            if (batchOffset == 0 && scanRecordCursorResult.getResult().size() == 0) {
                defaultStrategy = false;
                catalogToken.putContextMap(TOK_DEFAULT_STRATEGY, "false");
            }
            if(!defaultStrategy) {
                scanRecordCursorResult = getScanCursorResult(strategyCursorProcessor2.processor(batchNum, batchOffset), batchOffset, batchNum);
            }
            result.addAll(scanRecordCursorResult.getResult());
            remainNum = remainNum - scanRecordCursorResult.getResult().size();
            tokenOffset = scanRecordCursorResult.getNextOffset();
            if ((tokenOffset == 0) || (remainNum == 0)) {
                break;
            }
        }

        if (tokenOffset == 0) {
            return new TraverseCursorResult<>(result, null, previousToken);
        }

        CatalogToken catalogTokenNew = CatalogToken.buildCatalogToken(tokenKey, method,
                String.valueOf(tokenOffset), catalogToken.getReadVersion());
        if (!defaultStrategy) {
            catalogTokenNew.putContextMap(TOK_DEFAULT_STRATEGY, "false");
        }
        return new TraverseCursorResult<>(result, catalogTokenNew, previousToken);
    }

    /**
     * Speculate the token of the previous page.
     *
     * @param tokenKey
     * @param method
     * @param tokenOffset
     * @param limit
     * @param readVersion
     * @return
     */
    private static CatalogToken speculatePreviousToken(String tokenKey, String method, long tokenOffset, Integer limit, String readVersion) {
        CatalogToken previousToken = null;
        if (tokenOffset >= limit) {
            previousToken = CatalogToken.buildCatalogToken(tokenKey, method,
                    String.valueOf(tokenOffset - limit), readVersion);
        }
        return previousToken;
    }

    private static long getTokenOffset(CatalogToken catalogToken, String method) {
        if (catalogToken.getContextMapValue(method) != null) {
            return Long.parseLong(catalogToken.getContextMapValue(method));
        }
        return 0L;
    }

    private static boolean getTokenDefaultProcessorStrategy(CatalogToken catalogToken, String key) {
        if (catalogToken.getContextMapValue(key) != null) {
            return Boolean.parseBoolean(catalogToken.getContextMapValue(key));
        }
        return true;
    }
}
