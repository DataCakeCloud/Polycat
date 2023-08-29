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
package io.polycat.catalog.util;

import io.polycat.catalog.common.Logger;
import io.polycat.catalog.common.MetaStoreException;
import io.polycat.catalog.common.ErrorCode;

import org.apache.commons.lang3.StringUtils;


@SuppressWarnings("unchecked")
public class CheckUtil {

    private static final Logger log = Logger.getLogger(CheckUtil.class);

    private static final String namePattern = "[^0-9][\\w]*";

    /**
     * check params is illegal
     *
     * @param parameters
     */
    public static void checkStringParameter(String... parameters) {
        for (String parameter : parameters) {
            if (StringUtils.isBlank(parameter)) {
                throw new MetaStoreException(ErrorCode.ARGUMENT_ILLEGAL, "object");
            }
        }
    }

    public static void checkStringParameter(String value, String key) {
        if (StringUtils.isBlank(value)) {
            throw new MetaStoreException(ErrorCode.ARGUMENT_ILLEGAL, key);
        }
    }

    public static void assertNotNull(Object obj, ErrorCode errorCode, Object... params) {
        if (obj == null) {
            throw new MetaStoreException(errorCode, params);
        }
    }

    /**
     * check name legality
     * @param key
     * @param name
     */
    public static void checkNameLegality(String key, String name) {
        if (name == null || !name.matches(namePattern)) {
            throw new MetaStoreException(ErrorCode.ARGUMENT_ILLEGAL_2, key, name);
        }
    }

}

