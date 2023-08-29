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
package io.polycat.hivesdk.tools;

import io.polycat.catalog.common.ErrorCode;
import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.http.HttpResponseException;
import io.polycat.catalog.common.plugin.CatalogContext;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.springframework.http.HttpStatus;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class HiveClientHelper {

    @FunctionalInterface
    public interface NoReturnSupplier {
        void get();
    }

    private static final Map<String, Class<? extends TException>> exceptionMap = new HashMap<String, Class<? extends TException>>(){{
        put(ErrorCode.TABLE_ALREADY_EXIST.getErrorCode(), AlreadyExistsException.class);
        put(ErrorCode.DATABASE_ALREADY_EXIST.getErrorCode(), AlreadyExistsException.class);
        put(ErrorCode.CONFIG_SECURITY_INVALID.getErrorCode(), ConfigValSecurityException.class);
        put(ErrorCode.ARGUMENT_ILLEGAL.getErrorCode(), InvalidInputException.class);
        put(ErrorCode.INVALID_OBJECT.getErrorCode(), InvalidObjectException.class);
        put(ErrorCode.INVALID_OPERATION.getErrorCode(), InvalidOperationException.class);
        put(ErrorCode.INVALID_PARTITION.getErrorCode(), InvalidPartitionException.class);
        put(ErrorCode.META_OPERATE_ERROR.getErrorCode(), MetaException.class);
        put(ErrorCode.NO_SUCH_LOCK.getErrorCode(), NoSuchObjectException.class);
        put(ErrorCode.OBJECT_ID_NOT_FOUND.getErrorCode(), NoSuchObjectException.class);
        put(ErrorCode.NO_SUCH_TXN.getErrorCode(), NoSuchTxnException.class);
        put(ErrorCode.TXN_ABORTED.getErrorCode(), TxnAbortedException.class);
        put(ErrorCode.TXN_OPEN_FAILED.getErrorCode(), TxnOpenException.class);
        put(ErrorCode.DATABASE_NOT_FOUND.getErrorCode(), NoSuchObjectException.class);
        put(ErrorCode.PARTITION_NAME_NOT_FOUND.getErrorCode(), NoSuchObjectException.class);
        put(ErrorCode.TABLE_NOT_FOUND.getErrorCode(), NoSuchObjectException.class);
        put(ErrorCode.TRANSLATE_ERROR.getErrorCode(), TException.class);
        put(ErrorCode.THRIFT_APPLICATION_ERROR.getErrorCode(), TApplicationException.class);
    }};

    public static <R> R FuncExceptionHandler(Supplier<R> supplier)
        throws NoSuchObjectException, TException {
        try {
            return supplier.get();
        } catch (CatalogException e) {
            HttpResponseException cause = getExceptionCause(e);
            if (cause != null) {
                try {
                    Constructor<? extends TException> execConstructor = exceptionMap.getOrDefault(cause.getErrorCode(),
                        TException.class).getConstructor(String.class);
                    throw execConstructor.newInstance(e.getMessage());
                } catch (ReflectiveOperationException internalException) {
                    throw new TException("HiveClient: Exception is illegal");
                }
            }

            throw new TException(e.getMessage());
        }
    }

    public static void FuncExceptionHandler(NoReturnSupplier supplier)
        throws TException {
        try {
            supplier.get();
        } catch (CatalogException e) {
            HttpResponseException cause = getExceptionCause(e);
            if (cause != null) {
                try {
                    Constructor<? extends TException> execConstructor = exceptionMap.getOrDefault(cause.getErrorCode(),
                        TException.class).getConstructor(String.class);
                    throw execConstructor.newInstance(e.getMessage());
                } catch (ReflectiveOperationException internalException) {
                    throw new TException("HiveClient: Exception is illegal");
                }
            }

            throw new TException(e.getMessage());
        }
    }

    private static HttpResponseException getExceptionCause(CatalogException e) {
        Throwable cause = e.getCause();
        if (cause instanceof HttpResponseException) {
            return (HttpResponseException) cause;
        }
        return null;
    }


    public static void checkInputIsNoNull(Object... params) throws MetaException {
        if (Arrays.stream(params).anyMatch(Objects::isNull)) {
            throw new MetaException("params illegal");
        }
    }


    public static void noNullObject(Object object) {
        if (Objects.isNull(object)) {
            throw new IllegalArgumentException("param cant be null");
        }
    }

}
