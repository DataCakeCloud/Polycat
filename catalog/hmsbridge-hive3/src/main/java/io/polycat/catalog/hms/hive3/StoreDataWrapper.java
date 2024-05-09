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
package io.polycat.catalog.hms.hive3;

import io.polycat.catalog.common.ErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.api.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

@Slf4j
public class StoreDataWrapper<T>  {

    private T data;
    private T defaultValue;
    private Exception e;
    private Map<ErrorCode, Class<? extends Exception>> codeExceptionMap;

    public static <T> StoreDataWrapper<T> getInstance(T defaultValue) {
        StoreDataWrapper instance = new StoreDataWrapper();
        instance.setDefaultValue(defaultValue);
        return instance;
    }

    public static <T> StoreDataWrapper<T> getInstance(T defaultValue, ErrorCode errorCode, Class<? extends Exception> eClass) {
        StoreDataWrapper<T> instance = getInstance(defaultValue);
        instance.putCodeException(errorCode, eClass);
        return instance;
    }

    public void wrapper(Supplier<T> supplier) {
        wrapStoreDataThrow(this, supplier);
    }

    private static <T> void wrapStoreDataThrow(StoreDataWrapper<T> storeDataWrapper, Supplier<T> supplier) {
        try {
            storeDataWrapper.setData(supplier.get());
        } catch (Exception e) {
            log.warn("From catalog store get data error, {}", e.getMessage());
            if (storeDataWrapper.getCodeExceptionMap() != null) {
                Set<Map.Entry<ErrorCode, Class<? extends Exception>>> errorCodeEntries = storeDataWrapper.getCodeExceptionMap().entrySet();
                for (Map.Entry<ErrorCode, Class<? extends Exception>> errorCode : errorCodeEntries) {
                    if (e.getMessage().contains(errorCode.getKey().getErrorCode())) {
                        storeDataWrapper.setE(createException(errorCode.getValue(), e.getMessage()));
                        break;
                    }
                }
                if (!storeDataWrapper.hasException()) {
                    storeDataWrapper.setE(e);
                }
            }
            storeDataWrapper.setData(storeDataWrapper.getDefaultValue());
        }
    }

    public static Exception createException(Class<? extends Exception> exceptionClass, String message) {
        try {
            return exceptionClass.getConstructor(String.class).newInstance(message);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to create exception instance", e);
        }
    }

    public static Exception createException(Class<? extends Exception> exceptionClass, String message, Throwable cause) {
        try {
            return exceptionClass.getConstructor(String.class, Throwable.class).newInstance(message, cause);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to create exception instance", e);
        }
    }

    public Map<ErrorCode, Class<? extends Exception>> getCodeExceptionMap() {
        return codeExceptionMap;
    }

    public void putCodeException(ErrorCode errorCode, Class<? extends Exception> eClass) {
        if (codeExceptionMap == null) {
            codeExceptionMap = new HashMap<>();
        }
        codeExceptionMap.put(errorCode, eClass);
    }

    public T getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(T defaultValue) {
        this.defaultValue = defaultValue;
    }

    public boolean hasException() {
        return this.e != null;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public Exception getE() {
        return e;
    }

    public void setE(Exception e) {
        this.e = e;
    }

    public void throwUnknownDBException() throws UnknownDBException {
        if (this.hasException() && this.getE() instanceof UnknownDBException) {
            throw (UnknownDBException) this.getE();
        }
    }

    public void throwNoSuchObjectException() throws NoSuchObjectException {
        if (this.hasException() && this.getE() instanceof NoSuchObjectException) {
            throw (NoSuchObjectException) this.getE();
        }
    }

    public void throwInvalidInputException() throws InvalidInputException {
        if (this.hasException() && this.getE() instanceof InvalidInputException) {
            throw (InvalidInputException) this.getE();
        }
    }


    public void throwInvalidObjectException() throws InvalidObjectException {
        if (this.hasException() && this.getE() instanceof InvalidObjectException) {
            throw (InvalidObjectException) this.getE();
        }
    }

    public void throwMetaException() throws MetaException {
        if (this.hasException()) {
            final MetaException metaException = new MetaException(this.getE().getMessage());
            metaException.setStackTrace(this.getE().getStackTrace());
            throw metaException;
        }
    }
}
