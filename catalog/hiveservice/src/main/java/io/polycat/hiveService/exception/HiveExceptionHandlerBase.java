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
package io.polycat.hiveService.exception;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.polycat.catalog.common.ErrorCode;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;

public interface HiveExceptionHandlerBase {
    Map<Class<? extends Throwable>, ErrorCode> hiveExceptionMap = new HashMap<Class<? extends Throwable>, ErrorCode>(){{
        put(AlreadyExistsException.class, ErrorCode.TABLE_ALREADY_EXIST);
        put(ConfigValSecurityException.class, ErrorCode.CONFIG_SECURITY_INVALID);
        put(InvalidInputException.class, ErrorCode.ARGUMENT_ILLEGAL);
        put(InvalidObjectException.class, ErrorCode.INVALID_OBJECT);
        put(InvalidOperationException.class, ErrorCode.INVALID_OPERATION);
        put(InvalidPartitionException.class, ErrorCode.INVALID_PARTITION);
        put(MetaException.class, ErrorCode.META_OPERATE_ERROR);
        put(NoSuchLockException.class, ErrorCode.NO_SUCH_LOCK);
        put(NoSuchObjectException.class, ErrorCode.OBJECT_ID_NOT_FOUND);
        put(NoSuchTxnException.class, ErrorCode.NO_SUCH_TXN);
        put(TxnAbortedException.class, ErrorCode.TXN_ABORTED);
        put(TxnOpenException.class, ErrorCode.TXN_OPEN_FAILED);
        put(UnknownDBException.class, ErrorCode.DATABASE_NOT_FOUND);
        put(UnknownTableException.class, ErrorCode.TABLE_NOT_FOUND);
        put(UnknownPartitionException.class, ErrorCode.PARTITION_NAME_NOT_FOUND);
        put(TException.class, ErrorCode.TRANSLATE_ERROR);
        put(TApplicationException.class, ErrorCode.THRIFT_APPLICATION_ERROR);
        put(IOException.class, ErrorCode.INVALID_OPERATION);
    }};
}
