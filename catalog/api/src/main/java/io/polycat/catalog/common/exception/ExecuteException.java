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
package io.polycat.catalog.common.exception;

/**
 * 执行作业过程中发生的异常
 * @singe 2021/1/20
 */
public class ExecuteException extends CarbonSqlException {
    private String executeProcessorId;

    private long executeWorkerId;

    // 给作业发起方看的异常，传递到下游时，这个值会设置在父类的ErrorItem里，然后清空。
    private ErrorItem toJobCreatorError;

    private Object[] toJobCreatorArgs;
    
    // 也可以用默认的ErrorItem构造，使用默认的EXECUTE_GENERAL_ERROR
    public ExecuteException(String message) {
        super(ErrorItem.EXECUTE_GENERAL_ERROR, message);
        super.setArgs(new Object[]{message});
    }
    
    public ExecuteException(Throwable t) {
        super(t.getMessage(), t);
        super.setErrorItem(ErrorItem.EXECUTE_GENERAL_ERROR);
        super.setArgs(new Object[]{t.getMessage()});
    }
    
    public ExecuteException(ErrorItem errorItem, Object... args) {
        super(errorItem, args);
    }
    
    // 创建执行异常时，给作业发起发的错误是不同的,用静态方法是为了告知外部调用者这个方法是特殊情况下才使用的。
    public static ExecuteException createExecuteExceptionWithToJobCreaterDiff(ErrorItem exeucteNodeError,
            Object[] exeucteNodeArgs, ErrorItem toJobCreatorError, Object[] toJobCreaterArgs) {
        ExecuteException executeException = new ExecuteException(exeucteNodeError, exeucteNodeArgs);
        
        // 暂存toJobCreater异常，用于后面传递到下游时做异常变换
        executeException.toJobCreatorError = toJobCreatorError;
        executeException.toJobCreatorArgs = toJobCreaterArgs;
        return executeException;
    }
    
    // 如有需求，设置processorId和节点id给外部调用
    public ExecuteException setExecuteProcessorId(String executeProcessorId) {
        this.executeProcessorId = executeProcessorId;
        return this;
    }

    public ExecuteException setExecuteWorkerId(long workerId) {
        this.executeWorkerId = workerId;
        return this;
    }

    public String getExecuteProcessorId() {
        return executeProcessorId;
    }

    public long getExecuteWorkerId() {
        return executeWorkerId;
    }

    /**
     * 是否需要做异常转换，转成会返回给作业发起者的异常
     * @return
     */
    public boolean isNeedConvertToJobCreatorException() {
        return toJobCreatorError != null;
    }

    /**
     * 转成给作业发起发看的异常
     * @return
     */
    public ExecuteException toJobCreatorException() {
        if (!isNeedConvertToJobCreatorException()) {
            return this;
        }
        
        return new ExecuteException(toJobCreatorError, toJobCreatorArgs);
    }

    @Override
    public String getMessage() {
        String message = super.getMessage();
        if (getErrorItem() == ErrorItem.EXECUTE_GENERAL_ERROR && executeProcessorId != null) {
            return String.format("[processorId=%s, workerId=%d] %s", executeProcessorId, executeWorkerId, message);
        } else {
            return message;
        }
    }
}
