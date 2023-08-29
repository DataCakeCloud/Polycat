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


public class CarbonSqlException extends RuntimeException {
    
    // 异常类型，可以提供给外部服务化做判断
    private ErrorItem errorItem = ErrorItem.GENERAL_ERROR;

    // 异常相关参数，提供给外部服务化做自定义消息适配,例如国际化
    private Object[] args;
    
    // 不带errorItem的构造方法用于适配以前的用法，默认是ErrorItem.GENERAL_ERROR
    public CarbonSqlException(String message) {
        super(message);
        this.args = new Object[]{message};
    }

    public CarbonSqlException(Throwable cause) {
        super(ExceptionUtil.getMessageExcludeException(cause), cause);
        this.args = new Object[]{ExceptionUtil.getMessageExcludeException(cause)};
    }

    public CarbonSqlException(String message, Throwable cause) {
        super(message, cause);
        this.args = new Object[]{message};
    }
    
    public CarbonSqlException(ErrorItem item, Object... args) {
        // 如果使用errorItem和args，那么则是已经在考虑范围内的异常,message使用改造后的
        super(item.buildErrorMessage(args));
        
        // 仍然保留这2个成员，是为了外部调用可以自己做一些展示用的适配和处理
        this.errorItem = item;
        this.args = args;
    }

    public ErrorItem getErrorItem() {
        return errorItem;
    }

    public Object[] getArgs() {
        return args;
    }

    // 只有子类可以涉及修改，比如executeException可能涉及下游传递时变更异常
    protected CarbonSqlException setErrorItem(ErrorItem errorItem) {
        this.errorItem = errorItem;
        return this;
    }

    // 只有子类可以涉及修改，比如executeException可能涉及下游传递时变更异常
    public CarbonSqlException setArgs(Object[] args) {
        this.args = args;
        return this;
    }
}
