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
 * 生成计划过程中发生的异常
 * @singe 2021/1/20
 */
public class PlanException extends CarbonSqlException {
    private String jobId;
    
    // 不定义ErrorItem，则使用默认的PLAN_GENERAL_ERROR,参数就是reason
    public PlanException(String message) {
        super(message);
        super.setErrorItem(ErrorItem.PLAN_GENERAL_ERROR);
        super.setArgs(new Object[]{message});
    }

    // 不定义ErrorItem，则使用默认的PLAN_GENERAL_ERROR,参数就是reason
    public PlanException(Throwable t) {
        super(t);
        super.setErrorItem(ErrorItem.PLAN_GENERAL_ERROR);
        super.setArgs(new Object[]{ExceptionUtil.getMessageExcludeException(t)});
    }
    
    // 不定义ErrorItem，则使用默认的PLAN_GENERAL_ERROR
    public PlanException(String jobId, String message) {
        this(message);
        this.jobId = jobId;
    }
    
    // 执行期异常可能因为暴露给作业发起发的异常是不同的，因此传参也会不一样
    public PlanException(ErrorItem errorItem, Object... args) {
        super(errorItem, args);
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    public String getMessage() {
        String message = super.getMessage();
        if (getErrorItem() == ErrorItem.EXECUTE_GENERAL_ERROR && jobId != null) {
            return String.format("[jobId=%s] %s", jobId, message);
        } else {
            return message;
        }
    }
}
