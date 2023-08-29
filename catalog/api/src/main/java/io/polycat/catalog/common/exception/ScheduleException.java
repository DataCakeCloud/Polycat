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
 * 作业调度和部署中发生的异常
 * @singe 2021/1/20
 */
public class ScheduleException extends CarbonSqlException {
    // 发生问题的节点id
    private String nodeName;

    public ScheduleException(String message) {
        super(message);
        super.setErrorItem(ErrorItem.SCHEDULE_GENERAL_ERROR);
        super.setArgs(new Object[]{message});
    }

    // 不定义ErrorItem，则使用默认的PLAN_GENERAL_ERROR
    public ScheduleException(String nodeName, String message) {
        this(message);
        this.nodeName = nodeName;
    }
    
    // 执行期异常可能因为暴露给作业发起发的异常是不同的，因此传参也会不一样
    public ScheduleException(ErrorItem errorItem, Object... args) {
        super(errorItem, args);
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    @Override
    public String getMessage() {
        String message = super.getMessage();
        if (getErrorItem() == ErrorItem.EXECUTE_GENERAL_ERROR && nodeName != null) {
            return String.format("[nodeName=%s] %s", nodeName, message);
        } else {
            return message;
        }
    }
}
