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

import io.polycat.catalog.common.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * 异常错误项
 * 服务化外部根据这个item，自己定制国际化和前台展示策略
 *
 * @singe 2021/1/20
 */
public enum ErrorItem {
    // 表达式不支持某数据类型
    EXPRESSION_UNSUPPORT_DATATYPE("wrong field type", "The expression[%s] does not support the field type [%s]", 2),
    // 通用异常,用于未考虑到的异常
    GENERAL_ERROR("error happend", 
            "reason=%s", 1),
    // 计划生成期通用异常,用于未考虑到的异常。
    PLAN_GENERAL_ERROR("create plan failed", 
            "reason=%s", 1),
    // 作业调度期通用异常,用于未考虑到的异常
    SCHEDULE_GENERAL_ERROR("schedule job failed", 
            "reason=%s", 1),
    // 作业运行期通用异常,用于未考虑到的异常
    EXECUTE_GENERAL_ERROR("processor execute failed",
            "reason=%s", 1),
    // sql执行失败，需要数据集名、代理id、合作方id三个参数
    SQL_EXECUTOR_FAIL(
            "failed to query sql in agent, but SQL execution information is sensitive. Please confirm the reason in 'task manager' of the agent", 
            "datasetName=%s, workerId=%s, partnerAlias=%s", 3),
    // 连接器无效，需要数据集、连接器id、原因三个参数
    CONNECTOR_INVALID("try query dataset failed because of connector invalid", 
            "datasetName=%s, connector=%s, reason=%s", 3),
    // 序列化失败
    DESERIALIZATION_ERROR("deserialization failed. Please upgrade the node to the latest version.",
            "class=%s, reason=%s", 2),
    // 序列化失败
    SERIALIZATION_ERROR("serialization failed. Please upgrade the node to the latest version.",
            "class=%s, reason=%s", 2),
    // 隐私规则验证失败，其他原因
    PRIVACY_RULE_VALID_FAIL_UNKOWNREASON("Privacy rule verification failed", "reason=%s", 1),
    // 隐私规则验证失败,语法原因
    // 参数：合作方别名、数据集名、字段名、字段分类、对应语法
    PRIVACY_RULE_VALID_FAIL_GRAMMER_SUPPORT(Constants.VERIFICATION_FAILED, 
            Constants.NOT_SUPPUPRT_GRAMMER_FORMAT, 5),
    // 聚合的时候避免使用唯一id去分组。 参数：聚合字段名、id名
    AGG_WITH_GROUP_BY_ID(Constants.VERIFICATION_FAILED, Constants.NOT_SUPPUPRT_GRAMMER_FORMAT
            + ". Please not use id %s to group when select %s in final result", 5 + 2),
    // 可逆计算式  参数为敏感字段，计算式
    REVERSIBLE_CACULATE(Constants.VERIFICATION_FAILED, Constants.NOT_SUPPUPRT_GRAMMER_FORMAT
            + ". %s may be pushed backward in %s. Please increase the number of fields in the expression or refuse to use it",
            5 + 2),
    // 在agent侧sql解析失败, 需要代理名称、连接器类型，sql语句,原因
    SQL_PARSE_FAIL_IN_AGENT("sql parse failed in agent", "workerId=%s, sql=%s,reason=%s", 3),
    // 在建立作业时sql解析失败
    SQL_PARSE_FAIL("sql parse failed, please check sql", "reason=%s", 1),
    // SqlCheckExcpetion里错误码，后续SqlCheckExcpetion建议全部细化成不同异常码
    SQL_CHECK_FAIL("sql check failed", "reason=%s", 1),
    // 不支持的语法
    UNSUPPORT_EXPRESSION("SQL contains an unsupported expression", "unsupportExpression=%s", 1),
    // 相关代理节点未就绪
    AGENT_UNREADY("The related execution agent nodes is not ready, please inform partner to confirm the reason", 
            "partnerAlias=%s", 1),
    // 准备执行的聚合器节点未就绪
    AGG_UNREADY("The related execution agg cluster is not ready",
            "aggId=%s, aggHost=%s, state=%s", 3), 
    // 聚合器集群都未就绪
    AGG_CLUSTER_UNREADY("The agg cluster is not ready", "reason=%s", 1),
    // sql别名转换失败
    SQL_ALIAS_CONVERT_FAIL("There is a problem in sql alias conversion", 
            "covertSql=%s", 1),
    // 下发的执行计划存在问题,需要尝试更换数据集
    DISTRUIBUTE_EXECUTOR_PLAN_WRONG("There is an error in the execution plan distributed to the agent. Please contact tics development to confirm the reason or try other datasets",
            "workerId=%s, partnerAlias=%s", 2),
    // 连接器处执行sql查询失败
    SQL_QUERY_FAIL_BY_CONNECTOR("SQL query failed because of connector", "connector=%s, reason=%s", 2),
    // 结果存储失败
    RESULT_SAVE_FAIL("job result save failed", "reason=%s, filepath=%s", 2),
    // 作业发生取消
    JOB_CANCEL("job cancel", "", 0),
    // 部署任务到节点失败
    DEPLOY_STAGE_TO_WORKER_FAIL("deploye stage to worker failed", "workerId=%d,reason=%s", 2),
    // 解密数据失败
    DECRYPT_DATA_FAIL("Decrypt data failed", "nodeId=%s", 1),
    // 加密数据失败
    ENCRYPT_DATA_FAIL("Encrypt data failed", "nodeId=%s", 1),
    DATASET_NOEXIST("dataset is not exist", "partnerAlias=%,datasetName=%s", 2),
    DATASET_FIELD_NOEXIST("dataset field is not exist", "partnerAlias=%,datasetName=%s, field=%s", 3),
    DATASET_TYPE_WRONG("dataset type is wrong", "partnerAlias=%,datasetName=%s, needType=%s", 3),
    // join收敛检查不通过
    // 计算过程中发现最后join阶段存在隐私数据泄露风险， 字段%s可能会泄露%s这个唯一标识的具体值， 请避免在最后加select %s， 或者通知合作方%s把字段%s改成维度
    JOIN_CONVERGENCE_CHECK_FAIL("During the calculation, it is found that there is a risk of privacy data leakage in the last join stage", 
            " The field[%s] may disclose the specific value of the uniqueID[%s]. please avoid 'select %s ...' in final stage, or inform the partner %s to change the field %s to dimension", 
            // 参数为 project字段名称、 join字段名称、 project字段名称、 合作方别名、join字段名称
            5),
    // 计算过程中发现最后聚合阶段存在隐私数据泄露风险， 聚合操作(字段%s)可能会泄露 (字段%s)这个度量的具体值， 请增加分组后的组内数量， 或者通知合作方%s把字段%s改成维度
    AGGREGATE_CONVERGENCE_CHECK_FAIL("During the calculation, it is found that there is a risk of privacy data leakage in the final aggregation stage",
            "%s(%s) may disclose the value of the measurement[%s]. Please increase the number of groups after grouping, or inform the partner %s to change the field %s to dimension",
            // 参数为 聚合类型、 字段名称、 字段名称、 合作方名称，字段名称
            5),
    // rpc调用失败
    RPC_CALL_FAIL("Remote procedure call failed", "Call [%s] failed, error message: %s", 2),
    // 创建文件失败
    CREATE_FILE_FAIL("Create file failed", "Create file [%s] failed, error message: %s", 2),
    // 左右流都是大表，导致左右表的收集队列全满，以助于mapJoin无法往下发送数据，引发阻塞。需要用户检查是否存在大大表连接
    JOIN_DATA_INPUT_TOOBIG("The amount of data is too large to process. Please check whether there are large tables and large table connections",
            "block data line count=%s", 1),
    JOB_CANCELLED_STATE_CHANGE("Job is cancelled", "Job is cancelled because agent/agg node [%s] state changed", 1),
    // 获取上游输入失败。 参数:  等待的输入processorId, 超时时间
    PROCESSOR_INPUT_LOST("wait input timeout", "node wait processor %s input timeout %d ms, input node may have a exception", 2),
    // 建立双向数据流通道失败
    CREATE_DATA_CHANNEL_FAILED("create bidirectional data channel with node failed",
            "Please check whether the network communication between nodes. reason=%s", 1)
    ;

    private static final Logger LOGGER = Logger.getLogger(ErrorItem.class.getName());

    // 错误信息描述，不包含格式化信息，主要用于描述和提示
    private String desc;
    
    // 参考的错误细则提示格式化字符串, 用于提示一些细则，分开的话便于确认有哪些参数,不容易看漏
    private String argsFormat;

    public String getArgsFormat() {
        return argsFormat;
    }

    // 参数个数，用于校验
    private int argsCount;

    ErrorItem(String desc, String argsFormat, int argsCount) {
        this.desc = desc;
        this.argsFormat = argsFormat;
        this.argsCount = argsCount;
    }

    /**
     * 根据传参生成错误消息，生成desc + 格式化信息
     * @param args 消息参数
     * @return
     */
    public String buildErrorMessage(Object[] args) {
        List<Object> newArg = new ArrayList<>();
        // 有的参数可能是object[]，解析合并一下
        for (Object arg : args) {
            if (arg instanceof Object[]) {
                newArg.addAll(Arrays.asList((Object[])arg));    
            } else {
                newArg.add(arg);
            }
        }
        args = newArg.toArray();
        
        if (args == null || args.length == 0) {
            return desc;
        }

        if (args.length != argsCount) {
            // 参数个数不一致，则不做格式化，只返回描述信息
            LOGGER.error("item {} build error message fail, input args count {} is not equal {}", 
                    this.name(), args.length, argsCount);
            return desc;
        }
        
        return desc + "(" + String.format(argsFormat, args) + ")";
    }

    private static class Constants {
        public static final String NOT_SUPPUPRT_GRAMMER_FORMAT
                = "The field type of %s.%s.%s is %s, %s is not suppported in the final result";

        public static final String VERIFICATION_FAILED = "Privacy rule verification failed";
    }
}
