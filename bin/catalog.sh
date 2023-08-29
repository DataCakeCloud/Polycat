#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


FULL_PATH=${BASH_SOURCE[0]}
ROOT_PATH=$(dirname $FULL_PATH)/..
LIB_PATH=$ROOT_PATH/lib
LOCAL_MYPERF4J_PATH=$ROOT_PATH/lib

#判断是否编译出myperf4j，编译出来则使用
if [ -f $LOCAL_MYPERF4J_PATH/MyPerf4J-ASM-3.0.0.jar ]; then
    MYPERF_PATH=$LOCAL_MYPERF4J_PATH
else
    MYPERF_PATH=""
fi

if [[ -v JAVA_OPT_PARAM ]]; then
  JAVA_OPTS="${JAVA_OPTS} ${JAVA_OPT_PARAM}"
fi

if [ ! -n "$MYPERF_PATH" ]; then
  java ${JAVA_OPTS} -jar ${LIB_PATH}/polycat-catalog-server-0.1-SNAPSHOT-exec.jar $*
else
  #echo "used: " ${MYPERF_PATH}/lib/MyPerf4J-ASM-3.0.0.jar
  java ${JAVA_OPTS} -javaagent:${MYPERF_PATH}/MyPerf4J-ASM-3.0.0.jar -DMyPerf4JPropFile=${MYPERF_PATH}/MyPerf4J.properties -jar ${LIB_PATH}/polycat-catalog-server-0.1-SNAPSHOT-exec.jar $*
fi