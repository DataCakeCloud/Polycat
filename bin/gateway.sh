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
BIN_PATH=$ROOT_PATH/bin
LIB_PATH=$ROOT_PATH/lib
CONF_PATH=$ROOT_PATH/conf

source $ROOT_PATH/bin/common.sh

CLASS_NAME=io.polycat.gateway.GatewayServer

get_jar_package ${LIB_PATH}

java -Dlog4j.configuration=file:${CONF_PATH}/log4j.properties $JAVA_EXPORT -cp $g_all_jar_package $CLASS_NAME $CONF_PATH/gateway.conf $CONF_PATH/identity.conf

