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
CONF_PATH=$ROOT_PATH/conf
CLASS_NAME=io.polycat.metrics.FdbExporter

java -jar ${LIB_PATH}/polycat-metrics-0.1-SNAPSHOT-jar-with-dependencies.jar $CLASS_NAME $CONF_PATH/fdb-metrics.conf
