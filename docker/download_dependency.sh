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


CUR_PATH="$( cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_PATH=$CUR_PATH/target
JRE_PATH=$TARGET_PATH/jre

# download jre 11
if [ ! -d $JRE_PATH ]; then
  mkdir -p $JRE_PATH
fi
cd $JRE_PATH
if [ ! -f "jre-8u301-linux-x64.tar.gz" ]; then
  wget https://javadl.oracle.com/webapps/download/AutoDL?BundleId=245050_d3c52aa6bfa54d3ca74e617f18309292 -O jre-8u301-linux-x64.tar.gz
fi
cd -
