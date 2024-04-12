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

set -e
if [[ $# -gt 0 ]];then
  version=$1
else
  echo "must set version"
  exit 1
fi
obsutil cp -r -f obs://bdp-deploy-sg/BDP/BDP-hive-metastore-lib/hive-metastore-polycat/lib .
obsutil cp -f obs://bdp-deploy-sg/BDP/BDP-hadoop3.1.1/hadoop-3.1.1.tar.gz .
obsutil cp -f obs://bdp-deploy-sg/BDP/BDP-hms2.3.7/apache-hive-2.3.7-bin.tar.gz .

cp ../../lib/hadoop-huaweicloud-3.1.1-hw-45.jar lib
cp ../../catalog/hmsbridge-hive2/target/polycat-catalog-hmsbridge-hive2-0.1-SNAPSHOT.jar lib
cp ../../catalog/api/target/polycat-catalog-api-0.1-SNAPSHOT.jar lib
cp ../../catalog/authentication/target/polycat-catalog-authentication-0.1-SNAPSHOT.jar lib
cp ../../catalog/client/target/polycat-catalog-client-0.1-SNAPSHOT.jar lib
cp ../../polycat-hiveSDK/hive2/target/polycat-hiveSDK-hive2-0.1-SNAPSHOT.jar lib

