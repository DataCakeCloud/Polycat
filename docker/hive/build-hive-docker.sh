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

HADOOP=hadoop-3.1.1
HIVE_VERSION=2.3.7
HIVE=hive-${HIVE_VERSION}
HIVE_TAR=apache-${HIVE}-bin

DOCKER_NAME=hive
DOCKER_TAG=v${HIVE_VERSION}
DOCKER_FILE_PATH=$CUR_PATH/Dockerfile

if [ ! -d $TARGET_PATH ]; then
  mkdir -p $TARGET_PATH
fi
cd $TARGET_PATH

# download hadoop
if [ ! -f "${HADOOP}.tar.gz" ]; then
  wget https://mirrors.huaweicloud.com/apache/hadoop/common/${HADOOP}/${HADOOP}.tar.gz
fi

# download hive
if [ ! -f "${HIVE_TAR}.tar.gz" ]; then
  wget https://mirrors.huaweicloud.com/apache/hive/${HIVE}/${HIVE_TAR}.tar.gz
fi

# copy obsa
if [ ! -f "hadoop-huaweicloud-3.1.1-hw-45.jar" ]; then
  cp ../../../lib/hadoop-huaweicloud-3.1.1-hw-45.jar  $TARGET_PATH
fi

# download mysql-connector
if [ ! -f "mysql-connector-java-8.0.24.jar" ]; then
 wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.24/mysql-connector-java-8.0.24.jar
 wget https://repo1.maven.org/maven2/commons-lang/commons-lang/2.6/commons-lang-2.6.jar
fi

# copy hive2 hmsbridge
cp ../../../catalog/hmsbridge-hive2/target/lib/fastjson-*.jar  .
cp ../../../catalog/hmsbridge-hive2/target/polycat-catalog-hmsbridge-hive2-*.jar .
cp ../../../catalog/client/target/polycat-catalog-client-*.jar .
cp ../../../catalog/api/target/polycat-catalog-api-*.jar .
cp ../../../catalog/authentication/target/polycat-catalog-authentication-*.jar  .

cd -

#copy jre
if [ ! -d $JRE_PATH ]; then
  mkdir -p $JRE_PATH
fi
cp ../target/jre/* $JRE_PATH

## 1.3 copy sources
cp ../sources.list $TARGET_PATH

# build docker
bash $CUR_PATH/../build-docker.sh $DOCKER_NAME $DOCKER_TAG $DOCKER_FILE_PATH
