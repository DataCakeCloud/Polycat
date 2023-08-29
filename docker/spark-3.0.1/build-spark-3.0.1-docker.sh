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
VERSION=3.0.1
SPARK=spark-${VERSION}
HADOOP=hadoop3.2
SPARK_TAR=${SPARK}-bin-${HADOOP}

DOCKER_NAME=spark
DOCKER_TAG=v${VERSION}
DOCKER_FILE_PATH=$CUR_PATH/Dockerfile

if [ ! -d $TARGET_PATH ]; then
  mkdir -p $TARGET_PATH
fi
cd $TARGET_PATH

# download spark
if [ ! -f "${SPARK_TAR}.tgz" ]; then
  wget http://archive.apache.org/dist/spark/${SPARK}/${SPARK_TAR}.tgz
  tar xvf ${SPARK_TAR}.tgz
fi

# copy obsa
if [ ! -f "hadoop-huaweicloud-3.1.1-hw-45.jar" ]; then
  cp ../../../lib/hadoop-huaweicloud-3.1.1-hw-45.jar  ${SPARK_TAR}/jars
fi

# copy denpendency

cp ../../../lib/tini-amd64  ${SPARK_TAR}/
cp ../../target/jre/* ${SPARK_TAR}/
cp ../../sources.list  ${SPARK_TAR}/

# build docker, must in spark root dir
cd ${SPARK_TAR}
bash $CUR_PATH/../build-docker.sh $DOCKER_NAME $DOCKER_TAG $DOCKER_FILE_PATH
