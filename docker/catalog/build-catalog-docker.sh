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
FOUNDATIONDB_PATH=$TARGET_PATH/foundationdb
JRE_PATH=$TARGET_PATH/jre
TARGET_PATH_MYPERF4J=$TARGET_PATH/myperf4j
THIRDPART_TARGET_PATH=$CUR_PATH/../../thirdpart/target
CATALOG_DIR_NAME=polycat-catalog

DOCKER_NAME=catalog
DOCKER_TAB=latest
DOCKER_FILE_PATH=$CUR_PATH/Dockerfile

## 1. docker build prepare
### 1.1 download foundationdb
#if [ ! -d $FOUNDATIONDB_PATH ]; then
#  mkdir -p $FOUNDATIONDB_PATH
#fi
#cd $FOUNDATIONDB_PATH
#if [ ! -f "foundationdb-clients_6.3.15-1_amd64.deb" ]; then
#  wget https://www.foundationdb.org/downloads/6.3.15/ubuntu/installers/foundationdb-clients_6.3.15-1_amd64.deb
#fi
#if [ ! -f "foundationdb-server_6.3.15-1_amd64.deb" ]; then
#  wget https://www.foundationdb.org/downloads/6.3.15/ubuntu/installers/foundationdb-server_6.3.15-1_amd64.deb
#fi
#cd -

## 1.2 copy jre
if [ ! -d $JRE_PATH ]; then
  mkdir -p $JRE_PATH
fi
cp ../target/jre/* $JRE_PATH

## 1.3 copy sources
cp ../sources.list $TARGET_PATH

## 1.4 copy catalog
if [ -f ../../assembly/target/polycat-catalog-with-thirdpart-0.1-SNAPSHOT.tar.gz ]; then
  #unit name
  mkdir $TARGET_PATH/$CATALOG_DIR_NAME && tar -zxvf ../../assembly/target/polycat-catalog-with-thirdpart-0.1-SNAPSHOT.tar.gz -C $TARGET_PATH/$CATALOG_DIR_NAME --strip-components 1
  cd $TARGET_PATH
  tar -zcvf polycat-catalog-0.1-SNAPSHOT.tar.gz $CATALOG_DIR_NAME
  cd -
else
  cp ../../assembly/target/polycat-catalog-0.1-SNAPSHOT.tar.gz $TARGET_PATH
fi

# 2. docker build
bash $CUR_PATH/../build-docker.sh $DOCKER_NAME $DOCKER_TAB $DOCKER_FILE_PATH