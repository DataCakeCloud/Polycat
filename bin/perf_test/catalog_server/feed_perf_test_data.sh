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
ROOT_PATH=${CUR_PATH}/../../..
SERVER_PACKAGE_PATH=$ROOT_PATH/assembly/target

POLYCAT_PERF_TEST_TOOLS_DIR=$SERVER_PACKAGE_PATH/polycat-perf-test-tools

cd $SERVER_PACKAGE_PATH

echo "--------------- unpack perf-test-tools---------------"
if [ -d $POLYCAT_PERF_TEST_TOOLS_DIR ]; then
  echo "rm -rf $POLYCAT_PERF_TEST_TOOLS_DIR"
  rm -rf $POLYCAT_PERF_TEST_TOOLS_DIR
fi
tar -zxf polycat-perf-test-tools-0.1-SNAPSHOT.tar.gz

CLASS_NAME=FeedCatalogMetadata

#echo "$POLYCAT_PERF_TEST_TOOLS_DIR/lib/*"

java -cp $POLYCAT_PERF_TEST_TOOLS_DIR/lib/polycat-test-0.1-SNAPSHOT.jar:$POLYCAT_PERF_TEST_TOOLS_DIR/lib/* $CLASS_NAME
