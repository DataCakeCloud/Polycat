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
ROOT_PATH=${CUR_PATH}/..
SERVER_PACKAGE_PATH=$ROOT_PATH/assembly/target
POLYCAT_CATALOG_DIR=$SERVER_PACKAGE_PATH/polycat-catalog
POLYCAT_GATEWAY_CLIENT=$SERVER_PACKAGE_PATH/polycat-gateway-client
POLYCAT_MV_REWRITE=$SERVER_PACKAGE_PATH/polycat-mv-rewrite

source $CUR_PATH/common.sh

function start_foundationdb() {
    echo "---------------start foundationdb---------------"
    service foundationdb stop
    service foundationdb start
}

function print_help() {
    echo "usage: ./carbon.sh [all/server/catalog/carbonServer/carbonWorker/streamer/cli/help]"
    echo "       all: start all servers and cli"
    echo "       catalog: start catalog server"
    echo "       gateway: start gateway server"
    echo "       MV rewrite service: start MV rewrite service"
    echo "       cli: start gateway cli"
    echo "       help: show this help"
}

cd $SERVER_PACKAGE_PATH

if [ "$1" == "all" ]; then
    start_server
    if [ $? -ne 0 ]; then
      echo "start server failed"
      exit 1
    fi

    start_gateway_cli
    if [ $? -ne 0 ]; then
      echo "start gateway cli failed"
      exit 1
    fi
elif [ "$1" == "catalog" ]; then
    start_catalog
    if [ $? -ne 0 ]; then
      echo "start catalog server failed"
      exit 1
    fi
elif [ "$1" == "gateway" ]; then
    start_gateway
    if [ $? -ne 0 ]; then
      echo "start gateway server failed"
      exit 1
    fi
elif [ "$1" == "cli" ]; then
    start_gateway_cli
    if [ $? -ne 0 ]; then
      echo "start gateway cli failed"
      exit 1
    fi
elif [ "$1" == "MV rewrite service" ]; then
    start_mv_rewrite
    if [ $? -ne 0 ]; then
      echo "start MV rewrite service failed"
      exit 1
    fi
else
  print_help
fi

cd $CUR_PATH

