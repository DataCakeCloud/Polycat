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


OS=`uname -s`
g_all_jar_package=
g_delimiter=":"
if [[ ${OS} == "Linux" || ${OS} == "Darwin" ]];then
  g_delimiter=":"
else
  g_delimiter=";"
fi

JAVA_EXPORT=""

function get_jar_package() {
  first_flag=1

  for file_a in $1/*
do
  temp_file=`basename $file_a`
  #echo $temp_file
  if [ $first_flag == 1 ]
  then
    first_flag=0
    g_all_jar_package=$1/$temp_file
  else
    g_all_jar_package=$g_all_jar_package$g_delimiter$1/$temp_file
  fi
done
}

function kill_process() {
    if [ $# != 1 ]
    then
      echo "miss parameter procedure name"
      exit 1
    fi

    PROCESS=`ps -ef | grep $1 | grep -v grep | grep -v PPID | awk '{ print $2}'`
    for i in $PROCESS
    do
      echo "kill the $1 process [ $i ]"
      kill -9 $i
    done
}

function get_process() {
    if [ $# != 1 ]
    then
      echo "miss parameter procedure name"
      exit 1
    fi

    PROCESS=`ps -ef | grep $1 | grep -v grep | grep -v PPID | awk '{ print $2}'`
    count=10
    while [ ! -n "$PROCESS" -a $count -gt 0 ]
    do
      sleep 1
      let count--
      PROCESS=`ps -ef | grep $1 | grep -v grep | grep -v PPID | awk '{ print $2}'`
    done

    if [ ! -n "$PROCESS" ]; then
      echo "get process:$1 failed"
      exit 1;
    fi
}

function start_catalog() {
    echo "---------------start catalog server ---------------"
    if [ -d $POLYCAT_CATALOG_DIR ]; then
      echo "rm -rf $POLYCAT_CATALOG_DIR"
      rm -rf $POLYCAT_CATALOG_DIR
    fi
    if [ -f ./polycat-catalog-with-thirdpart-0.1-SNAPSHOT.tar.gz ]; then
      mkdir $POLYCAT_CATALOG_DIR && tar -zxvf polycat-catalog-with-thirdpart-0.1-SNAPSHOT.tar.gz -C $POLYCAT_CATALOG_DIR --strip-components 1
    else
      tar -zxf polycat-catalog-0.1-SNAPSHOT.tar.gz
    fi
    cd $POLYCAT_CATALOG_DIR
    nohup ./bin/catalog.sh &

    get_process "polycat-catalog-server"
    if [ $? -ne 0 ]; then
      exit 1
    fi

    cd -
}

function clean_environment() {
    if [[ ${OS} == "Linux" || ${OS} == "Darwin" ]];then
      echo "---------------Linux clean environment---------------"
      process_name_array=("polycat-catalog-server")
      for element in ${process_name_array[*]}
      do
        kill_process $element
        if [ $? -ne 0 ];then
          echo "kill process:$element failed"
          exit 1
        fi
      done
    else
      echo "---------------Windows clean environment---------------"
    fi
}
