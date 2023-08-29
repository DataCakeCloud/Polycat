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


function recursive_copy_file() {
    dirlist=$(ls $1)
    for name in ${dirlist[*]}
    do
      if [ -f $1/$name ] && [ "$1/$name" == "./pom.xml" ]; then
          continue
      fi
      if [ -f $1/$name ] && [ "$name" == "pre_build.sh" ]; then
          continue
      fi
      #echo "replace: $1/$name"
      if [ -f $1/$name ]; then
          # 如果是文件，并且$2存在该目录，则直接copy，否则报错
          #echo $1/$name "-> to " $2/$name
          cp $1/$name $2/$name
      elif [ -d $1/$name ]; then
          if [ ! -d $2/$name ]; then
              echo "dir not exist" $1/$name $2/$name
            exit
          fi
      # 递归
      recursive_copy_file $1/$name $2/$name
      fi
    done
}

function copy_Myperf4J() {
    source_dir="."
    dest_dir="../MyPerf4J"
    recursive_copy_file $source_dir $dest_dir
}

copy_Myperf4J
