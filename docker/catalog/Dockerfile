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

FROM ubuntu:18.04

RUN apt-get update -y && \
    apt-get install -y vim curl wget git lsof net-tools iputils-ping iproute2 && \
    mkdir -p /opt/jre/ && \
    wget https://javadl.oracle.com/webapps/download/AutoDL?BundleId=245050_d3c52aa6bfa54d3ca74e617f18309292 -O /opt/jre/jre-8u301-linux-x64.tar.gz && \
    tar xvf /opt/jre/jre-8u301-linux-x64.tar.gz -C /opt/jre/

ENV JAVA_HOME=/opt/jre/jre1.8.0_301
ENV CLASSPATH=$JAVA_HOME/lib
ENV PATH=$PATH:$JAVA_HOME/bin

ADD assembly/target/polycat-catalog-0.1-SNAPSHOT.tar.gz /opt/dash/
ENTRYPOINT bash /opt/dash/polycat-catalog/bin/catalog.sh