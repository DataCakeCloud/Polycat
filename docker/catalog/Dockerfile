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
MAINTAINER polycat_developer

# update ubuntu sources
RUN mv /etc/apt/sources.list  /etc/apt/sources.list_bak
ADD ./target/sources.list /etc/apt/sources.list

# install vim
RUN apt-get update -y && apt-get install -y vim curl wget git lsof net-tools iputils-ping iproute2

# install jre
ADD ./target/jre/jre-8u301-linux-x64.tar.gz /opt/jre/
ENV JAVA_HOME=/opt/jre/jre1.8.0_301
ENV CLASSPATH=$JAVA_HOME/lib
ENV PATH=$PATH:$JAVA_HOME/bin

# install foundationdb client
#COPY ./target/foundationdb /opt/foundationdb
#RUN dpkg -i /opt/foundationdb/foundationdb-clients_6.3.15-1_amd64.deb

# install dash
ADD ./target/polycat-catalog-0.1-SNAPSHOT.tar.gz /opt/dash/

# start catalog
ENTRYPOINT bash /opt/dash/polycat-catalog/bin/catalog.sh