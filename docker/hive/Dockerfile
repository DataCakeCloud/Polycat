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
MAINTAINER dash_developer
ARG HADOOP=hadoop-3.1.1
ARG HIVE_TAR=apache-hive-2.3.7-bin

RUN set -ex && \
    ln -s /lib /lib64 && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd && \
    rm -rf /var/cache/apt/*


RUN mv /etc/apt/sources.list /etc/apt/sources.list.bak
ADD ./target/sources.list   /etc/apt/sources.list
RUN apt-get update && \
    apt-get install -y bash tini libc6 libpam-modules krb5-user libnss3 procps &&\
    apt-get install -y vim curl wget git lsof net-tools iputils-ping iproute2

ADD ./target/jre/jre-8u301-linux-x64.tar.gz /opt/
ENV JAVA_HOME=/opt/jre1.8.0_301
ENV CLASSPATH=$JAVA_HOME/lib
ENV PATH=$PATH:$JAVA_HOME/bin

ADD ./target/${HADOOP}.tar.gz    /opt/
ENV HADOOP_HOME=/opt/${HADOOP}
ENV PATH=$PATH:$HADOOP_HOME/bin
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native

ADD ./target/${HIVE_TAR}.tar.gz /opt/
ENV HIVE_HOME=/opt/${HIVE_TAR}
ENV PATH=$PATH:$HIVE_HOME/bin
#RUN mv $HIVE_HOME/lib/guava-14.0.1.jar $HIVE_HOME/lib/guava-14.0.1.jar.bak && \
#    cp $HADOOP_HOME/share/hadoop/common/lib/guava-27.0-jre.jar  $HIVE_HOME/lib
   

COPY ./target/mysql-connector-java-8.0.24.jar $HIVE_HOME/lib/
COPY ./target/hadoop-huaweicloud-3.1.1-hw-45.jar  $HIVE_HOME/lib/
COPY ./target/commons-lang-2.6.jar   $HIVE_HOME/lib/

COPY ./target/polycat-catalog-*.jar $HIVE_HOME/lib/
COPY ./target/fastjson-*.jar $HIVE_HOME/lib/
COPY ./entrypoint.sh /
RUN chmod +x /entrypoint.sh


WORKDIR $HIVE_HOME
EXPOSE 9083

ENTRYPOINT ["/entrypoint.sh"]

