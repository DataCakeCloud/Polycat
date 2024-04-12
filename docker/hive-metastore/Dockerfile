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

FROM openjdk:8
RUN set -ex && \
    apt-get upgrade  && \
    apt-get install bash && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh

ADD obslib/lib/ /opt/lib
ADD apache-hive-2.3.7-bin /opt/apache-hive-2.3.7-bin
ADD hadoop-3.1.1.tar.gz  /opt/
ADD lib/hadoop-huaweicloud-3.1.1-hw-45.jar /opt/apache-hive-2.3.7-bin/lib
ADD catalog/hmsbridge-hive2/target/polycat-catalog-hmsbridge-hive2-0.1-SNAPSHOT.jar /opt/apache-hive-2.3.7-bin/lib
ADD catalog/api/target/polycat-catalog-api-0.1-SNAPSHOT.jar /opt/apache-hive-2.3.7-bin/lib
ADD catalog/authentication/target/polycat-catalog-authentication-0.1-SNAPSHOT.jar /opt/apache-hive-2.3.7-bin/lib
ADD catalog/client/target/polycat-catalog-client-0.1-SNAPSHOT.jar /opt/apache-hive-2.3.7-bin/lib

ENV HIVE_HOME=/opt/apache-hive-2.3.7-bin
ENV HADOOP_HOME=/opt/hadoop-3.1.1
ENV HADOOP_CONF_DIR=${HIVE_HOME}/conf

WORKDIR $HIVE_HOME
EXPOSE 9083
ENTRYPOINT ["bin/hive","--service", "metastore"]