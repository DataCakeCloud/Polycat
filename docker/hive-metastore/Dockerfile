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

WORKDIR /opt
ENV HIVE_HOME=/opt/apache-hive-2.3.7-bin
ENV HADOOP_HOME=/opt/hadoop-3.1.1
ENV HADOOP_CONF_DIR=${HIVE_HOME}/conf

# RUN wget https://mirrors.huaweicloud.com/apache/hadoop/common/hadoop-3.1.1/hadoop-3.1.1.tar.gz -O - | tar -xz
# RUN wget -c https://mirrors.huaweicloud.com/apache/hive/hive-2.3.7/apache-hive-2.3.7-bin.tar.gz -O - | tar -xz

ADD hadoop-3.1.1.tar.gz /opt/
ADD apache-hive-2.3.7-bin.tar.gz /opt/

COPY lib/* ${HIVE_HOME}/lib/
RUN rm ${HIVE_HOME}/lib/jackson-core-2.6.5.jar -f && \
 rm ${HIVE_HOME}/lib/jackson-annotations-2.6.0.jar -f && \
 rm ${HIVE_HOME}/lib/jackson-databind-2.6.5.jar -f && \
 rm ${HIVE_HOME}/lib/jackson-jaxrs-base-2.4.6.jar -f && \
 rm ${HIVE_HOME}/lib/jackson-jaxrs-json-provider-2.4.6.jar -f && \
 rm ${HIVE_HOME}/lib/jackson-module-jaxb-annotations-2.4.6.jar -f && \
 rm ${HIVE_HOME}/lib/avatica-1.8.0.jar -f


WORKDIR $HIVE_HOME
EXPOSE 9083

ENTRYPOINT ["bin/hive"]
CMD ["--service", "metastore"]