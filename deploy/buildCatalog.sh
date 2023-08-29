#!/usr/bin/env bash
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

source  /etc/profile
set -x

CURRENT_DIR=$(
  #shellcheck disable=SC2046
  cd $(dirname "$0")
  pwd
) || exit

PROJECT_ROOT="${CURRENT_DIR}"/..
WORK_DIR=${PROJECT_ROOT}
DEPLOY_DIR=${CURRENT_DIR}

component_name="catalog"
module_dir="server"
server_name="polycat-catalog-server"
server_version="0.1-SNAPSHOT"
buildConfigPath="${PROJECT_ROOT}/.build_config"
target_name="catalog"

function modify_pom_for_intranet() {
    cd "${PROJECT_ROOT}"
    sed -i "s#https://mirrors.huaweicloud.com/repository/maven/#http://wlg1.artifactory.cd-cloud-artifact.tools.huawei.com/artifactory/cbu-maven-public/#g"  catalog/authorization/pom.xml
    sed -i "s#huaweicloudsdk.org.apache.ranger#org.apache.ranger#g"     catalog/ranger-authorization/pom.xml

    sed -i "s#https://mirrors.huaweicloud.com/repository/maven/#http://wlg1.artifactory.cd-cloud-artifact.tools.huawei.com/artifactory/cbu-maven-public/#g" catalog/authentication/pom.xml
    sed -i "s#huaweicloudsdk.com.huawei.apigateway#com.huawei.apigateway#g"  catalog/authentication/pom.xml

    sed -i "s#https://mirrors.huaweicloud.com/repository/maven/#http://wlg1.artifactory.cd-cloud-artifact.tools.huawei.com/artifactory/cbu-maven-public/#g"  catalog/authticator-oneaccess/pom.xml
    sed -i "s#huaweicloudsdk.com.huawei.apigateway#com.huawei.apigateway#g"  catalog/authticator-oneaccess/pom.xml
    return 0
}

if [ ! -d "${buildConfigPath}" ]; then
  mkdir "${buildConfigPath}" -p
fi

cd "${WORK_DIR}" || exit

modify_pom_for_intranet
mvn clean package -DskipTests

cd "${WORK_DIR}"/"${component_name}"/"${module_dir}"/target ||exit

mkdir "${target_name}"
cp -rf "${server_name}"-"${server_version}"/* ./"${target_name}"/
tar -cvzf "${target_name}".tar.gz "${target_name}"
# shellcheck disable=SC2181
if [ $? -ne 0 ]; then
  exit 1
fi
rm -rf "${target_name}"

COMPONENT_TAR=$(ls "${WORK_DIR}"/"${component_name}"/"${module_dir}"/target/"${target_name}".tar.gz)
rm -rf "${DEPLOY_DIR}"/${target_name}
tar zxf "${COMPONENT_TAR}" -C "${DEPLOY_DIR}"

xbuild package --workspace "${WORK_DIR}"  --config "${DEPLOY_DIR}"/xbuild_catalog_rpm.yml
