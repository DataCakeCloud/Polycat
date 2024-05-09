/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polycat.catalog.client;

import com.google.gson.Gson;
import io.polycat.catalog.common.lineage.*;
import io.polycat.catalog.common.model.LineageInfo;
import io.polycat.catalog.common.plugin.request.*;
import io.polycat.catalog.common.plugin.request.input.LineageInfoInput;
import io.polycat.catalog.common.utils.LineageUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class SDKLineageTest {

    /**
     * TODO
     * @return
     */
    protected static PolyCatClient getClient() {
        PolyCatClient client = new PolyCatClient();
        client.setContext(CatalogUserInformation.doAuth("test", "dash"));
        client.getContext().setTenantName("shenzhen");
        client.getContext().setProjectId("default_project");
        client.getContext().setToken("nonull");
        return client;
    }

    @Test
    public void test_updateLineage() {
        final PolyCatClient client = getClient();
        String catalogName = "c_" + System.currentTimeMillis();
        LineageInfoInput input = makeLineageInput(catalogName);
        UpdateDataLineageRequest request = new UpdateDataLineageRequest(client.getProjectId(), input);
        client.updateDataLineage(request);
        SearchDataLineageRequest searchDataLineageRequest = new SearchDataLineageRequest(client.getProjectId(), EDbType.HIVE, ELineageObjectType.TABLE, LineageUtils.getTableQualifiedName(catalogName, "d1", "t1"),
                2, null, null);
        LineageInfo lineageInfo = client.searchDataLineageGraph(searchDataLineageRequest);
        assert lineageInfo != null;
        assertEquals(2, lineageInfo.getNodes().size());
        assertEquals(1, lineageInfo.getRelations().size());
        System.out.println(lineageInfo.toString());;
        LineageFact dataLineageFact = client.getDataLineageFact(new GetDataLineageFactRequest(client.getProjectId(), lineageInfo.getRelations().get(0).getJobFactId()));
        assert dataLineageFact != null;
        System.out.println(dataLineageFact.toString());

        searchDataLineageRequest.setObjectType(ELineageObjectType.COLUMN);
        searchDataLineageRequest.setQualifiedName(LineageUtils.getColumnQualifiedName(catalogName, "d1", "t1", "a2"));
        LineageInfo lineageInfo1 = client.searchDataLineageGraph(searchDataLineageRequest);
        assert lineageInfo1 != null;
        assertEquals(3, lineageInfo1.getNodes().size());
        assertEquals(2, lineageInfo1.getRelations().size());

    }



    private LineageInfoInput makeLineageInput(String catalogName) {
        LineageInfoInput lineageInfo = new LineageInfoInput();
        lineageInfo.setCreateTime(System.currentTimeMillis());
        lineageInfo.setJobFact(makeJobFact());
        String t1 = LineageUtils.getTableQualifiedName(catalogName, "d1", "t1");
        String t2 = LineageUtils.getTableQualifiedName(catalogName, "d2", "t2");

        LineageNode lineageNode = new LineageNode(t1, ELineageObjectType.TABLE, ELineageSourceType.LINEAGE, false);
        HashMap<String, Object> params = new HashMap<>();
        params.put("spark.kyuubi.session.app.templatecode", "Hive2Hive");
        params.put("spark.kyuubi.session.app.cluster",
                "type:k8s,region:us-east-1,sla:normal,provider:aws,rbac.cluster:bdp-prod ");
        params.put("spark.kubernetes.driver.label.kyuubi-unique-app", "gateway");
        lineageNode.setParams(params);
        lineageInfo.addNode(lineageNode);
        lineageInfo.addNode(new LineageNode(t2, ELineageObjectType.TABLE, ELineageSourceType.LINEAGE, false));
        lineageInfo.addNode(
                new LineageNode(LineageUtils.getColumnQualifiedName(t1, "a1"), ELineageObjectType.COLUMN,
                        ELineageSourceType.LINEAGE, false));
        lineageInfo.addNode(
                new LineageNode(LineageUtils.getColumnQualifiedName(t1, "a2"), ELineageObjectType.COLUMN,
                        ELineageSourceType.LINEAGE, false));
        lineageInfo.addNode(
                new LineageNode(LineageUtils.getColumnQualifiedName(t1, "a3"), ELineageObjectType.COLUMN,
                        ELineageSourceType.LINEAGE, false));
        lineageInfo.addNode(
                new LineageNode(LineageUtils.getColumnQualifiedName(t2, "a1"), ELineageObjectType.COLUMN,
                        ELineageSourceType.LINEAGE, false));
        lineageInfo.addNode(
                new LineageNode(LineageUtils.getColumnQualifiedName(t2, "a2"), ELineageObjectType.COLUMN,
                        ELineageSourceType.LINEAGE, false));
        lineageInfo.addNode(
                new LineageNode(LineageUtils.getColumnQualifiedName(t2, "a3"), ELineageObjectType.COLUMN,
                        ELineageSourceType.LINEAGE, false));
        lineageInfo.addNode(
                new LineageNode(LineageUtils.getColumnQualifiedName(t2, "a4"), ELineageObjectType.COLUMN,
                        ELineageSourceType.LINEAGE, false));
        LineageInfoInput.LineageRsInput lineageRs = new LineageInfoInput.LineageRsInput(ELineageType.TABLE_DEPEND_TABLE);
        lineageRs.setDownstreamNode(EDbType.HIVE, ELineageObjectType.TABLE, t1);
        lineageRs.addUpstreamNode(EDbType.HIVE, ELineageObjectType.TABLE, t2);
        lineageInfo.addRelationShipMap(lineageRs);
        /**
         * [ColLine [toNameParse=a1, colCondition=default.t2.a1, fromNameSet=[default.t2.a1], conditionSet=[], toTable=default.t1, toName=null],
         * ColLine [toNameParse=a2, colCondition=default.t2.a2 + default.t2.a3, fromNameSet=[default.t2.a3, default.t2.a2], conditionSet=[], toTable=default.t1, toName=null],
         * ColLine [toNameParse=a3, colCondition=length(default.t2.a4), fromNameSet=[default.t2.a4], conditionSet=[], toTable=default.t1, toName=null]]
         */
        addColLineageRs(lineageInfo, LineageUtils.getColumnQualifiedName(t1, "a1"),
                LineageUtils.getColumnQualifiedName(t2, "a1"), ELineageType.FIELD_DEPEND_FIELD,
                EDependWay.SIMPLE, null);
        addColLineageRs(lineageInfo, LineageUtils.getColumnQualifiedName(t1, "a2"),
                LineageUtils.getColumnQualifiedName(t2, "a2"), ELineageType.FIELD_DEPEND_FIELD,
                EDependWay.EXPRESSION, "default.t2.a2 + default.t2.a3");
        addColLineageRs(lineageInfo, LineageUtils.getColumnQualifiedName(t1, "a2"),
                LineageUtils.getColumnQualifiedName(t2, "a3"), ELineageType.FIELD_DEPEND_FIELD,
                EDependWay.EXPRESSION, "default.t2.a2 + default.t2.a3");
        addColLineageRs(lineageInfo, LineageUtils.getColumnQualifiedName(t1, "a3"),
                LineageUtils.getColumnQualifiedName(t2, "a4"), ELineageType.FIELD_DEPEND_FIELD,
                EDependWay.EXPRESSION, "length(default.t2.a4)");
        log.info("{}", new Gson().toJson(lineageInfo));
        log.info("{}", lineageInfo.toString());
        System.out.println(lineageInfo.toString());


        return lineageInfo;
    }

    private static void addColLineageRs(LineageInfoInput lineageInfo, String downColQualifiedName,
                                        String upColQualifiedName, ELineageType lineageType, EDependWay dependWay, String setDependCodeSegment) {
        LineageInfoInput.LineageRsInput lineageRs = new LineageInfoInput.LineageRsInput(lineageType, dependWay, setDependCodeSegment);
        lineageRs.setDownstreamNode(EDbType.HIVE, ELineageObjectType.COLUMN, downColQualifiedName);
        lineageRs.addUpstreamNode(EDbType.HIVE, ELineageObjectType.COLUMN, upColQualifiedName);
        lineageInfo.addRelationShipMap(lineageRs);
    }

    private LineageFact makeJobFact() {
        LineageFact lineageFact = new LineageFact();
        lineageFact.setJobStatus(EJobStatus.FAILED);
        lineageFact.setCluster("dc-cluster1");
        lineageFact.setExecuteUser("user1");
        lineageFact.setJobName("spark-app1");
        lineageFact.setJobId("app-fasdkfksadf");
        lineageFact.setJobType("spark-sql");
        lineageFact.setProcessType("scheduler");
        lineageFact.setSql("insert into table t1 select a1, a2+a3 a2, length(a4) a3 from t2");
        lineageFact.setStartTime(System.currentTimeMillis());
        lineageFact.setEndTime(System.currentTimeMillis() + 100000);
        lineageFact.setErrorMsg("NullPointException");
        HashMap<String, Object> params = new HashMap<>();
        params.put("spark.kyuubi.session.app.templatecode", "Hive2Hive");
        params.put("spark.kyuubi.session.app.cluster",
                "type:k8s,region:us-east-1,sla:normal,provider:aws,rbac.cluster:bdp-prod ");
        params.put("spark.kubernetes.driver.label.kyuubi-unique-app", "gateway");
        lineageFact.setParams(params);
        return lineageFact;
    }
}
