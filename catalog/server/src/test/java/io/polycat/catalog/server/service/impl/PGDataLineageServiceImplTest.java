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
package io.polycat.catalog.server.service.impl;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import io.polycat.catalog.common.lineage.*;
import io.polycat.catalog.common.model.LineageInfo;
import io.polycat.catalog.common.plugin.request.input.LineageInfoInput;
import io.polycat.catalog.common.utils.LineageUtils;
import io.polycat.catalog.service.api.DataLineageService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;


@Slf4j
@SpringBootTest
public class PGDataLineageServiceImplTest extends PGBaseServiceImplTest {

    @Autowired
    private DataLineageService dataLineageService;

    @Test
    public void updateDataLineage_should_success() {
        LineageInfoInput lineageInput = makeLineageInput();
        functionAssertDoesNotThrow(() -> dataLineageService.updateDataLineage(PROJECT_ID, lineageInput));
        String tableQualifiedName = LineageUtils.getTableQualifiedName("c1", "d1", "t1");
        LineageInfo tableLineageGraph = dataLineageService.getLineageGraph(PROJECT_ID, EDbType.HIVE, ELineageObjectType.TABLE,
                tableQualifiedName, 10,
                ELineageDirection.BOTH, ELineageType.TABLE_DEPEND_TABLE, null);
        log.info("{}", tableLineageGraph);
        assert tableLineageGraph != null;
        valueAssertEquals(2, tableLineageGraph.getNodes().size());
        valueAssertEquals(1, tableLineageGraph.getRelations().size());
        LineageInfo colLineageGraph = dataLineageService.getLineageGraph(PROJECT_ID, EDbType.HIVE, ELineageObjectType.COLUMN,
                LineageUtils.getColumnQualifiedName(tableQualifiedName, "a2"), 10,
                ELineageDirection.BOTH, ELineageType.FIELD_DEPEND_FIELD, null);
        assert colLineageGraph != null;
        valueAssertEquals(3, colLineageGraph.getNodes().size());
        valueAssertEquals(2, colLineageGraph.getRelations().size());
        valueAssertEquals("default.t2.a2 + default.t2.a3", colLineageGraph.getRelations().get(0).getParams().get("dcs"));
        LineageFact tableLineageJobFact = dataLineageService.getLineageJobFact(PROJECT_ID, tableLineageGraph.getRelations().get(0).getJobFactId());
        LineageFact colLineageJobFact = dataLineageService.getLineageJobFact(PROJECT_ID, colLineageGraph.getRelations().get(0).getJobFactId());
        valueAssertEquals(tableLineageJobFact.getJobId(), colLineageJobFact.getJobId());


    }

    private LineageInfoInput makeLineageInput() {
        LineageInfoInput lineageInfo = new LineageInfoInput();
        lineageInfo.setCreateTime(System.currentTimeMillis());
        lineageInfo.setJobFact(makeJobFact());
        String t1 = LineageUtils.getTableQualifiedName("c1", "d1", "t1");
        String t2 = LineageUtils.getTableQualifiedName("c1", "d2", "t2");

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
        log.info("{}", lineageInfo);
        log.info("{}", JSON.toJSONString(lineageInfo));


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
