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
package io.polycat.catalog.spark;

import java.util.Collections;
import java.util.List;

import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.DelegateStorageProvider;
import io.polycat.catalog.common.model.record.Record;
import io.polycat.common.RowBatch;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.polycat.catalog.spark.SparkCatalogTestEnv.checkAnswer;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.getUser;
import static io.polycat.catalog.spark.SparkCatalogTestEnv.sql;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({SparkCatalogTestEnv.class})
public class DelegateTest {

    @Test
    public void basicDelegateTest() {
        String delegateName = "my_delegate";
        String delegateNamePrefix = "my_";
        String domainName = "her_domain";
        String agencyName = "her_agency";
        String allowedLocations = "[obs://bucket/path1, obs://bucket/path2]";
        String blockedLocations = "[]";

        // case: create delegate
        sql("CREATE DELEGATE " + delegateName
            + " STORAGE_PROVIDER = " + DelegateStorageProvider.OBS.name()
            + " PROVIDER_DOMAIN_NAME = " + domainName
            + " AGENCY_NAME = " + agencyName
            + " ALLOWED_LOCATIONS = ('obs://bucket/path1','obs://bucket/path2')");

        // case: desc delegate
        List<Row> delegate = sql("DESC DELEGATE my_delegate").collectAsList();
        assertEquals(6, delegate.size());
        assertEquals(delegate.get(0).getString(1), delegateName);
        assertEquals(delegate.get(1).getString(1), DelegateStorageProvider.OBS.name());
        assertEquals(delegate.get(2).getString(1), domainName);
        assertEquals(delegate.get(3).getString(1), agencyName);
        assertEquals(delegate.get(4).getString(1), allowedLocations);
        assertEquals(delegate.get(5).getString(1), blockedLocations);

        // case: show all delegates
        RowBatch expectedListRecords = new RowBatch(Collections.singletonList(
            new Record(delegateName, DelegateStorageProvider.OBS.name(), getUser())
        ));
        checkAnswer(sql("SHOW DELEGATES"), expectedListRecords);

        // case: show delegate with prefix
        checkAnswer(sql("SHOW DELEGATES LIKE '" + delegateNamePrefix + "'"), expectedListRecords);

        // case: drop delegate
        sql("DROP DELEGATE my_delegate");
        CatalogException e = assertThrows(CatalogException.class, () ->
            sql("DESC DELEGATE my_delegate"));
    }
}
