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
package io.polycat.hiveService.impl;

import java.util.ArrayList;
import java.util.List;

import io.polycat.catalog.common.model.Constraint;
import io.polycat.catalog.common.model.ConstraintType;
import io.polycat.catalog.common.model.ForeignKey;
import io.polycat.catalog.common.model.PrimaryKey;
import io.polycat.catalog.common.model.TableName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
public class ConstraintServiceTest extends TestUtil {

    protected final String TABLE_NAME2 = "tb_name2";

    protected final String CHECK_NAME = "check_name";
    protected final String DEFAULT_NAME = "default_name";
    protected final String NOTNULL_NAME = "notnull_name";
    protected final String UNIQUE_NAME = "unique_name";
    protected final String FOREIGN_KEY_NAME = "foreign_key";
    protected final String PRIMARY_KEY_NAME = "primary_key";

    protected final Constraint CHECK_CONSTRAIN = new Constraint();
    protected final Constraint DEFAULT_CONSTRAIN = new Constraint();
    protected final Constraint NOTNULL_CONSTRAIN = new Constraint();
    protected final Constraint UNIQUE_CONSTRAIN = new Constraint();
    protected final ForeignKey FOREIGN_KEY = new ForeignKey();
    protected final PrimaryKey PRIMARY_KEY = new PrimaryKey();
    protected final TableName tableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME);

    {
        CHECK_CONSTRAIN.setCatName(CATALOG_NAME);
        CHECK_CONSTRAIN.setDbName(DATABASE_NAME);
        CHECK_CONSTRAIN.setTable_name(TABLE_NAME);
        CHECK_CONSTRAIN.setColumn_name(COLUMN_1);
        CHECK_CONSTRAIN.setCstr_type(ConstraintType.CHECK_CSTR);
        CHECK_CONSTRAIN.setCstr_name(CHECK_NAME);
        CHECK_CONSTRAIN.setEnable_cstr(false);
        CHECK_CONSTRAIN.setRely_cstr(false);
        CHECK_CONSTRAIN.setValidate_cstr(false);
        CHECK_CONSTRAIN.setCstr_info("CheckInfo");

        DEFAULT_CONSTRAIN.setCatName(CATALOG_NAME);
        DEFAULT_CONSTRAIN.setDbName(DATABASE_NAME);
        DEFAULT_CONSTRAIN.setTable_name(TABLE_NAME);
        DEFAULT_CONSTRAIN.setColumn_name(COLUMN_1);
        DEFAULT_CONSTRAIN.setCstr_type(ConstraintType.DEFAULT_CSTR);
        DEFAULT_CONSTRAIN.setCstr_name(DEFAULT_NAME);
        DEFAULT_CONSTRAIN.setEnable_cstr(false);
        DEFAULT_CONSTRAIN.setRely_cstr(false);
        DEFAULT_CONSTRAIN.setValidate_cstr(false);
        DEFAULT_CONSTRAIN.setCstr_info("default_value");

        NOTNULL_CONSTRAIN.setCatName(CATALOG_NAME);
        NOTNULL_CONSTRAIN.setDbName(DATABASE_NAME);
        NOTNULL_CONSTRAIN.setTable_name(TABLE_NAME);
        NOTNULL_CONSTRAIN.setColumn_name(COLUMN_1);
        NOTNULL_CONSTRAIN.setCstr_type(ConstraintType.NOT_NULL_CSTR);
        NOTNULL_CONSTRAIN.setCstr_name(NOTNULL_NAME);
        NOTNULL_CONSTRAIN.setEnable_cstr(false);
        NOTNULL_CONSTRAIN.setRely_cstr(false);
        NOTNULL_CONSTRAIN.setValidate_cstr(false);
        NOTNULL_CONSTRAIN.setCstr_info("");

        UNIQUE_CONSTRAIN.setCatName(CATALOG_NAME);
        UNIQUE_CONSTRAIN.setDbName(DATABASE_NAME);
        UNIQUE_CONSTRAIN.setTable_name(TABLE_NAME);
        UNIQUE_CONSTRAIN.setColumn_name(COLUMN_1);
        UNIQUE_CONSTRAIN.setCstr_type(ConstraintType.UNIQUE_CSTR);
        UNIQUE_CONSTRAIN.setCstr_name(UNIQUE_NAME);
        UNIQUE_CONSTRAIN.setEnable_cstr(false);
        UNIQUE_CONSTRAIN.setRely_cstr(false);
        UNIQUE_CONSTRAIN.setValidate_cstr(false);
        UNIQUE_CONSTRAIN.setCstr_info(String.valueOf(1));
        
        PRIMARY_KEY.setCatName(CATALOG_NAME);
        PRIMARY_KEY.setDbName(DATABASE_NAME);
        PRIMARY_KEY.setTableName(TABLE_NAME);
        PRIMARY_KEY.setColumnName(COLUMN_1);
        PRIMARY_KEY.setPkName(PRIMARY_KEY_NAME);
        PRIMARY_KEY.setEnable_cstr(false);
        PRIMARY_KEY.setRely_cstr(false);
        PRIMARY_KEY.setValidate_cstr(false);
        PRIMARY_KEY.setKeySeq(0);

        FOREIGN_KEY.setCatName(CATALOG_NAME);
        FOREIGN_KEY.setFkTableDb(DATABASE_NAME);
        FOREIGN_KEY.setPkTableDb(DATABASE_NAME);
        FOREIGN_KEY.setFkTableName(TABLE_NAME2);
        FOREIGN_KEY.setPkTableName(TABLE_NAME);
        FOREIGN_KEY.setPkColumnName(COLUMN_1);
        FOREIGN_KEY.setFkColumnName(COLUMN_2);
        FOREIGN_KEY.setPkName(PRIMARY_KEY_NAME);
        FOREIGN_KEY.setFkName(FOREIGN_KEY_NAME);
        FOREIGN_KEY.setEnable_cstr(false);
        FOREIGN_KEY.setRely_cstr(false);
        FOREIGN_KEY.setValidate_cstr(false);
        FOREIGN_KEY.setKeySeq(0);
        FOREIGN_KEY.setDeleteRule(0);
        FOREIGN_KEY.setUpdateRule(0);
    }
    @Test
    public void should_create_constraint_success() {
        tableService.addConstraint(PROJECT_ID, new ArrayList<Constraint>() {{
            add(CHECK_CONSTRAIN);
        }});
        tableService.addConstraint(PROJECT_ID, new ArrayList<Constraint>() {{
            add(DEFAULT_CONSTRAIN);
        }});
        tableService.addConstraint(PROJECT_ID, new ArrayList<Constraint>() {{
            add(NOTNULL_CONSTRAIN);
        }});
        tableService.addConstraint(PROJECT_ID, new ArrayList<Constraint>() {{
            add(UNIQUE_CONSTRAIN);
        }});


        List<Constraint> actual = tableService.getConstraints(tableName, ConstraintType.UNIQUE_CSTR);
        assertEquals(1, actual.size());
        assertEquals(UNIQUE_NAME, actual.get(0).getCstr_name());

        actual = tableService.getConstraints(tableName, ConstraintType.DEFAULT_CSTR);
        assertEquals(1, actual.size());
        assertEquals(DEFAULT_NAME, actual.get(0).getCstr_name());

        actual = tableService.getConstraints(tableName, ConstraintType.NOT_NULL_CSTR);
        assertEquals(1, actual.size());
        assertEquals(NOTNULL_NAME, actual.get(0).getCstr_name());

        actual = tableService.getConstraints(tableName, ConstraintType.CHECK_CSTR);
        assertEquals(1, actual.size());
        assertEquals(CHECK_NAME, actual.get(0).getCstr_name());
    }

    @Test
    public void should_drop_constraint_success() {
        tableService.addConstraint(PROJECT_ID, new ArrayList<Constraint>() {{
            add(CHECK_CONSTRAIN);
        }});

        List<Constraint> constraints = tableService.getConstraints(tableName, ConstraintType.CHECK_CSTR);
        assertEquals(1, constraints.size());

        tableService.dropConstraint(tableName, CHECK_NAME);
        constraints = tableService.getConstraints(tableName, ConstraintType.CHECK_CSTR);
        assertEquals(0, constraints.size());
    }

    @Test
    public void should_create_keys_success() {
        createForeignTable();
        tableService.addPrimaryKey(PROJECT_ID, new ArrayList<PrimaryKey>() {{
            add(PRIMARY_KEY);
        }});

        tableService.addForeignKey(PROJECT_ID, new ArrayList<ForeignKey>() {{
            add(FOREIGN_KEY);
        }});

        List<PrimaryKey> actualPriKey = tableService.getPrimaryKeys(tableName);
        assertEquals(1, actualPriKey.size());
        assertEquals(PRIMARY_KEY_NAME, actualPriKey.get(0).getPkName());

        TableName foreignTableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME2);
        List<ForeignKey> actualForKey = tableService.getForeignKeys(tableName, foreignTableName);
        assertEquals(1, actualForKey.size());
        assertEquals(FOREIGN_KEY_NAME, actualForKey.get(0).getFkName());
    }

    @Test
    public void should_delete_keys_success() {
        createForeignTable();
        tableService.addPrimaryKey(PROJECT_ID, new ArrayList<PrimaryKey>() {{
            add(PRIMARY_KEY);
        }});
        tableService.addForeignKey(PROJECT_ID, new ArrayList<ForeignKey>() {{
            add(FOREIGN_KEY);
        }});

        List<PrimaryKey> actualPriKey = tableService.getPrimaryKeys(tableName);
        assertEquals(1, actualPriKey.size());
        assertEquals(PRIMARY_KEY_NAME, actualPriKey.get(0).getPkName());

        TableName foreignTableName = new TableName(PROJECT_ID, CATALOG_NAME, DATABASE_NAME, TABLE_NAME2);
        List<ForeignKey> actualForKey = tableService.getForeignKeys(tableName, foreignTableName);
        assertEquals(1, actualForKey.size());
        assertEquals(FOREIGN_KEY_NAME, actualForKey.get(0).getFkName());

        // delete key
        tableService.dropConstraint(tableName, FOREIGN_KEY_NAME);
        tableService.dropConstraint(tableName, PRIMARY_KEY_NAME);

        actualPriKey = tableService.getPrimaryKeys(tableName);
        assertEquals(0, actualPriKey.size());

        actualForKey = tableService.getForeignKeys(tableName, foreignTableName);
        assertEquals(0, actualForKey.size());
    }

    private void createForeignTable() {
        tableService.createTable(databaseName, createTableInput(TABLE_NAME2));
    }


    @AfterEach
    private void cleanEnv() {
        deleteTestDataBaseAfterTest();
    }

    @BeforeEach
    private void makeEnv() {
        createTestTableBeforeTest();
    }
}
