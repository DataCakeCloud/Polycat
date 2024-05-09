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
package io.polycat.catalog.client.authorization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;

/**
 * @author liangyouze
 * @date 2024/2/28
 */
public class BasicCredentials implements Credentials, Serializable {

    private String projectId;
    private String userName;
    private String password;

    private String token;


    @Override
    public String getUserName() {
        return this.userName;
    }

    @Override
    public String getPassword() {
        return this.password;
    }

    @Override
    public String getToken() {
        if (this.token == null) {
            this.token = createUserTokenByPassword();
        }
        return this.token;
    }

    public String createUserTokenByPassword() throws RuntimeException {

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(256);
        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(this);
            objectOutputStream.flush();
            objectOutputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String base64encodedString = Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());

        return base64encodedString;
    }

    @Override
    public String getProjectId() {
        return this.projectId;
    }

    public BasicCredentials(String projectId, String userName, String password) {
        this.projectId = projectId;
        this.userName = userName;
        this.password = password;
    }
}
