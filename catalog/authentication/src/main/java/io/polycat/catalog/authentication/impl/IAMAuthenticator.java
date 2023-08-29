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
package io.polycat.catalog.authentication.impl;

import io.polycat.catalog.authentication.api.Authenticator;
import io.polycat.catalog.authentication.model.AuthenticationResult;
import io.polycat.catalog.authentication.model.Identity;
import io.polycat.catalog.authentication.model.TokenParseResult;

import java.io.IOException;


/*
IAM authenticator, which uses IAM for unified authentication.
 */
public class IAMAuthenticator implements Authenticator {

    //创建 LocalAuthenticator 的一个对象
    private static IAMAuthenticator instance = new IAMAuthenticator();
    //获取唯一可用的对象
    public static IAMAuthenticator getInstance() {
        return instance;
    }

    @Override
    public AuthenticationResult authAndCreateToken(Identity identity) {
        return null;
    }

    @Override
    public String  getShortName() { return "IAMAuthenticator"; };

    @Override
    public TokenParseResult CheckAndParseToken(String token) throws IOException, ClassNotFoundException{
        return null;
    }

}