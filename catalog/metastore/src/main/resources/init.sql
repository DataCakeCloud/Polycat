--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

;    id SERIAL PRIMARY KEY,

CREATE TABLE IF NOT EXISTS delegate (
    delegate_name CHAR(64) PRIMARY KEY,
    user_id   CHAR(64),
    storage_provider  CHAR(64),
    provider_domain_name CHAR(128),
    agency_name   CHAR(64),
    storage_allowed_locations CHAR(128),
    storage_blocked_locations CHAR(128)
);