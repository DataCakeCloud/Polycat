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
package io.polycat.catalog.common.utils;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;

public class CodecUtilTest {

    @Test
    void should_hex_to_bytes_to_hex_be_same() {
        Arrays.asList(
                "000000000000000000000000",
                "05703366c6b9a4e61b8ad58f",
                "66c6b9a4e61b8ad58f403133",
                "66c6b9a4e61b8ad58f403178",
                "ffffffffffffffffffffffff"
                ).forEach(x -> {
            byte[] bytes = CodecUtil.hex2Bytes(x);
            String hexString = CodecUtil.bytes2Hex(bytes);
            assertThat("should be same", x.equals(hexString));
        });
    }
}
