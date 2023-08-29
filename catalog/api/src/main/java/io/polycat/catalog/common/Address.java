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
package io.polycat.catalog.common;

import io.polycat.catalog.common.serialization.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class Address implements Writable, Serializable {
    private static final long serialVersionUID = -4520044179799025411L;

    private static Set<Integer> portSet = new HashSet<>();

    // 避免随机生成address时有重复的port
    private static synchronized Address createRandomAddress() throws UnknownHostException {
        String hostname = InetAddress.getLocalHost().getHostName();
        int port;
        do {
            port = new SecureRandom().nextInt(2000) + 10000;
        } while (portSet.contains(port));
        portSet.add(port);
        return new Address(hostname, port);
    }

    private String hostname;

    private int port;

    public Address() {
    }

    public Address(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    public Address(int port) throws UnknownHostException {
        this(InetAddress.getLoopbackAddress().getHostAddress(), port);
    }

    public static Address randomAddress() throws UnknownHostException {
        return new Address(InetAddress.getLocalHost().getHostName(), createRandomAddress().getPort());
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return hostname + ':' + port;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(hostname);
        out.writeInt(port);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        hostname = in.readUTF();
        port = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Address address = (Address) o;
        return port == address.port && Objects.equals(hostname, address.hostname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostname, port);
    }
}
