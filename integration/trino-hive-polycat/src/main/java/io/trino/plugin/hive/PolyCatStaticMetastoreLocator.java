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
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.trino.plugin.hive.metastore.thrift.*;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreAuthenticationConfig.ThriftMetastoreAuthenticationType;
import org.apache.thrift.TException;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.metastore.thrift.StaticMetastoreConfig.HIVE_METASTORE_USERNAME;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class PolyCatStaticMetastoreLocator
    extends StaticMetastoreLocator {

    private final List<Backoff> backoffs;
    private final PolyCatThriftMetastoreClientFactory clientFactory;
    private final String metastoreUsername;

    @Inject
    public PolyCatStaticMetastoreLocator(StaticMetastoreConfig config,
        ThriftMetastoreAuthenticationConfig authenticationConfig, ThriftMetastoreClientFactory clientFactory) {
        this(config.getMetastoreUris(), config.getMetastoreUsername(), clientFactory, Ticker.systemTicker());
        checkArgument(
            isNullOrEmpty(metastoreUsername)
                || authenticationConfig.getAuthenticationType() == ThriftMetastoreAuthenticationType.NONE,
            "%s cannot be used together with %s authentication",
            HIVE_METASTORE_USERNAME,
            authenticationConfig.getAuthenticationType());
    }

    @VisibleForTesting
    PolyCatStaticMetastoreLocator(StaticMetastoreConfig config,
        ThriftMetastoreAuthenticationConfig authenticationConfig, ThriftMetastoreClientFactory clientFactory,
        Ticker ticker) {
        this(config.getMetastoreUris(), config.getMetastoreUsername(), clientFactory, ticker);

        checkArgument(
            isNullOrEmpty(metastoreUsername)
                || authenticationConfig.getAuthenticationType() == ThriftMetastoreAuthenticationType.NONE,
            "%s cannot be used together with %s authentication",
            HIVE_METASTORE_USERNAME,
            authenticationConfig.getAuthenticationType());
    }

    public PolyCatStaticMetastoreLocator(List<URI> metastoreUris, @Nullable String metastoreUsername,
        ThriftMetastoreClientFactory clientFactory) {
        this(metastoreUris, metastoreUsername, clientFactory, Ticker.systemTicker());
    }

    public PolyCatStaticMetastoreLocator(List<URI> metastoreUris, @Nullable String metastoreUsername,
        ThriftMetastoreClientFactory clientFactory, Ticker ticker) {
        super(metastoreUris, metastoreUsername, clientFactory);
        requireNonNull(metastoreUris, "metastoreUris is null");
        checkArgument(!metastoreUris.isEmpty(), "metastoreUris must specify at least one URI");
        this.backoffs = metastoreUris.stream()
            .map(PolyCatStaticMetastoreLocator::checkMetastoreUri)
            .map(uri -> HostAndPort.fromParts(uri.getHost(), uri.getPort()))
            .map(address -> new PolyCatStaticMetastoreLocator.Backoff(address, ticker))
            .collect(toImmutableList());
        this.metastoreUsername = metastoreUsername;
        this.clientFactory = new PolyCatThriftMetastoreClientFactory();//requireNonNull(clientFactory, "clientFactory is null");
    }

    /**
     * Create a metastore client connected to the Hive metastore.
     * <p>
     * As per Hive HA metastore behavior, return the first metastore in the list list of available metastores (i.e. the
     * default metastore) if a connection can be made, else try another of the metastores at random, until either a
     * connection succeeds or there are no more fallback metastores.
     */
    @Override
    public ThriftMetastoreClient createMetastoreClient(Optional<String> delegationToken)
        throws TException {
        Comparator<PolyCatStaticMetastoreLocator.Backoff> comparator = Comparator.comparingLong(
                PolyCatStaticMetastoreLocator.Backoff::getBackoffDuration)
            .thenComparingLong(PolyCatStaticMetastoreLocator.Backoff::getLastFailureTimestamp);
        List<PolyCatStaticMetastoreLocator.Backoff> backoffsSorted = backoffs.stream()
            .sorted(comparator)
            .collect(toImmutableList());

        TException lastException = null;
        for (PolyCatStaticMetastoreLocator.Backoff backoff : backoffsSorted) {
            try {
                return getClient(backoff.getAddress(), backoff, delegationToken);
            } catch (TException e) {
                lastException = e;
            }
        }

        List<HostAndPort> addresses = backoffsSorted.stream().map(PolyCatStaticMetastoreLocator.Backoff::getAddress)
            .collect(toImmutableList());
        throw new TException("Failed connecting to Hive metastore: " + addresses, lastException);
    }

    private ThriftMetastoreClient getClient(HostAndPort address, PolyCatStaticMetastoreLocator.Backoff backoff,
        Optional<String> delegationToken)
        throws TException {
        //PolyCatFailureAwareThriftMetastoreClient 替换成自定义的client
        ThriftMetastoreClient client = new PolyCatFailureAwareThriftMetastoreClient(clientFactory.create(),
            new PolyCatFailureAwareThriftMetastoreClient.Callback() {
                @Override
                public void success() {
                    backoff.success();
                }

                @Override
                public void failed(TException e) {
                    backoff.fail();
                }
            });
        if (!isNullOrEmpty(metastoreUsername)) {
            client.setUGI(metastoreUsername);
        }
        return client;
    }

    private static URI checkMetastoreUri(URI uri) {
        requireNonNull(uri, "metastoreUri is null");
        String scheme = uri.getScheme();
        checkArgument(!isNullOrEmpty(scheme), "metastoreUri scheme is missing: %s", uri);
        checkArgument(scheme.equals("thrift"), "metastoreUri scheme must be thrift: %s", uri);
        checkArgument(uri.getHost() != null, "metastoreUri host is missing: %s", uri);
        checkArgument(uri.getPort() != -1, "metastoreUri port is missing: %s", uri);
        return uri;
    }

    @VisibleForTesting
    static class Backoff {

        static final long MIN_BACKOFF = new Duration(50, MILLISECONDS).roundTo(NANOSECONDS);
        static final long MAX_BACKOFF = new Duration(60, SECONDS).roundTo(NANOSECONDS);

        private final HostAndPort address;
        private final Ticker ticker;
        private long backoffDuration = MIN_BACKOFF;
        private OptionalLong lastFailureTimestamp = OptionalLong.empty();

        Backoff(HostAndPort address, Ticker ticker) {
            this.address = requireNonNull(address, "address is null");
            this.ticker = requireNonNull(ticker, "ticker is null");
        }

        public HostAndPort getAddress() {
            return address;
        }

        synchronized void fail() {
            lastFailureTimestamp = OptionalLong.of(ticker.read());
            backoffDuration = min(backoffDuration * 2, MAX_BACKOFF);
        }

        synchronized void success() {
            lastFailureTimestamp = OptionalLong.empty();
            backoffDuration = MIN_BACKOFF;
        }

        synchronized long getLastFailureTimestamp() {
            return lastFailureTimestamp.orElse(Long.MIN_VALUE);
        }

        synchronized long getBackoffDuration() {
            if (lastFailureTimestamp.isEmpty()) {
                return 0;
            }
            long timeSinceLastFail = ticker.read() - lastFailureTimestamp.getAsLong();
            return max(backoffDuration - timeSinceLastFail, 0);
        }
    }
}
