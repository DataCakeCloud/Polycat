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
package io.prestosql.plugin.hive;

import com.google.common.net.HostAndPort;
import io.airlift.security.pem.PemReader;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.authentication.HiveMetastoreAuthentication;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreClientFactory;
import io.prestosql.spi.NodeManager;
import org.apache.thrift.transport.TTransportException;

import javax.inject.Inject;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.lang.Math.toIntExact;
import static java.util.Collections.list;
import static java.util.Objects.requireNonNull;

import io.polycat.hivesdk.hive3.impl.PolyCatMetaStoreClient;

public class PolyCatThriftMetastoreClientFactory
    extends ThriftMetastoreClientFactory {

    private final Optional<SSLContext> sslContext;
    private final Optional<HostAndPort> socksProxy;
    private final int timeoutMillis;
    private final HiveMetastoreAuthentication metastoreAuthentication;
    private final String hostname;

    public PolyCatThriftMetastoreClientFactory(
        Optional<SSLContext> sslContext,
        Optional<HostAndPort> socksProxy,
        Duration timeout,
        HiveMetastoreAuthentication metastoreAuthentication,
        String hostname) {
        super(sslContext, socksProxy, timeout, metastoreAuthentication, hostname);
        this.sslContext = requireNonNull(sslContext, "sslContext is null");
        this.socksProxy = requireNonNull(socksProxy, "socksProxy is null");
        this.timeoutMillis = toIntExact(timeout.toMillis());
        this.metastoreAuthentication = requireNonNull(metastoreAuthentication, "metastoreAuthentication is null");
        this.hostname = requireNonNull(hostname, "hostname is null");
    }

    @Inject
    public PolyCatThriftMetastoreClientFactory(
        ThriftMetastoreConfig config,
        HiveMetastoreAuthentication metastoreAuthentication,
        NodeManager nodeManager) {
        this(
            buildSslContext(
                config.isTlsEnabled(),
                Optional.ofNullable(config.getKeystorePath()),
                Optional.ofNullable(config.getKeystorePassword()),
                config.getTruststorePath()),
            Optional.ofNullable(config.getSocksProxy()),
            config.getMetastoreTimeout(),
            metastoreAuthentication,
            nodeManager.getCurrentNode().getHost());
    }

    //修改成polycat连接的client
    public PolyCatMetaStoreClient create()
        throws TTransportException {
        return new PolyCatMetaStoreClient(null, null, false);
    }

    private static Optional<SSLContext> buildSslContext(
        boolean tlsEnabled,
        Optional<File> keyStorePath,
        Optional<String> keyStorePassword,
        File trustStorePath) {
        if (!tlsEnabled) {
            return Optional.empty();
        }

        try {
            // load KeyStore if configured and get KeyManagers
            KeyManager[] keyManagers = null;
            if (keyStorePath.isPresent()) {
                KeyStore keyStore = PemReader.loadKeyStore(keyStorePath.get(), keyStorePath.get(), keyStorePassword);
                validateCertificates(keyStore);
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
                    KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, new char[0]);
                keyManagers = keyManagerFactory.getKeyManagers();
            }

            // load TrustStore
            KeyStore trustStore = loadTrustStore(trustStorePath);

            // create TrustManagerFactory
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            // get X509TrustManager
            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
                throw new RuntimeException("Unexpected default trust managers:" + Arrays.toString(trustManagers));
            }

            // create SSLContext
            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(keyManagers, trustManagers, null);
            return Optional.of(sslContext);
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static KeyStore loadTrustStore(File trustStorePath)
        throws IOException, GeneralSecurityException {
        List<X509Certificate> certificateChain = PemReader.readCertificateChain(trustStorePath);

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        for (X509Certificate certificate : certificateChain) {
            X500Principal principal = certificate.getSubjectX500Principal();
            trustStore.setCertificateEntry(principal.getName(), certificate);
        }
        return trustStore;
    }

    private static void validateCertificates(KeyStore keyStore)
        throws GeneralSecurityException {
        for (String alias : list(keyStore.aliases())) {
            if (!keyStore.isKeyEntry(alias)) {
                continue;
            }
            Certificate certificate = keyStore.getCertificate(alias);
            if (!(certificate instanceof X509Certificate)) {
                continue;
            }

            try {
                ((X509Certificate) certificate).checkValidity();
            } catch (CertificateExpiredException e) {
                throw new CertificateExpiredException("KeyStore certificate is expired: " + e.getMessage());
            } catch (CertificateNotYetValidException e) {
                throw new CertificateNotYetValidException("KeyStore certificate is not yet valid: " + e.getMessage());
            }
        }
    }
}