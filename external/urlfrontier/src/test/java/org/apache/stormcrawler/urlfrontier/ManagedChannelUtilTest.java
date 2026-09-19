/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.urlfrontier;

import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_TLS_CLIENT_CERT_CHAIN_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_TLS_CLIENT_PRIVATE_KEY_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_TLS_ENABLED_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_TLS_TRUST_CERT_COLLECTION_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import crawlercommons.urlfrontier.URLFrontierGrpc;
import crawlercommons.urlfrontier.Urlfrontier.QueueWithinCrawlParams;
import crawlercommons.urlfrontier.Urlfrontier.Stats;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import io.grpc.StatusRuntimeException;
import io.grpc.TlsChannelCredentials;
import io.grpc.TlsServerCredentials;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ManagedChannelUtilTest {

    private static SelfSignedCertificate serverCert;
    private static SelfSignedCertificate clientCert;

    private Server server;
    private ManagedChannel channel;

    @BeforeAll
    static void createCertificates() throws Exception {
        serverCert = new SelfSignedCertificate("localhost");
        clientCert = new SelfSignedCertificate("client");
    }

    @AfterAll
    static void deleteCertificates() {
        serverCert.delete();
        clientCert.delete();
    }

    @AfterEach
    void shutdown() throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void plaintextByDefault() {
        assertInstanceOf(
                InsecureChannelCredentials.class,
                ManagedChannelUtil.createCredentials(new HashMap<>()));

        // the other keys are ignored while TLS is disabled
        Map<String, Object> conf = new HashMap<>();
        conf.put(URLFRONTIER_TLS_ENABLED_KEY, false);
        conf.put(URLFRONTIER_TLS_CLIENT_CERT_CHAIN_KEY, clientCert.certificate().getPath());
        assertInstanceOf(
                InsecureChannelCredentials.class, ManagedChannelUtil.createCredentials(conf));
    }

    @Test
    void tlsWithSystemTrustStore() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(URLFRONTIER_TLS_ENABLED_KEY, true);
        TlsChannelCredentials tls =
                assertInstanceOf(
                        TlsChannelCredentials.class, ManagedChannelUtil.createCredentials(conf));
        assertNull(tls.getRootCertificates());
        assertNull(tls.getCertificateChain());
        assertNull(tls.getPrivateKey());
    }

    @Test
    void tlsWithTrustCollectionAndClientCertificate() {
        TlsChannelCredentials tls =
                assertInstanceOf(
                        TlsChannelCredentials.class,
                        ManagedChannelUtil.createCredentials(mutualTlsConf()));
        assertNotNull(tls.getRootCertificates());
        assertNotNull(tls.getCertificateChain());
        assertNotNull(tls.getPrivateKey());
    }

    @Test
    void clientCertificateWithoutPrivateKeyIsRejected() {
        Map<String, Object> conf = mutualTlsConf();
        conf.remove(URLFRONTIER_TLS_CLIENT_PRIVATE_KEY_KEY);
        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> ManagedChannelUtil.createCredentials(conf));
        assertTrue(e.getMessage().contains(URLFRONTIER_TLS_CLIENT_PRIVATE_KEY_KEY));
    }

    @Test
    void privateKeyWithoutClientCertificateIsRejected() {
        Map<String, Object> conf = mutualTlsConf();
        conf.remove(URLFRONTIER_TLS_CLIENT_CERT_CHAIN_KEY);
        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> ManagedChannelUtil.createCredentials(conf));
        assertTrue(e.getMessage().contains(URLFRONTIER_TLS_CLIENT_CERT_CHAIN_KEY));
    }

    @Test
    void unreadableTrustCollectionIsRejected() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(URLFRONTIER_TLS_ENABLED_KEY, true);
        conf.put(URLFRONTIER_TLS_TRUST_CERT_COLLECTION_KEY, "/does/not/exist.pem");
        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> ManagedChannelUtil.createCredentials(conf));
        assertTrue(e.getMessage().contains(URLFRONTIER_TLS_TRUST_CERT_COLLECTION_KEY));
    }

    @Test
    void tlsRoundTrip() throws Exception {
        server = startServer(serverCredentials());

        Map<String, Object> conf = new HashMap<>();
        conf.put(URLFRONTIER_TLS_ENABLED_KEY, true);
        conf.put(URLFRONTIER_TLS_TRUST_CERT_COLLECTION_KEY, serverCert.certificate().getPath());
        channel = ManagedChannelUtil.createChannel("localhost", server.getPort(), conf);

        assertEquals(42, getStats(channel).getSize());
    }

    @Test
    void untrustedServerCertificateFails() throws Exception {
        server = startServer(serverCredentials());

        // trusts a certificate which did not sign the one of the server
        Map<String, Object> conf = new HashMap<>();
        conf.put(URLFRONTIER_TLS_ENABLED_KEY, true);
        conf.put(URLFRONTIER_TLS_TRUST_CERT_COLLECTION_KEY, clientCert.certificate().getPath());
        channel = ManagedChannelUtil.createChannel("localhost", server.getPort(), conf);

        assertThrows(StatusRuntimeException.class, () -> getStats(channel));
    }

    @Test
    void plaintextClientCannotTalkToTlsServer() throws Exception {
        server = startServer(serverCredentials());

        channel = ManagedChannelUtil.createChannel("localhost", server.getPort(), new HashMap<>());

        assertThrows(StatusRuntimeException.class, () -> getStats(channel));
    }

    @Test
    void mutualTlsRoundTrip() throws Exception {
        server = startServer(mutualTlsServerCredentials());

        channel = ManagedChannelUtil.createChannel("localhost", server.getPort(), mutualTlsConf());

        assertEquals(42, getStats(channel).getSize());
    }

    @Test
    void mutualTlsWithoutClientCertificateFails() throws Exception {
        server = startServer(mutualTlsServerCredentials());

        Map<String, Object> conf = mutualTlsConf();
        conf.remove(URLFRONTIER_TLS_CLIENT_CERT_CHAIN_KEY);
        conf.remove(URLFRONTIER_TLS_CLIENT_PRIVATE_KEY_KEY);
        channel = ManagedChannelUtil.createChannel("localhost", server.getPort(), conf);

        assertThrows(StatusRuntimeException.class, () -> getStats(channel));
    }

    private static Map<String, Object> mutualTlsConf() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(URLFRONTIER_TLS_ENABLED_KEY, true);
        conf.put(URLFRONTIER_TLS_TRUST_CERT_COLLECTION_KEY, serverCert.certificate().getPath());
        conf.put(URLFRONTIER_TLS_CLIENT_CERT_CHAIN_KEY, clientCert.certificate().getPath());
        conf.put(URLFRONTIER_TLS_CLIENT_PRIVATE_KEY_KEY, clientCert.privateKey().getPath());
        return conf;
    }

    private static ServerCredentials serverCredentials() throws Exception {
        return TlsServerCredentials.create(serverCert.certificate(), serverCert.privateKey());
    }

    private static ServerCredentials mutualTlsServerCredentials() throws Exception {
        return TlsServerCredentials.newBuilder()
                .keyManager(serverCert.certificate(), serverCert.privateKey())
                .trustManager(clientCert.certificate())
                .clientAuth(TlsServerCredentials.ClientAuth.REQUIRE)
                .build();
    }

    private static Server startServer(ServerCredentials credentials) throws Exception {
        return Grpc.newServerBuilderForPort(0, credentials)
                .addService(
                        new URLFrontierGrpc.URLFrontierImplBase() {
                            @Override
                            public void getStats(
                                    QueueWithinCrawlParams request,
                                    StreamObserver<Stats> responseObserver) {
                                responseObserver.onNext(Stats.newBuilder().setSize(42).build());
                                responseObserver.onCompleted();
                            }
                        })
                .build()
                .start();
    }

    private static Stats getStats(ManagedChannel channel) {
        return URLFrontierGrpc.newBlockingStub(channel)
                .withDeadlineAfter(10, TimeUnit.SECONDS)
                .getStats(QueueWithinCrawlParams.getDefaultInstance());
    }
}
