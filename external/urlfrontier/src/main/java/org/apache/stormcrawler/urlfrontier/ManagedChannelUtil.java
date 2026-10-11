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

import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_DEFAULT_PORT;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_TLS_CLIENT_CERT_CHAIN_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_TLS_CLIENT_PRIVATE_KEY_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_TLS_CLIENT_PRIVATE_KEY_PASSWORD_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_TLS_ENABLED_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_TLS_TRUST_CERT_COLLECTION_KEY;

import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.stormcrawler.util.ConfUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * At some point we have to write a mechanism to share the same ManagedChannel in the same runtime
 * see: https://github.com/apache/stormcrawler/pull/982#issuecomment-1175272094
 */
final class ManagedChannelUtil {
    private ManagedChannelUtil() {}

    private static final Logger LOG = LoggerFactory.getLogger(ManagedChannelUtil.class);

    /** Gets a channel for the given host and port. */
    @NotNull
    static ManagedChannel createChannel(
            @NotNull String host,
            @Range(from = 0, to = 65535) int port,
            @NotNull Map<String, Object> conf) {
        return createChannel(host + ":" + port, conf);
    }

    /**
     * Gets a channel for the given address. The channel uses TLS when {@code
     * urlfrontier.tls.enabled} is true and plaintext otherwise.
     */
    @NotNull
    static ManagedChannel createChannel(
            @NotNull String address, @NotNull Map<String, Object> conf) {
        // add the default port if missing
        if (!address.contains(":")) {
            address += ":" + URLFRONTIER_DEFAULT_PORT;
        }
        ChannelCredentials credentials = createCredentials(conf);
        if (credentials instanceof TlsChannelCredentials) {
            LOG.info("Initialisation of TLS connection to URLFrontier service on {}", address);
        } else {
            LOG.info(
                    "Initialisation of plaintext connection to URLFrontier service on {}; set {}"
                            + " to encrypt it",
                    address,
                    URLFRONTIER_TLS_ENABLED_KEY);
        }
        return Grpc.newChannelBuilder(address, credentials).build();
    }

    /**
     * Builds the channel credentials from the configuration: plaintext unless {@code
     * urlfrontier.tls.enabled} is true. With TLS the server certificate is checked against the
     * certificates in {@code urlfrontier.tls.trust.cert.collection}, or against the JVM trust store
     * if that key is not set. A client certificate for mutual TLS is sent when both {@code
     * urlfrontier.tls.client.cert.chain} and {@code urlfrontier.tls.client.private.key} are set.
     *
     * @throws IllegalArgumentException if only one of the client certificate chain and private key
     *     is set, or if one of the configured files cannot be read
     */
    @NotNull
    static ChannelCredentials createCredentials(@NotNull Map<String, Object> conf) {
        if (!ConfUtils.getBoolean(conf, URLFRONTIER_TLS_ENABLED_KEY, false)) {
            return InsecureChannelCredentials.create();
        }

        TlsChannelCredentials.Builder builder = TlsChannelCredentials.newBuilder();

        String trustCerts = ConfUtils.getString(conf, URLFRONTIER_TLS_TRUST_CERT_COLLECTION_KEY);
        if (StringUtils.isNotBlank(trustCerts)) {
            try {
                builder.trustManager(new File(trustCerts));
            } catch (IOException | RuntimeException e) {
                throw new IllegalArgumentException(
                        "Cannot read the certificates in "
                                + URLFRONTIER_TLS_TRUST_CERT_COLLECTION_KEY
                                + ": "
                                + trustCerts,
                        e);
            }
        }

        String certChain = ConfUtils.getString(conf, URLFRONTIER_TLS_CLIENT_CERT_CHAIN_KEY);
        String privateKey = ConfUtils.getString(conf, URLFRONTIER_TLS_CLIENT_PRIVATE_KEY_KEY);
        boolean hasCertChain = StringUtils.isNotBlank(certChain);
        boolean hasPrivateKey = StringUtils.isNotBlank(privateKey);
        if (hasCertChain != hasPrivateKey) {
            throw new IllegalArgumentException(
                    URLFRONTIER_TLS_CLIENT_CERT_CHAIN_KEY
                            + " and "
                            + URLFRONTIER_TLS_CLIENT_PRIVATE_KEY_KEY
                            + " must be set together for mutual TLS, only "
                            + (hasCertChain
                                    ? URLFRONTIER_TLS_CLIENT_CERT_CHAIN_KEY
                                    : URLFRONTIER_TLS_CLIENT_PRIVATE_KEY_KEY)
                            + " is set");
        }
        if (hasCertChain) {
            String password =
                    ConfUtils.getString(conf, URLFRONTIER_TLS_CLIENT_PRIVATE_KEY_PASSWORD_KEY);
            try {
                builder.keyManager(
                        new File(certChain),
                        new File(privateKey),
                        StringUtils.isEmpty(password) ? null : password);
            } catch (IOException | RuntimeException e) {
                throw new IllegalArgumentException(
                        "Cannot read the client certificate chain "
                                + certChain
                                + " or the private key "
                                + privateKey,
                        e);
            }
        }

        return builder.build();
    }
}
