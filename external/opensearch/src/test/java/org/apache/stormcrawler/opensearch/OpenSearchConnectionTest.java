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

package org.apache.stormcrawler.opensearch;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.generic.Requests;

class OpenSearchConnectionTest {

    @Test
    void parseSeconds() {
        assertEquals(5000, OpenSearchConnection.parseTimeValueToMillis("5s", 0));
    }

    @Test
    void parseMilliseconds() {
        assertEquals(500, OpenSearchConnection.parseTimeValueToMillis("500ms", 0));
    }

    @Test
    void parseMinutes() {
        assertEquals(120000, OpenSearchConnection.parseTimeValueToMillis("2m", 0));
    }

    @Test
    void parsePlainNumber() {
        assertEquals(42, OpenSearchConnection.parseTimeValueToMillis("42", 0));
    }

    @Test
    void nullReturnsDefault() {
        assertEquals(5000, OpenSearchConnection.parseTimeValueToMillis(null, 5000));
    }

    @Test
    void emptyReturnsDefault() {
        assertEquals(5000, OpenSearchConnection.parseTimeValueToMillis("", 5000));
    }

    @Test
    void blankReturnsDefault() {
        assertEquals(5000, OpenSearchConnection.parseTimeValueToMillis("   ", 5000));
    }

    @Test
    void invalidReturnsDefault() {
        assertEquals(3000, OpenSearchConnection.parseTimeValueToMillis("abc", 3000));
    }

    @Test
    void invalidWithSuffixReturnsDefault() {
        assertEquals(3000, OpenSearchConnection.parseTimeValueToMillis("abcs", 3000));
    }

    @Test
    void whitespaceIsTrimmed() {
        assertEquals(5000, OpenSearchConnection.parseTimeValueToMillis("  5s  ", 0));
    }

    @Test
    void disabledTlsValidationKeepsTenConnectionsPerNode() throws Exception {
        try (ServerSocket server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            // accepts the connections and never answers, so each request keeps its connection
            final List<Socket> accepted = new CopyOnWriteArrayList<>();
            final Thread acceptor =
                    new Thread(
                            () -> {
                                try {
                                    while (true) {
                                        accepted.add(server.accept());
                                    }
                                } catch (IOException e) {
                                    // server closed
                                }
                            });
            acceptor.setDaemon(true);
            acceptor.start();

            final Map<String, Object> conf = new HashMap<>();
            conf.put("opensearch.test.addresses", "http://127.0.0.1:" + server.getLocalPort());
            conf.put("opensearch.disable.tls.validation", true);
            final OpenSearchClient client = OpenSearchConnection.getClient(conf, "test");
            final ExecutorService requests = Executors.newFixedThreadPool(12);
            try {
                for (int i = 0; i < 12; i++) {
                    requests.submit(
                            () ->
                                    client.generic()
                                            .execute(
                                                    Requests.builder()
                                                            .endpoint("/")
                                                            .method("GET")
                                                            .build()));
                }
                await().during(Duration.ofMillis(300))
                        .atMost(Duration.ofSeconds(5))
                        .untilAsserted(() -> assertEquals(10, accepted.size()));
            } finally {
                for (Socket socket : accepted) {
                    socket.close();
                }
                requests.shutdownNow();
                client._transport().close();
            }
        }
    }
}
