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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.impl.bootstrap.HttpServer;
import org.apache.hc.core5.http.impl.bootstrap.ServerBootstrap;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.io.CloseMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch._types.Script;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

/**
 * Real client and HTTP transport against a local server that records the request bodies and answers
 * with the queued responses, then with an empty bulk response.
 */
class AsyncBulkProcessorHttpTest {

    private final List<byte[]> bodies = new CopyOnWriteArrayList<>();

    private final Queue<String> responses = new ConcurrentLinkedQueue<>();

    private final AsyncBulkProcessor.Listener listener = mock(AsyncBulkProcessor.Listener.class);

    private HttpServer server;

    private OpenSearchTransport transport;

    private AsyncBulkProcessor processor;

    @BeforeEach
    void start() throws IOException {
        server =
                ServerBootstrap.bootstrap()
                        .setLocalAddress(InetAddress.getByName("127.0.0.1"))
                        .setListenerPort(0)
                        .setCanonicalHostName("127.0.0.1")
                        .register(
                                "*",
                                (request, response, context) -> {
                                    bodies.add(EntityUtils.toByteArray(request.getEntity()));
                                    String next = responses.poll();
                                    response.setCode(200);
                                    response.setEntity(
                                            new StringEntity(
                                                    next != null
                                                            ? next
                                                            : "{\"took\":1,\"errors\":false,\"items\":[]}",
                                                    ContentType.APPLICATION_JSON));
                                })
                        .create();
        server.start();
        transport =
                ApacheHttpClient5TransportBuilder.builder(
                                new HttpHost("http", "127.0.0.1", server.getLocalPort()))
                        .setMapper(new JacksonJsonpMapper())
                        .build();
    }

    @AfterEach
    void stop() throws Exception {
        if (processor != null) {
            processor.awaitClose(100, TimeUnit.MILLISECONDS);
        }
        transport.close();
        server.close(CloseMode.IMMEDIATE);
    }

    private AsyncBulkProcessor processor(int bulkActions) {
        processor =
                new AsyncBulkProcessor.Builder(new OpenSearchAsyncClient(transport), listener)
                        .setBulkActions(bulkActions)
                        .setFlushIntervalMillis(60_000)
                        .build();
        return processor;
    }

    private List<String> lines(int request) {
        return new String(bodies.get(request), StandardCharsets.UTF_8).lines().toList();
    }

    private static BulkOperation index(String id) {
        return BulkOperation.of(b -> b.index(i -> i.index("idx").id(id).document(Map.of("k", id))));
    }

    /** The first response rejects the second item with 429; the retry is accepted. */
    @Test
    @Timeout(30)
    void resendsOnlyTheRejectedItem() {
        responses.add(
                "{\"took\":1,\"errors\":true,\"items\":["
                        + "{\"index\":{\"_index\":\"idx\",\"_id\":\"a\",\"status\":200}},"
                        + "{\"index\":{\"_index\":\"idx\",\"_id\":\"b\",\"status\":429,"
                        + "\"error\":{\"type\":\"rejected_execution_exception\","
                        + "\"reason\":\"rejected\"}}}]}");
        responses.add(
                "{\"took\":1,\"errors\":false,\"items\":["
                        + "{\"index\":{\"_index\":\"idx\",\"_id\":\"b\",\"status\":200}}]}");
        AsyncBulkProcessor p = processor(2);
        p.add(index("a"));
        p.add(index("b"));

        ArgumentCaptor<BulkResponse> response = ArgumentCaptor.forClass(BulkResponse.class);
        verify(listener, timeout(10_000)).afterBulk(anyLong(), any(), response.capture());
        assertEquals(2, bodies.size());
        // action and document lines of "b"
        assertEquals(lines(0).subList(2, 4), lines(1));
        assertEquals(
                List.of("a:200", "b:200"),
                response.getValue().items().stream().map(i -> i.id() + ":" + i.status()).toList());
    }

    @Test
    @Timeout(30)
    void countedSizeEqualsTheBytesTheTransportSends() {
        AsyncBulkProcessor p = processor(1);
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("list", Arrays.asList(null, "ünïcödé 日本語 😀"));
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("content", "line1\nline2\t\u0001   </script> \"q\" \\");
        doc.put("metadata", metadata);
        Script script =
                Script.of(
                        s ->
                                s.inline(
                                        in ->
                                                in.source("ctx._source.n += params.n")
                                                        .params("n", JsonData.of(5))));
        List<BulkOperation> operations =
                List.of(
                        BulkOperation.of(b -> b.index(i -> i.index("idx").id("é").document(doc))),
                        BulkOperation.of(b -> b.create(c -> c.index("idx").id("c").document(doc))),
                        BulkOperation.of(b -> b.update(u -> u.index("idx").id("v").document(doc))),
                        BulkOperation.of(
                                b ->
                                        b.update(
                                                u ->
                                                        u.index("idx")
                                                                .id("u")
                                                                .document(doc)
                                                                .upsert(doc))),
                        BulkOperation.of(
                                b ->
                                        b.update(
                                                u ->
                                                        u.index("idx")
                                                                .id("s")
                                                                .script(script)
                                                                .upsert(doc))),
                        BulkOperation.of(b -> b.delete(d -> d.index("idx").id("d"))));

        for (int i = 0; i < operations.size(); i++) {
            p.add(operations.get(i));
            final int sent = i + 1;
            await().atMost(Duration.ofSeconds(5)).until(() -> bodies.size() == sent);
            assertEquals(
                    bodies.get(i).length,
                    AsyncBulkProcessor.serializedSize(new JacksonJsonpMapper(), operations.get(i)));
        }
    }

    @Test
    @Timeout(30)
    void flushesWhenTheBufferedBytesReachTheThreshold() {
        AsyncBulkProcessor p = processor(1000);
        Map<String, Object> doc = Map.of("content", "x".repeat(1_000_000));
        for (int i = 0; i < 12; i++) {
            final String id = Integer.toString(i);
            p.add(BulkOperation.of(b -> b.index(x -> x.index("idx").id(id).document(doc))));
            // five documents of 1 MB stay below 5 MiB, the sixth crosses it
            if (i == 4) {
                assertTrue(bodies.isEmpty(), "flushed before reaching 5 MiB");
            }
            if (i == 5) {
                await().atMost(Duration.ofSeconds(10)).until(() -> bodies.size() == 1);
            }
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> bodies.size() == 2);
        for (byte[] body : bodies) {
            assertEquals(12, new String(body, StandardCharsets.UTF_8).lines().count());
            assertTrue(body.length >= AsyncBulkProcessor.BULK_SIZE_BYTES);
        }
    }
}
