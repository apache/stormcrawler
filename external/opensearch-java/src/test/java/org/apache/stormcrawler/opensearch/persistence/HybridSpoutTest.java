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

package org.apache.stormcrawler.opensearch.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hc.core5.http.HttpHost;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.opensearch.bolt.AbstractOpenSearchTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

class HybridSpoutTest extends AbstractOpenSearchTest {

    private static final int NUM_URLS = 3;

    private HybridSpout spout;

    private TestOutputCollector output;

    private OpenSearchTransport transport;

    @BeforeEach
    void setupSpout() {
        transport =
                ApacheHttpClient5TransportBuilder.builder(
                                new HttpHost(
                                        "http",
                                        opensearchContainer.getHost(),
                                        opensearchContainer.getMappedPort(9200)))
                        .setMapper(new JacksonJsonpMapper())
                        .build();

        Map<String, Object> conf = new HashMap<>();
        conf.put(
                "opensearch.status.addresses",
                opensearchContainer.getHost() + ":" + opensearchContainer.getFirstMappedPort());
        conf.put(
                "urlbuffer.class", "org.apache.stormcrawler.persistence.urlbuffer.SimpleURLBuffer");
        conf.put("opensearch.status.bucket.field", "key");
        // the aggregation returns the first URL of the queue, each reload of the queue the next
        // one: the 2nd URL is queried after the sort values of the aggregation, the 3rd after
        // those of the previous reload
        conf.put("opensearch.status.max.urls.per.bucket", 1);
        conf.put("opensearch.status.max.urls.per.reload", 1);
        conf.put("opensearch.status.bucket.sort.field", List.of("nextFetchDate", "url"));
        // a single aggregation query for the whole test
        conf.put("spout.min.delay.queries", 600_000);
        conf.put("spout.max.delay.queries", 0);

        spout = new HybridSpout();
        output = new TestOutputCollector();
        spout.open(conf, TestUtil.getMockedTopologyContext(), new SpoutOutputCollector(output));
    }

    @AfterEach
    void closeSpout() throws IOException {
        spout.close();
        transport.close();
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void reloadsQueueAfterLastEmittedURL() throws Exception {
        OpenSearchClient client = new OpenSearchClient(transport);
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < NUM_URLS; i++) {
            String url = "https://example.com/" + i;
            expected.add(url);
            Map<String, Object> doc = new HashMap<>();
            doc.put("url", url);
            doc.put("key", "example.com");
            doc.put("status", "DISCOVERED");
            doc.put("nextFetchDate", "2020-01-0" + (i + 1) + "T00:00:00Z");
            client.index(r -> r.index("status").id(url).document(doc).refresh(Refresh.True));
        }

        spout.activate();

        long deadline = System.currentTimeMillis() + 20_000;
        while (output.getEmitted().size() < NUM_URLS && System.currentTimeMillis() < deadline) {
            // the spout sends its queries and handles their responses on a single thread: once
            // this no-op task has run, the responses to the queries sent so far are fully handled,
            // so no URL is emitted while a response is still being added to the buffer
            spout.queryExecutor.submit(() -> {}).get(10, TimeUnit.SECONDS);
            spout.nextTuple();
        }

        List<String> emitted = new ArrayList<>();
        output.getEmitted().forEach(t -> emitted.add((String) t.get(0)));
        assertEquals(expected, emitted);
    }
}
