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

package org.apache.stormcrawler.bolt;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.util.URLPartitioner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FetcherBoltTest extends AbstractFetcherBoltTest {

    @BeforeEach
    void setUpContext() throws Exception {
        bolt = new FetcherBolt();
    }

    // --- a rate-limited server emits a host back-off signal on the host-info stream ---

    @Test
    void retryAfterEmitsHostInfo(WireMockRuntimeInfo wmRuntimeInfo) {
        stubFor(
                get(urlMatching(".+"))
                        .willReturn(aResponse().withStatus(429).withHeader("Retry-After", "120")));

        TestOutputCollector output = runFetch(wmRuntimeInfo, new HashMap<>());

        List<List<Object>> hostInfo = output.getEmitted(Constants.HOST_INFO_STREAM_NAME);
        assertEquals(1, hostInfo.size());
        // key is the lower-cased host of the fetched URL
        assertEquals("localhost", hostInfo.get(0).get(0));
        long blockUntil = (Long) hostInfo.get(0).get(1);
        long nowSecs = System.currentTimeMillis() / 1000L;
        // ~120s in the future, with a few seconds of slack
        assertTrue(
                blockUntil >= nowSecs + 110 && blockUntil <= nowSecs + 125,
                "blockUntil was " + blockUntil + " now " + nowSecs);
    }

    @Test
    void retryAfterKeyUsesFrontierPartitionMode(WireMockRuntimeInfo wmRuntimeInfo) {
        stubFor(
                get(urlMatching(".+"))
                        .willReturn(aResponse().withStatus(429).withHeader("Retry-After", "120")));
        // the frontier partitions its queues by partition.url.mode; the emitted
        // host-info key must match that queue key so blockQueueUntil hits the
        // right queue - not the fetcher's own queue mode (fetcher.queue.mode,
        // byHost by default). Set the two to different modes to prove it.
        Map<String, Object> extra = new HashMap<>();
        extra.put(Constants.PARTITION_MODEParamName, "byIP");

        TestOutputCollector output = runFetch(wmRuntimeInfo, extra);

        List<List<Object>> hostInfo = output.getEmitted(Constants.HOST_INFO_STREAM_NAME);
        assertEquals(1, hostInfo.size());
        String url = "http://localhost:" + wmRuntimeInfo.getHttpPort() + "/";
        // expected key is what the frontier used to create the queue: the
        // partition key under partition.url.mode (here the resolved IP)
        String expectedKey = URLPartitioner.getPartition(url, new Metadata(), "byIP");
        assertEquals(expectedKey, hostInfo.get(0).get(0));
    }

    @Test
    void retryAfterCappedByConfig(WireMockRuntimeInfo wmRuntimeInfo) {
        stubFor(
                get(urlMatching(".+"))
                        .willReturn(aResponse().withStatus(429).withHeader("Retry-After", "3600")));
        Map<String, Object> extra = new HashMap<>();
        extra.put("fetcher.max.retry.after", 60L);

        TestOutputCollector output = runFetch(wmRuntimeInfo, extra);

        List<List<Object>> hostInfo = output.getEmitted(Constants.HOST_INFO_STREAM_NAME);
        assertEquals(1, hostInfo.size());
        long blockUntil = (Long) hostInfo.get(0).get(1);
        long nowSecs = System.currentTimeMillis() / 1000L;
        // capped to 60s, not 3600
        assertTrue(blockUntil <= nowSecs + 65, "blockUntil was " + blockUntil);
    }

    @Test
    void noHostInfoWhenHeaderAbsent(WireMockRuntimeInfo wmRuntimeInfo) {
        stubFor(get(urlMatching(".+")).willReturn(aResponse().withStatus(429)));

        TestOutputCollector output = runFetch(wmRuntimeInfo, new HashMap<>());

        assertTrue(output.getEmitted(Constants.HOST_INFO_STREAM_NAME).isEmpty());
    }

    /** Fetches a single URL against the WireMock server and returns the collector. */
    private TestOutputCollector runFetch(
            WireMockRuntimeInfo wmRuntimeInfo, Map<String, Object> extraConfig) {
        // allow robots.txt explicitly: the tests use a catch-all 429 stub, and a
        // 429/error on robots.txt makes crawler-commons treat the host as
        // "deny all", so the page would never be fetched. Higher priority than
        // the catch-all so it always wins for /robots.txt.
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .atPriority(1)
                        .willReturn(aResponse().withStatus(404)));

        TestOutputCollector output = new TestOutputCollector();
        Map<String, Object> config = new HashMap<>();
        config.put("http.agent.name", "this_is_only_a_test");
        config.putAll(extraConfig);
        bolt.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn("source");
        when(tuple.getStringByField("url"))
                .thenReturn("http://localhost:" + wmRuntimeInfo.getHttpPort() + "/");
        when(tuple.getValueByField("metadata")).thenReturn(null);
        bolt.execute(tuple);

        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> !output.getEmitted(Constants.StatusStreamName).isEmpty());
        return output;
    }
}
