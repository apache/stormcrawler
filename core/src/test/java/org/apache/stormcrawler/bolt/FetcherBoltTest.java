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
import static com.github.tomakehurst.wiremock.client.WireMock.findAll;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@WireMockTest
public class FetcherBoltTest extends AbstractFetcherBoltTest {

    private static final DateTimeFormatter HTTP_DATE =
            DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.ROOT)
                    .withZone(ZoneOffset.UTC);

    @BeforeEach
    void setUpContext() throws Exception {
        bolt = new FetcherBolt();
    }

    @Test
    void parsesDelayInSeconds() {
        Assertions.assertEquals(120_000L, FetcherBolt.parseRetryAfterDelay("120"));
        // surrounding whitespace is tolerated
        Assertions.assertEquals(90_000L, FetcherBolt.parseRetryAfterDelay("  90 "));
        Assertions.assertEquals(0L, FetcherBolt.parseRetryAfterDelay("0"));
    }

    @Test
    void parsesHttpDate() {
        String future = HTTP_DATE.format(Instant.now().plusSeconds(120));
        long delay = FetcherBolt.parseRetryAfterDelay(future);
        // allow some slack for the clock ticking between formatting and parsing
        Assertions.assertTrue(
                delay > 100_000L && delay <= 120_000L, "unexpected delay: " + delay);
    }

    @Test
    void returnsMinusOneForInvalidOrAbsentValues() {
        Assertions.assertEquals(-1L, FetcherBolt.parseRetryAfterDelay(null));
        Assertions.assertEquals(-1L, FetcherBolt.parseRetryAfterDelay(""));
        Assertions.assertEquals(-1L, FetcherBolt.parseRetryAfterDelay("not-a-date"));
        // a date in the past is ignored
        String past = HTTP_DATE.format(Instant.now().minusSeconds(120));
        Assertions.assertEquals(-1L, FetcherBolt.parseRetryAfterDelay(past));
    }

    @Test
    void retryAfterDelaysNextFetchFromSameQueue(WireMockRuntimeInfo wmRuntimeInfo) {
        // first URL of the host asks for a 3s back-off, the second is fine
        stubFor(
                get(urlEqualTo("/a"))
                        .willReturn(aResponse().withStatus(503).withHeader("Retry-After", "3")));
        stubFor(get(urlEqualTo("/b")).willReturn(aResponse().withStatus(200).withBody("ok")));

        TestOutputCollector output = new TestOutputCollector();
        Map<String, Object> config = new HashMap<>();
        config.put("http.agent.name", "this_is_only_a_test");
        // keep the regular politeness delay low so only the Retry-After can
        // explain a multi-second gap between the two fetches
        config.put("fetcher.server.delay", 0.1f);
        bolt.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        String base = "http://localhost:" + wmRuntimeInfo.getHttpPort();
        bolt.execute(tupleForUrl(base + "/a"));
        bolt.execute(tupleForUrl(base + "/b"));

        // both URLs are fetched and acked
        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> output.getAckedTuples().size() == 2);

        List<LoggedRequest> a = findAll(getRequestedFor(urlEqualTo("/a")));
        List<LoggedRequest> b = findAll(getRequestedFor(urlEqualTo("/b")));
        Assertions.assertEquals(1, a.size());
        Assertions.assertEquals(1, b.size());

        long gap = b.get(0).getLoggedDate().getTime() - a.get(0).getLoggedDate().getTime();
        // the second fetch must have waited roughly the requested 3 seconds
        Assertions.assertTrue(
                gap >= 2_500L,
                "second fetch happened only " + gap + "ms after the first, Retry-After ignored");
    }

    private static Tuple tupleForUrl(String url) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn("source");
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(null);
        return tuple;
    }
}
