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
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import java.util.HashMap;
import java.util.Map;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SimpleFetcherBoltTest extends AbstractFetcherBoltTest {

    @BeforeEach
    void setUpContext() throws Exception {
        bolt = new SimpleFetcherBolt();
    }

    @Test
    void forcedLongCrawlDelayIsReportedInMetadata(WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        // robots.txt with a Crawl-delay above fetcher.max.crawl.delay
        stubFor(
                get(urlEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withBody("User-agent: *\nCrawl-delay: 120\n")));
        stubFor(get(urlEqualTo("/page")).willReturn(aResponse().withStatus(200).withBody("hello")));

        Map<String, Object> config = new HashMap<>();
        config.put("http.agent.name", "this_is_only_a_test");
        config.put("fetcher.max.crawl.delay", 30);
        config.put("fetcher.max.crawl.delay.force", true);

        Metadata md = fetchAndGetContentMetadata(wmRuntimeInfo, config, "/page");
        assertEquals("120", md.getFirstValue(Constants.ROBOTS_CRAWL_DELAY_KEY));
    }

    @Test
    void shortCrawlDelayIsNotReported(WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        stubFor(
                get(urlEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withBody("User-agent: *\nCrawl-delay: 5\n")));
        stubFor(get(urlEqualTo("/page")).willReturn(aResponse().withStatus(200).withBody("hello")));

        Map<String, Object> config = new HashMap<>();
        config.put("http.agent.name", "this_is_only_a_test");
        config.put("fetcher.max.crawl.delay", 30);
        config.put("fetcher.max.crawl.delay.force", true);

        Metadata md = fetchAndGetContentMetadata(wmRuntimeInfo, config, "/page");
        assertNull(md.getFirstValue(Constants.ROBOTS_CRAWL_DELAY_KEY));
    }
}
