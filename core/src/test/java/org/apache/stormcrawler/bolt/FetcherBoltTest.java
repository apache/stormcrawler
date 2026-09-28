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
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRules.RobotRulesMode;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.Protocol;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class FetcherBoltTest extends AbstractFetcherBoltTest {

    @BeforeEach
    void setUpContext() throws Exception {
        bolt = new FetcherBolt();
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
        stubFor(
                get(urlEqualTo("/page"))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withHeader("Robots.Crawl.Delay", "1")
                                        .withBody("hello")));

        Map<String, Object> config = new HashMap<>();
        config.put("http.agent.name", "this_is_only_a_test");
        config.put("fetcher.max.crawl.delay", 30);
        config.put("fetcher.max.crawl.delay.force", true);

        Metadata md = fetchAndGetContentMetadata(wmRuntimeInfo, config, "/page");
        assertEquals("120", md.getFirstValue(Constants.ROBOTS_CRAWL_DELAY_KEY));
    }

    @Test
    void fractionalLongCrawlDelayIsRoundedUp(WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        stubFor(
                get(urlEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withBody("User-agent: *\nCrawl-delay: 30.5\n")));
        stubFor(get(urlEqualTo("/page")).willReturn(aResponse().withStatus(200).withBody("hello")));

        Map<String, Object> config = new HashMap<>();
        config.put("http.agent.name", "this_is_only_a_test");
        config.put("fetcher.max.crawl.delay", 30);
        config.put("fetcher.max.crawl.delay.force", true);

        Metadata md = fetchAndGetContentMetadata(wmRuntimeInfo, config, "/page");
        assertEquals("31", md.getFirstValue(Constants.ROBOTS_CRAWL_DELAY_KEY));
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
        stubFor(
                get(urlEqualTo("/page"))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withHeader("Robots.Crawl.Delay", "120")
                                        .withBody("hello")));

        Map<String, Object> config = new HashMap<>();
        config.put("http.agent.name", "this_is_only_a_test");
        config.put("fetcher.max.crawl.delay", 30);
        config.put("fetcher.max.crawl.delay.force", true);

        Metadata md = fetchAndGetContentMetadata(wmRuntimeInfo, config, "/page");
        assertNull(md.getFirstValue(Constants.ROBOTS_CRAWL_DELAY_KEY));
    }

    @Test
    void unforcedLongCrawlDelayStillEmitsCrawlDelayErrorWithoutMetadata(
            WireMockRuntimeInfo wmRuntimeInfo) throws ReflectiveOperationException {
        // regression guard: force=false + long delay must keep today's behaviour, i.e. a
        // Status.ERROR/crawl_delay emission and no robots.crawl.delay metadata key
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
        config.put("fetcher.max.crawl.delay.force", false);

        List<Object> statusTuple = fetchAndGetStatusTuple(wmRuntimeInfo, config, "/page");
        assertEquals(Status.ERROR, statusTuple.get(2));
        Metadata md = (Metadata) statusTuple.get(1);
        assertEquals("crawl_delay", md.getFirstValue(Constants.STATUS_ERROR_CAUSE));
        assertNull(md.getFirstValue(Constants.ROBOTS_CRAWL_DELAY_KEY));
    }

    @Test
    void noHelperThreadsWithOkhttpAndFetchTimeout(WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        stubFor(get(urlEqualTo("/page")).willReturn(aResponse().withStatus(200).withBody("hello")));
        Map<String, Object> config = new HashMap<>();
        config.put("http.agent.name", "this_is_only_a_test");
        config.put("fetcher.thread.timeout", 5L);
        fetchAndGetContentMetadata(wmRuntimeInfo, config, "/page");
        assertEquals(
                0,
                ((FetcherBolt) bolt).helperPoolSize(),
                "okhttp cancels the call itself: no helper threads expected");
    }

    /** With okhttp the robots.txt fetch goes through the same call deadline as the page. */
    @Test
    void slowRobotsTxtIsBoundedByTheFetchTimeoutWithOkhttp(WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        stubFor(
                get(urlEqualTo("/robots.txt"))
                        .willReturn(aResponse().withStatus(200).withFixedDelay(10_000)));
        stubFor(get(urlEqualTo("/page")).willReturn(aResponse().withStatus(200).withBody("hello")));
        Map<String, Object> config = new HashMap<>();
        config.put("http.agent.name", "this_is_only_a_test");
        config.put("http.timeout", 30_000);
        config.put("fetcher.thread.timeout", 1L);
        long start = System.currentTimeMillis();
        // the robots.txt lookup fails at the deadline; the parser then allows the fetch
        Metadata md = fetchAndGetContentMetadata(wmRuntimeInfo, config, "/page");
        Assertions.assertNotNull(md);
        Assertions.assertTrue(
                System.currentTimeMillis() - start < 6_000, "robots.txt lookup was not bounded");
        assertEquals(0, ((FetcherBolt) bolt).helperPoolSize());
    }

    /** The queue timeout is checked after the robots.txt lookup, which the stub delays past it. */
    @Test
    void urlOverQueueTimeoutIsAckedWithoutFetchOrStatusAndCounted(WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        stubFor(
                get(urlEqualTo("/robots.txt"))
                        .willReturn(aResponse().withStatus(404).withFixedDelay(1500)));
        stubFor(get(urlEqualTo("/page")).willReturn(aResponse().withStatus(200).withBody("hello")));
        Map<String, Object> config = new HashMap<>();
        config.put("http.agent.name", "this_is_only_a_test");
        // above the robots.txt delay, so that the lookup completes
        config.put("http.timeout", 10_000);
        config.put("fetcher.timeout.queue", 1);

        resetProtocolFactory();
        TopologyContext context = TestUtil.getMockedTopologyContext();
        TestOutputCollector output = new TestOutputCollector();
        bolt.prepare(config, context, new OutputCollector(output));
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn("source");
        when(tuple.getStringByField("url"))
                .thenReturn("http://localhost:" + wmRuntimeInfo.getHttpPort() + "/page");
        bolt.execute(tuple);

        await().atMost(10, TimeUnit.SECONDS).until(() -> output.getAckedTuples().contains(tuple));
        WireMock.verify(0, getRequestedFor(urlEqualTo("/page")));
        // no sitemap in this robots.txt, so nothing at all is emitted
        assertEquals(0, output.getEmitted(Utils.DEFAULT_STREAM_ID).size());
        assertEquals(0, output.getEmitted(Constants.StatusStreamName).size());
        ArgumentCaptor<MultiCountMetric> counters = ArgumentCaptor.forClass(MultiCountMetric.class);
        Mockito.verify(context).registerMetric(eq("fetcher_counter"), counters.capture(), anyInt());
        assertEquals(1L, counters.getValue().getValueAndReset().get("queue.timeout"));
    }

    @Test
    void urlDisallowedByRobotsTxtIsAnError(WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        stubFor(
                get(urlEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withBody("User-agent: *\nDisallow: /private\n")));

        List<Object> status = fetchAndGetStatusTuple(wmRuntimeInfo, config(), "/private/page");
        assertEquals(Status.ERROR, status.get(2));
        assertEquals(
                "robots.txt",
                ((Metadata) status.get(1)).getFirstValue(Constants.STATUS_ERROR_CAUSE));
        WireMock.verify(0, getRequestedFor(urlEqualTo("/private/page")));
    }

    /** foundSitemap reports what robots.txt declares, whether discovery is on or not. */
    @ParameterizedTest
    @CsvSource({"true, , 1", "false, , 0", "false, true, 1", "true, false, 0"})
    void sitemapsDeclaredInRobotsTxtAreDiscovered(
            boolean configured,
            String override,
            int expectedOutlinks,
            WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        String sitemap = stubRobotsTxtWithSitemap(wmRuntimeInfo, "");
        stubFor(get(urlEqualTo("/page")).willReturn(aResponse().withStatus(200).withBody("hello")));
        Map<String, Object> config = config();
        config.put("sitemap.discovery", configured);
        Metadata sourceMetadata = new Metadata();
        if (override != null) {
            sourceMetadata.setValue("sitemap.discovery", override);
        }

        TestOutputCollector output = fetch(wmRuntimeInfo, config, "/page", sourceMetadata);
        List<List<Object>> outlinks = output.getEmitted(Constants.StatusStreamName);
        assertEquals(expectedOutlinks, outlinks.size());
        for (List<Object> outlink : outlinks) {
            assertSitemapOutlink(sitemap, outlink);
        }
        assertFoundSitemap(output.getEmitted(Utils.DEFAULT_STREAM_ID));
    }

    @Test
    void sitemapsAreNotSentAgainWhenRobotsTxtComesFromTheCache(WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        String sitemap = stubRobotsTxtWithSitemap(wmRuntimeInfo, "");
        stubFor(get(urlEqualTo("/a")).willReturn(aResponse().withStatus(200).withBody("a")));
        stubFor(get(urlEqualTo("/b")).willReturn(aResponse().withStatus(200).withBody("b")));
        Map<String, Object> config = config();
        config.put("sitemap.discovery", true);
        config.put("fetcher.server.delay", 0.0f);

        // one bolt for both URLs, so that the second finds robots.txt in the protocol's cache
        resetProtocolFactory();
        TestOutputCollector output = new TestOutputCollector();
        context = TestUtil.getMockedTopologyContext();
        bolt.prepare(config, context, new OutputCollector(output));
        for (String path : new String[] {"/a", "/b"}) {
            Tuple tuple = tuple(wmRuntimeInfo.getHttpBaseUrl() + path);
            bolt.execute(tuple);
            await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> output.getAckedTuples().contains(tuple));
        }

        WireMock.verify(1, getRequestedFor(urlEqualTo("/robots.txt")));
        List<List<Object>> outlinks = output.getEmitted(Constants.StatusStreamName);
        assertEquals(1, outlinks.size());
        assertSitemapOutlink(sitemap, outlinks.get(0));
        List<List<Object>> pages = output.getEmitted(Utils.DEFAULT_STREAM_ID);
        assertEquals(2, pages.size());
        for (List<Object> page : pages) {
            assertEquals(
                    "true",
                    ((Metadata) page.get(2)).getFirstValue(SiteMapParserBolt.foundSitemapKey));
        }
        assertEquals(
                Map.of(
                        "robots.fetched", 1L,
                        "robots.fromCache", 1L,
                        "fetched", 2L,
                        "bytes_fetched", 2L,
                        "status_200", 2L),
                counters(context));
    }

    @Test
    void sitemapDisallowedByRobotsTxtIsNotSent(WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        stubRobotsTxtWithSitemap(wmRuntimeInfo, "Disallow: /sitemap.xml\n");
        stubFor(get(urlEqualTo("/page")).willReturn(aResponse().withStatus(200).withBody("hello")));
        Map<String, Object> config = config();
        config.put("sitemap.discovery", true);

        TestOutputCollector output = fetch(wmRuntimeInfo, config, "/page");
        assertEquals(0, output.getEmitted(Constants.StatusStreamName).size());
        assertFoundSitemap(output.getEmitted(Utils.DEFAULT_STREAM_ID));
    }

    @Test
    void fetchedPageGoesToTheDefaultStreamWithItsMetadataAndCounters(
            WireMockRuntimeInfo wmRuntimeInfo) throws ReflectiveOperationException {
        stubFor(
                get(urlEqualTo("/robots.txt"))
                        .willReturn(aResponse().withStatus(200).withBody("User-agent: *\n")));
        stubFor(get(urlEqualTo("/page")).willReturn(aResponse().withStatus(200).withBody("hello")));
        Metadata sourceMetadata = new Metadata();
        // left over from an earlier fetch of the URL
        sourceMetadata.setValue("fetch.exception", "Socket timeout fetching");

        TestOutputCollector output = fetch(wmRuntimeInfo, config(), "/page", sourceMetadata);
        assertEquals(0, output.getEmitted(Constants.StatusStreamName).size());
        List<List<Object>> pages = output.getEmitted(Utils.DEFAULT_STREAM_ID);
        assertEquals(1, pages.size());
        assertEquals(wmRuntimeInfo.getHttpBaseUrl() + "/page", pages.get(0).get(0));
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), (byte[]) pages.get(0).get(1));
        Metadata md = (Metadata) pages.get(0).get(2);
        assertEquals("200", md.getFirstValue("fetch.statusCode"));
        assertEquals("5", md.getFirstValue("fetch.byteLength"));
        assertTrue(Long.parseLong(md.getFirstValue("fetch.loadingTime")) >= 0);
        assertTrue(Long.parseLong(md.getFirstValue("fetch.timeInQueues")) >= 0);
        assertEquals("false", md.getFirstValue(SiteMapParserBolt.foundSitemapKey));
        assertNull(md.getFirstValue("fetch.exception"));
        assertEquals(
                Map.of("robots.fetched", 1L, "fetched", 1L, "bytes_fetched", 5L, "status_200", 1L),
                counters(context));
    }

    @ParameterizedTest
    @CsvSource({"true, 1", "false, 0"})
    void redirectIsReportedAndItsTargetDiscoveredWhenAllowed(
            boolean allowed, int expectedOutlinks, WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        String target = stubRedirect(wmRuntimeInfo, "/old", "/new");
        Map<String, Object> config = config();
        config.put("redirections.allowed", allowed);

        TestOutputCollector output = fetch(wmRuntimeInfo, config, "/old");
        List<List<Object>> statuses = output.getEmitted(Constants.StatusStreamName);
        assertEquals(expectedOutlinks + 1, statuses.size());
        List<List<Object>> outlinks = withStatus(statuses, Status.DISCOVERED);
        assertEquals(expectedOutlinks, outlinks.size());
        for (List<Object> outlink : outlinks) {
            assertEquals(target, outlink.get(0));
        }
        List<Object> redirect = withStatus(statuses, Status.REDIRECTION).get(0);
        assertEquals(wmRuntimeInfo.getHttpBaseUrl() + "/old", redirect.get(0));
        assertEquals(target, ((Metadata) redirect.get(1)).getFirstValue("_redirTo"));
        assertEquals(0, output.getEmitted(Utils.DEFAULT_STREAM_ID).size());
    }

    @Test
    void overriddenAllowRedirsIsHonoured(WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        bolt =
                new FetcherBolt() {
                    @Override
                    protected boolean allowRedirs() {
                        return false;
                    }
                };
        stubRedirect(wmRuntimeInfo, "/old", "/new");
        Map<String, Object> config = config();
        config.put("redirections.allowed", true);

        List<Object> status = fetchAndGetStatusTuple(wmRuntimeInfo, config, "/old");
        assertEquals(Status.REDIRECTION, status.get(2));
    }

    @Test
    void redirectedSitemapKeepsItsSitemapKey(WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        String target = stubRedirect(wmRuntimeInfo, "/sitemap.xml", "/sitemap_index.xml");
        Metadata sourceMetadata = new Metadata();
        sourceMetadata.setValue(SiteMapParserBolt.isSitemapKey, "true");

        TestOutputCollector output = fetch(wmRuntimeInfo, config(), "/sitemap.xml", sourceMetadata);
        List<List<Object>> statuses = output.getEmitted(Constants.StatusStreamName);
        assertEquals(2, statuses.size());
        assertSitemapOutlink(target, withStatus(statuses, Status.DISCOVERED).get(0));
        assertEquals(
                wmRuntimeInfo.getHttpBaseUrl() + "/sitemap.xml",
                withStatus(statuses, Status.REDIRECTION).get(0).get(0));
    }

    @ParameterizedTest
    @ValueSource(ints = {404, 500})
    void errorStatusGoesToTheStatusStream(int code, WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        stubFor(get(urlEqualTo("/page")).willReturn(aResponse().withStatus(code)));

        List<Object> status = fetchAndGetStatusTuple(wmRuntimeInfo, config(), "/page");
        assertEquals(Status.FETCH_ERROR, status.get(2));
        assertEquals(
                Integer.toString(code),
                ((Metadata) status.get(1)).getFirstValue("fetch.statusCode"));
    }

    @Test
    void unknownHostIsAFetchError(WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        Map<String, Object> config = config();
        config.put("http.protocol.implementation", UnknownHostProtocol.class.getName());

        List<Object> status = fetchAndGetStatusTuple(wmRuntimeInfo, config, "/page");
        assertEquals(Status.FETCH_ERROR, status.get(2));
        assertEquals("Unknown host", ((Metadata) status.get(1)).getFirstValue("fetch.exception"));
        assertEquals(Map.of("robots.fetched", 1L, "exception", 1L), counters(context));
    }

    /**
     * With no fetcher thread nothing leaves the fetch queues, so the fail is immediate and the test
     * needs no timing. Each row sends one URL per host with the key in the same position ("-" for
     * none) and lists the positions expected to fail.
     */
    @ParameterizedTest(name = "{0}")
    @CsvSource(
            delimiter = '|',
            value = {
                "same host, no key        | a a     | - -     | 1",
                "two hosts, same key      | a b     | k k     | 1",
                "two hosts, no key        | a b a b | - - - - | 2 3",
                "keys differ only by case | a b     | K k     | 1",
            })
    void urlForAFullFetchQueueIsFailed(
            String scenario, String hosts, String keys, String expectedFailed) {
        Map<String, Object> config = config();
        config.put("fetcher.threads.number", 0);
        config.put("fetcher.max.queue.size", 1);
        TestOutputCollector output = new TestOutputCollector();
        bolt.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        String[] hostList = hosts.split(" ");
        String[] keyList = keys.split(" ");
        List<String> failed = new ArrayList<>();
        for (int i = 0; i < hostList.length; i++) {
            Tuple tuple = tuple("http://" + hostList[i] + ".test/" + i);
            if (!"-".equals(keyList[i])) {
                when(tuple.contains("key")).thenReturn(true);
                when(tuple.getStringByField("key")).thenReturn(keyList[i]);
            }
            bolt.execute(tuple);
            if (output.getFailedTuples().contains(tuple)) {
                failed.add(Integer.toString(i));
            }
        }
        assertEquals(expectedFailed, String.join(" ", failed));
        assertEquals(0, output.getAckedTuples().size());
    }

    @Test
    void blankUrlIsAckedAndIgnored() {
        TestOutputCollector output = new TestOutputCollector();
        bolt.prepare(config(), TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        Tuple tuple = tuple(" ");
        bolt.execute(tuple);

        assertEquals(List.of(tuple), output.getAckedTuples());
        assertEquals(0, output.getFailedTuples().size());
        assertEquals(0, output.getEmitted(Utils.DEFAULT_STREAM_ID).size());
        assertEquals(0, output.getEmitted(Constants.StatusStreamName).size());
    }

    /**
     * Two URLs of one host sent together: the second request waits for the delay of robots.txt,
     * raised to fetcher.server.delay when that one is forced.
     */
    @ParameterizedTest
    @CsvSource({"0.0, false, 1000", "1.5, true, 1500"})
    void robotsCrawlDelayPacesTheFetchQueue(
            float serverDelay, boolean force, long minGap, WireMockRuntimeInfo wmRuntimeInfo)
            throws ReflectiveOperationException {
        stubFor(
                get(urlEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withBody("User-agent: *\nCrawl-delay: 1\n")));
        stubFor(get(urlEqualTo("/a")).willReturn(aResponse().withStatus(200).withBody("a")));
        stubFor(get(urlEqualTo("/b")).willReturn(aResponse().withStatus(200).withBody("b")));
        Map<String, Object> config = config();
        config.put("fetcher.threads.per.queue", 1);
        config.put("fetcher.server.delay", serverDelay);
        config.put("fetcher.server.delay.force", force);

        resetProtocolFactory();
        TestOutputCollector output = new TestOutputCollector();
        bolt.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        bolt.execute(tuple(wmRuntimeInfo.getHttpBaseUrl() + "/a"));
        bolt.execute(tuple(wmRuntimeInfo.getHttpBaseUrl() + "/b"));

        await().atMost(15, TimeUnit.SECONDS).until(() -> output.getAckedTuples().size() == 2);
        long gap = requestTime("/b") - requestTime("/a");
        assertTrue(gap >= minGap, "second request after " + gap + " ms");
    }

    public static class UnknownHostProtocol implements Protocol {

        @Override
        public void configure(Config conf) {}

        @Override
        public ProtocolResponse getProtocolOutput(String url, Metadata metadata) throws Exception {
            throw new UnknownHostException(url);
        }

        @Override
        public BaseRobotRules getRobotRules(String url) {
            return new SimpleRobotRules(RobotRulesMode.ALLOW_ALL);
        }

        @Override
        public void cleanup() {}
    }

    private static Map<String, Object> config() {
        Map<String, Object> config = new HashMap<>();
        config.put("http.agent.name", "this_is_only_a_test");
        return config;
    }

    private static Tuple tuple(String url) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn("source");
        when(tuple.getStringByField("url")).thenReturn(url);
        return tuple;
    }

    /** Stubs a robots.txt declaring /sitemap.xml after the given rules; returns its URL. */
    private static String stubRobotsTxtWithSitemap(
            WireMockRuntimeInfo wmRuntimeInfo, String rules) {
        String sitemap = wmRuntimeInfo.getHttpBaseUrl() + "/sitemap.xml";
        stubFor(
                get(urlEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withBody(
                                                "User-agent: *\n"
                                                        + rules
                                                        + "Sitemap: "
                                                        + sitemap
                                                        + "\n")));
        return sitemap;
    }

    /** Stubs a 301 from one path to another; returns the absolute target. */
    private static String stubRedirect(WireMockRuntimeInfo wmRuntimeInfo, String from, String to) {
        String target = wmRuntimeInfo.getHttpBaseUrl() + to;
        stubFor(
                get(urlEqualTo(from))
                        .willReturn(aResponse().withStatus(301).withHeader("Location", target)));
        return target;
    }

    private static void assertSitemapOutlink(String url, List<Object> outlink) {
        assertEquals(url, outlink.get(0));
        assertEquals(Status.DISCOVERED, outlink.get(2));
        assertEquals(
                "true", ((Metadata) outlink.get(1)).getFirstValue(SiteMapParserBolt.isSitemapKey));
    }

    private static void assertFoundSitemap(List<List<Object>> pages) {
        assertEquals(1, pages.size());
        assertEquals(
                "true",
                ((Metadata) pages.get(0).get(2)).getFirstValue(SiteMapParserBolt.foundSitemapKey));
    }

    private static List<List<Object>> withStatus(List<List<Object>> tuples, Status status) {
        return tuples.stream().filter(t -> t.get(2) == status).toList();
    }

    private static Map<String, ?> counters(TopologyContext context) {
        ArgumentCaptor<MultiCountMetric> counters = ArgumentCaptor.forClass(MultiCountMetric.class);
        Mockito.verify(context).registerMetric(eq("fetcher_counter"), counters.capture(), anyInt());
        return counters.getValue().getValueAndReset();
    }

    private static long requestTime(String path) {
        return WireMock.getAllServeEvents().stream()
                .filter(e -> e.getRequest().getUrl().equals(path))
                .findFirst()
                .orElseThrow()
                .getRequest()
                .getLoggedDate()
                .getTime();
    }
}
