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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import crawlercommons.urlfrontier.CrawlID;
import crawlercommons.urlfrontier.URLFrontierGrpc;
import crawlercommons.urlfrontier.URLFrontierGrpc.URLFrontierBlockingStub;
import crawlercommons.urlfrontier.Urlfrontier.BlockQueueParams;
import crawlercommons.urlfrontier.Urlfrontier.GetParams;
import crawlercommons.urlfrontier.Urlfrontier.URLInfo;
import io.grpc.ManagedChannel;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class HostBlockBoltTest {

    private static final String HOST = "example.com";

    private URLFrontierContainer container;
    private ManagedChannel channel;
    private URLFrontierBlockingStub blocking;

    @BeforeEach
    void before() {
        String image = "crawlercommons/url-frontier";
        String version = System.getProperty("urlfrontier-version");
        if (version != null) {
            image += ":" + version;
        }
        container = new URLFrontierContainer(image);
        container.start();
        var connection = container.getFrontierConnection();
        channel = ManagedChannelUtil.createChannel(connection.getHost(), connection.getPort());
        blocking = URLFrontierGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void after() {
        if (channel != null) {
            channel.shutdownNow();
        }
        container.close();
    }

    private Map<String, Object> frontierConfig() {
        var connection = container.getFrontierConnection();
        Map<String, Object> config = new HashMap<>();
        config.put(Constants.URLFRONTIER_HOST_KEY, connection.getHost());
        config.put(Constants.URLFRONTIER_PORT_KEY, connection.getPort());
        return config;
    }

    /** Seeds one DISCOVERED url for {@link #HOST} so the queue exists and is fetchable. */
    private void seedOneUrl() {
        StatusUpdaterBolt seeder = new StatusUpdaterBolt();
        TestOutputCollector out = new TestOutputCollector();
        Map<String, Object> config = frontierConfig();
        config.put(
                "urlbuffer.class", "org.apache.stormcrawler.persistence.urlbuffer.SimpleURLBuffer");
        config.put("scheduler.class", "org.apache.stormcrawler.persistence.DefaultScheduler");
        config.put("status.updater.cache.spec", "maximumSize=10000,expireAfterAccess=1h");
        config.put("urlfrontier.updater.max.messages", 1);
        config.put("urlfrontier.cache.expireafter.sec", 10);
        seeder.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(out));

        String url = "http://" + HOST + "/";
        Tuple tuple = mock(Tuple.class);
        when(tuple.getValueByField("status")).thenReturn(Status.DISCOVERED);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(new Metadata());
        seeder.execute(tuple);

        await().atMost(20, TimeUnit.SECONDS)
                .until(
                        () ->
                                out.getAckedTuples().stream()
                                        .anyMatch(t -> url.equals(t.getStringByField("url"))));
        seeder.cleanup();
    }

    /** Returns the set of queue keys currently handed out by getURLs. */
    private Set<String> keysFromGetURLs() {
        GetParams params =
                GetParams.newBuilder()
                        .setCrawlID(CrawlID.DEFAULT)
                        .setMaxQueues(10)
                        .setMaxUrlsPerQueue(10)
                        .build();
        Set<String> keys = new HashSet<>();
        Iterator<URLInfo> it = blocking.getURLs(params);
        while (it.hasNext()) {
            keys.add(it.next().getKey());
        }
        return keys;
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void blocksHostQueueUntilTime() {
        seedOneUrl();

        long nowSecs = System.currentTimeMillis() / 1000L;

        // block the host for an hour via the bolt under test: a queue-stream
        // tuple whose metadata reports a 429 with a Retry-After of one hour.
        // The bolt is configured through urlfrontier.address to cover the
        // address resolution as well
        HostBlockBolt bolt = new HostBlockBolt();
        var connection = container.getFrontierConnection();
        Map<String, Object> conf = new HashMap<>();
        conf.put(
                Constants.URLFRONTIER_ADDRESS_KEY,
                connection.getHost() + ":" + connection.getPort());
        conf.put(ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "protocol.");
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), mock(OutputCollector.class));
        Tuple t = mock(Tuple.class);
        when(t.getStringByField("key")).thenReturn(HOST);
        Metadata md = new Metadata();
        md.setValue("fetch.statusCode", "429");
        md.setValue("protocol.retry-after", "3600");
        when(t.getValueByField("metadata")).thenReturn(md);
        bolt.execute(t);

        // give the fire-and-forget RPC time to land before polling: getURLs
        // must never hand the URL out, and URLFrontier exposes no read API
        // for the block state. Polling getURLs() before the block is
        // effective would put the URL in-flight and make the assertion below
        // pass even without a block, so a blind delay is the only option
        await().pollDelay(Duration.ofSeconds(2)).atMost(3, TimeUnit.SECONDS).until(() -> true);
        assertTrue(
                keysFromGetURLs().isEmpty(),
                "the URL was handed out although the queue should be blocked");

        // unblock (a past time releases the queue) and the host becomes
        // available again; the URL never went in-flight, so this cannot be
        // satisfied by the in-flight timeout
        blocking.blockQueueUntil(
                BlockQueueParams.newBuilder()
                        .setKey(HOST)
                        .setCrawlID(CrawlID.DEFAULT)
                        .setTime(nowSecs - 1)
                        .build());
        await().atMost(15, TimeUnit.SECONDS).until(() -> keysFromGetURLs().contains(HOST));

        bolt.cleanup();
    }
}
