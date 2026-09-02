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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.persistence.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Runs the bolt against a frontier which predates the batched PutDiscovered endpoint (URLFrontier
 * 2.6): the bolt must detect the unsupported endpoint and fall back to sending the discovered URLs
 * individually on the streaming endpoint.
 */
@Testcontainers(disabledWithoutDocker = true)
class StatusUpdaterBoltFallbackTest {

    private StatusUpdaterBolt bolt;

    private TestOutputCollector output;

    private URLFrontierContainer urlFrontierContainer;

    private static final String persistedKey = "somePersistedKey";

    @BeforeEach
    void before() {
        // an image released before the batched endpoint existed
        urlFrontierContainer = new URLFrontierContainer("crawlercommons/url-frontier:2.5");
        urlFrontierContainer.start();
        var connection = urlFrontierContainer.getFrontierConnection();
        final var config = new HashMap<String, Object>();
        config.put(
                "urlbuffer.class", "org.apache.stormcrawler.persistence.urlbuffer.SimpleURLBuffer");
        config.put(Constants.URLFRONTIER_HOST_KEY, connection.getHost());
        config.put(Constants.URLFRONTIER_PORT_KEY, connection.getPort());
        config.put("scheduler.class", "org.apache.stormcrawler.persistence.DefaultScheduler");
        config.put("status.updater.cache.spec", "maximumSize=10000,expireAfterAccess=1h");
        config.put("metadata.persist", persistedKey);
        config.put("urlfrontier.cache.expireafter.sec", 60);
        output = new TestOutputCollector();
        bolt = new StatusUpdaterBolt();
        bolt.prepare(config, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @AfterEach
    void after() {
        bolt.cleanup();
        urlFrontierContainer.close();
        output = null;
    }

    private void store(String url, Status status, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getValueByField("status")).thenReturn(status);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);
    }

    private boolean isAcked(String url, long timeoutSeconds) {
        try {
            await().atMost(timeoutSeconds, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    output.getAckedTuples().stream()
                                            .anyMatch(
                                                    tuple ->
                                                            tuple.getStringByField("url")
                                                                    .equals(url)));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void fallsBackToStreamingWhenPutDiscoveredIsUnsupported() {
        // the first batch is flushed after the batch delay, then rejected by the old frontier:
        // the bolt resends the buffered URLs individually and acks them
        final var first = "https://www.url.net/fallback-1";
        final var second = "https://www.url.net/fallback-2";
        store(first, Status.DISCOVERED, new Metadata());
        store(second, Status.DISCOVERED, new Metadata());
        Assertions.assertTrue(isAcked(first, 30), first + " not acked");
        Assertions.assertTrue(isAcked(second, 30), second + " not acked");

        // batching has been switched off: later URLs go out straight away
        Assertions.assertFalse(bolt.isBatching());
        final var third = "https://www.url.net/fallback-3";
        store(third, Status.DISCOVERED, new Metadata());
        Assertions.assertTrue(isAcked(third, 10), third + " not acked");
        Assertions.assertEquals(0, output.getFailedTuples().size());
    }
}
