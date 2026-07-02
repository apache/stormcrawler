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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.persistence.Status;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class StatusUpdaterBoltTest {

    private StatusUpdaterBolt bolt;

    private TestOutputCollector output;

    private URLFrontierContainer urlFrontierContainer;

    private static final String persistedKey = "somePersistedKey";

    private static final String notPersistedKey = "someNotPersistedKey";

    private static ExecutorService executorService;

    @BeforeAll
    static void beforeClass() {
        executorService = Executors.newFixedThreadPool(2);
    }

    @AfterAll
    static void afterClass() {
        executorService.shutdown();
        executorService = null;
    }

    @BeforeEach
    void before() {
        String image = "crawlercommons/url-frontier";
        String version = System.getProperty("urlfrontier-version");
        if (version != null) {
            image += ":" + version;
        }
        urlFrontierContainer = new URLFrontierContainer(image);
        urlFrontierContainer.start();
        bolt = new StatusUpdaterBolt();
        var connection = urlFrontierContainer.getFrontierConnection();
        final var config = new HashMap<String, Object>();
        config.put(
                "urlbuffer.class", "org.apache.stormcrawler.persistence.urlbuffer.SimpleURLBuffer");
        config.put(Constants.URLFRONTIER_HOST_KEY, connection.getHost());
        config.put(Constants.URLFRONTIER_PORT_KEY, connection.getPort());
        config.put("scheduler.class", "org.apache.stormcrawler.persistence.DefaultScheduler");
        config.put("status.updater.cache.spec", "maximumSize=10000,expireAfterAccess=1h");
        config.put("metadata.persist", persistedKey);
        config.put("urlfrontier.updater.max.messages", 1);
        config.put("urlfrontier.cache.expireafter.sec", 10);
        output = new TestOutputCollector();
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

    private boolean isAcked(String url, long timeoutSeconds, long start) {
        long elapsed = System.currentTimeMillis() - start;
        long remaining = timeoutSeconds * 1000 - elapsed;
        if (remaining <= 0) {
            return false;
        }
        try {
            await().atMost(remaining, TimeUnit.MILLISECONDS)
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
    void canAckSimpleTupleWithMetadata()
            throws ExecutionException, InterruptedException, TimeoutException {
        final var url = "https://www.url.net/something";
        final var meta = new Metadata();
        meta.setValue(persistedKey, "somePersistedMetaInfo");
        meta.setValue(notPersistedKey, "someNotPersistedMetaInfo");
        store(url, Status.DISCOVERED, meta);
        Assertions.assertEquals(true, isAcked(url, 5));
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void exceedingMaxMessagesInFlightAfterFrontierRestart()
            throws ExecutionException, InterruptedException, TimeoutException {
        // Stopping the frontier to simulate the following situation:
        // The inFlightSemaphore runs full during an intermediate downtime of the frontier
        urlFrontierContainer.stop();
        // Sending two URLs and therefore exceeding the maximum number of messages in flight
        // This must not lead to starvation after frontier restart.
        store("http://example.com/?test=1", Status.DISCOVERED, new Metadata());
        store("http://example.com/?test=2", Status.DISCOVERED, new Metadata());
        long start = System.currentTimeMillis();
        Assertions.assertEquals(false, isAcked("http://example.com/?test=1", 5, start));
        Assertions.assertEquals(false, isAcked("http://example.com/?test=2", 5, start));
        urlFrontierContainer.start();
        store("http://example.com/?test=3", Status.DISCOVERED, new Metadata());
        Assertions.assertEquals(true, isAcked("http://example.com/?test=3", 10));
    }
}
