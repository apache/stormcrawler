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
package org.apache.stormcrawler.solr.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.solr.SolrContainerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies that when having 2 Solr shards, documents will be processed by the 2 respective Spouts
 */
class SpoutTest extends SolrContainerTest {

    private StatusUpdaterBolt bolt;
    private SolrSpout spoutOne;
    private SolrSpout spoutTwo;
    private TestOutputCollector boltOutput;
    private TestOutputCollector spoutOneOutput;
    private TestOutputCollector spoutTwoOutput;

    private static final Logger LOG = LoggerFactory.getLogger(StatusBoltTest.class);

    @BeforeEach
    void setup() throws IOException, InterruptedException {
        createCollection("status", 2);

        bolt = new StatusUpdaterBolt();
        boltOutput = new TestOutputCollector();

        Map<String, Object> conf = new HashMap<>();
        conf.put("scheduler.class", "org.apache.stormcrawler.persistence.DefaultScheduler");
        conf.put("status.updater.cache.spec", "maximumSize=10000,expireAfterAccess=1h");
        conf.put("solr.status.url", getSolrBaseUrl() + "/status");

        conf.put("solr.status.routing.shards", 2);
        conf.put(
                "urlbuffer.class", "org.apache.stormcrawler.persistence.urlbuffer.SimpleURLBuffer");

        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(boltOutput));

        spoutOne = new SolrSpout();
        spoutTwo = new SolrSpout();
        spoutOneOutput = new TestOutputCollector();
        spoutTwoOutput = new TestOutputCollector();

        spoutOne.open(conf, getContextForTask(0), new SpoutOutputCollector(spoutOneOutput));
        spoutTwo.open(conf, getContextForTask(1), new SpoutOutputCollector(spoutTwoOutput));
    }

    @AfterEach
    void close() {
        LOG.info("Closing updater bolt and SOLR container");
        bolt.cleanup();
        spoutOne.close();
        spoutTwo.close();
        boltOutput = null;
        spoutOneOutput = null;
        spoutTwoOutput = null;
    }

    private TopologyContext getContextForTask(int taskId) {
        Map<Integer, String> taskToComponent = new HashMap<>();
        Map<String, List<Integer>> componentToTasks = new HashMap<>();

        taskToComponent.put(0, "solrSpout");
        taskToComponent.put(1, "solrSpout");
        componentToTasks.put("solrSpout", Arrays.asList(0, 1));

        // Mock the task related components of the context
        return new TopologyContext(
                null,
                Map.of("storm.cluster.mode", "local"),
                taskToComponent,
                componentToTasks,
                new HashMap<>(),
                null,
                null,
                null,
                null,
                taskId,
                null,
                null,
                null,
                null,
                null,
                new HashMap<>(),
                new AtomicBoolean(false),
                null);
    }

    private Future<Integer> store(String url, Status status, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getValueByField("status")).thenReturn(status);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);

        return executorService.submit(
                () -> {
                    var outputSize = boltOutput.getAckedTuples().size();
                    while (outputSize == 0) {
                        Thread.sleep(100);
                        outputSize = boltOutput.getAckedTuples().size();
                    }
                    return outputSize;
                });
    }

    /**
     * When using two shards,<br>
     * the status documents should be distributed among the two spouts
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void twoShardsTest() throws ExecutionException, InterruptedException, TimeoutException {
        int expected = 100;

        for (int i = 0; i < expected; i++) {
            String url = "https://" + i + "/something";
            Metadata md = new Metadata();
            md.addValue("someKey", "someValue");
            store(url, Status.DISCOVERED, md).get(10, TimeUnit.SECONDS);
        }

        spoutOne.activate();
        spoutTwo.activate();

        while (spoutOneOutput.getEmitted().size() + spoutTwoOutput.getEmitted().size() < expected) {
            spoutOne.nextTuple();
            spoutTwo.nextTuple();
        }

        assertFalse(spoutOneOutput.getEmitted().isEmpty());
        assertFalse(spoutTwoOutput.getEmitted().isEmpty());
        assertEquals(
                expected, spoutOneOutput.getEmitted().size() + spoutTwoOutput.getEmitted().size());
    }
}
