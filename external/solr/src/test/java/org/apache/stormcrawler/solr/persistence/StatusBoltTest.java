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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.solr.SolrContainerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StatusBoltTest extends SolrContainerTest {
    private StatusUpdaterBolt bolt;
    protected TestOutputCollector output;

    private static final Logger LOG = LoggerFactory.getLogger(StatusBoltTest.class);

    @BeforeEach
    void setup() throws IOException, InterruptedException {
        createCollection("status", 4);

        bolt = new StatusUpdaterBolt();
        output = new TestOutputCollector();

        Map<String, Object> conf = new HashMap<>();
        conf.put("scheduler.class", "org.apache.stormcrawler.persistence.DefaultScheduler");
        conf.put("status.updater.cache.spec", "maximumSize=10000,expireAfterAccess=1h");
        conf.put("solr.status.url", getSolrBaseUrl() + "/status");

        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @AfterEach
    void close() {
        LOG.info("Closing updater bolt and SOLR container");
        bolt.cleanup();
        output = null;
    }

    private Future<Integer> store(String url, Status status, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getValueByField("status")).thenReturn(status);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);

        return executorService.submit(
                () -> {
                    var outputSize = output.getAckedTuples().size();
                    while (outputSize == 0) {
                        Thread.sleep(100);
                        outputSize = output.getAckedTuples().size();
                    }
                    return outputSize;
                });
    }

    @Test
    void basicTest() throws ExecutionException, InterruptedException, TimeoutException {

        String url = "https://www.url.net/something";

        Metadata md = new Metadata();

        md.addValue("someKey", "someValue");

        store(url, Status.DISCOVERED, md).get(10, TimeUnit.SECONDS);

        assertEquals(1, output.getAckedTuples().size());
    }

    @Test
    void errorTest() throws Exception {

        String url = "https://www.url.net/something";

        Metadata md = new Metadata();

        md.addValue("someKey", "someValue");

        store(url, Status.ERROR, md).get(10, TimeUnit.SECONDS);

        assertEquals(1, output.getAckedTuples().size());
    }
}
