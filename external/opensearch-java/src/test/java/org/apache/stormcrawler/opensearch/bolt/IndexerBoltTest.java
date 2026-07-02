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

package org.apache.stormcrawler.opensearch.bolt;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.indexing.AbstractIndexerBolt;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IndexerBoltTest extends AbstractOpenSearchTest {

    private IndexerBolt bolt;

    protected TestOutputCollector output;

    private static final Logger LOG = LoggerFactory.getLogger(IndexerBoltTest.class);

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
    void setupIndexerBolt() {
        bolt = new IndexerBolt("content");
        // give the indexer the port for connecting to OpenSearch
        final String host = opensearchContainer.getHost();
        final Integer port = opensearchContainer.getFirstMappedPort();
        final Map<String, Object> conf = new HashMap<>();
        conf.put(AbstractIndexerBolt.urlFieldParamName, "url");
        conf.put(AbstractIndexerBolt.canonicalMetadataParamName, "canonical");
        conf.put("opensearch.indexer.addresses", host + ":" + port);
        output = new TestOutputCollector();
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @AfterEach
    void close() {
        LOG.info("Closing indexer bolt and Opensearch container");
        super.close();
        bolt.cleanup();
        output = null;
    }

    private void index(String url, String text, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("text")).thenReturn(text);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);
    }

    private int lastIndex(String url, String text, Metadata metadata, long timeoutInMs)
            throws ExecutionException, InterruptedException, TimeoutException {
        var oldSize = output.getEmitted(Constants.StatusStreamName).size();
        index(url, text, metadata);
        return executorService
                .submit(
                        () -> {
                            await().atMost(timeoutInMs, TimeUnit.MILLISECONDS)
                                    .until(
                                            () ->
                                                    output.getEmitted(Constants.StatusStreamName)
                                                                    .size()
                                                            > oldSize);
                            return output.getEmitted(Constants.StatusStreamName).size();
                        })
                .get(timeoutInMs, TimeUnit.MILLISECONDS);
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    // https://github.com/apache/stormcrawler/issues/832
    void simultaneousCanonicals()
            throws ExecutionException, InterruptedException, TimeoutException {
        Metadata m1 = new Metadata();
        String url =
                "https://www.obozrevatel.com/ukr/dnipro/city/u-dnipri-ta-oblasti-ogolosili-shtormove-poperedzhennya.htm";
        m1.addValue("canonical", url);
        Metadata m2 = new Metadata();
        String url2 =
                "https://www.obozrevatel.com/ukr/dnipro/city/u-dnipri-ta-oblasti-ogolosili-shtormove-poperedzhennya/amp.htm";
        m2.addValue("canonical", url);
        index(url, "", m1);
        lastIndex(url2, "", m2, 10_000);
        // should be two in status output
        assertEquals(2, output.getEmitted(Constants.StatusStreamName).size());
        // and 2 acked
        assertEquals(2, output.getAckedTuples().size());
        // TODO check output in Opensearch?
    }
}
