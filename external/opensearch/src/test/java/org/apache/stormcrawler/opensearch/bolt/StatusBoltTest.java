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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.http.HttpHost;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.opensearch.persistence.StatusUpdaterBolt;
import org.apache.stormcrawler.persistence.Status;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StatusBoltTest extends AbstractOpenSearchTest {

    private StatusUpdaterBolt bolt;

    protected TestOutputCollector output;

    protected org.opensearch.client.RestHighLevelClient client;

    private static final Logger LOG = LoggerFactory.getLogger(StatusBoltTest.class);

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
    void setupStatusBolt() throws IOException {
        bolt = new StatusUpdaterBolt();
        RestClientBuilder builder =
                RestClient.builder(
                        new HttpHost(
                                opensearchContainer.getHost(),
                                opensearchContainer.getMappedPort(9200)));
        client = new RestHighLevelClient(builder);
        // configure the status updater bolt
        Map<String, Object> conf = new HashMap<>();
        conf.put("opensearch.status.routing.fieldname", "metadata.key");
        conf.put(
                "opensearch.status.addresses",
                opensearchContainer.getHost() + ":" + opensearchContainer.getFirstMappedPort());
        conf.put("scheduler.class", "org.apache.stormcrawler.persistence.DefaultScheduler");
        conf.put("status.updater.cache.spec", "maximumSize=10000,expireAfterAccess=1h");
        conf.put("metadata.persist", "someKey");
        output = new TestOutputCollector();
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @AfterEach
    void close() {
        LOG.info("Closing updater bolt and Opensearch container");
        super.close();
        bolt.cleanup();
        output = null;
        try {
            client.close();
        } catch (IOException e) {
        }
    }

    private Future<Integer> store(String url, Status status, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getValueByField("status")).thenReturn(status);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);
        return executorService.submit(
                () -> {
                    await().atMost(30, TimeUnit.SECONDS)
                            .until(() -> output.getAckedTuples().size() > 0);
                    return output.getAckedTuples().size();
                });
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    // see https://github.com/apache/stormcrawler/issues/885
    void checkListKeyFromOpensearch()
            throws IOException, ExecutionException, InterruptedException, TimeoutException {
        String url = "https://www.url.net/something";
        Metadata md = new Metadata();
        md.addValue("someKey", "someValue");
        store(url, Status.DISCOVERED, md).get(10, TimeUnit.SECONDS);
        assertEquals(1, output.getAckedTuples().size());
        // check output in Opensearch?
        String id = org.apache.commons.codec.digest.DigestUtils.sha256Hex(url);
        GetResponse result = client.get(new GetRequest("status", id), RequestOptions.DEFAULT);
        Map<String, Object> sourceAsMap = result.getSourceAsMap();
        final String pfield = "metadata.somekey";
        sourceAsMap = (Map<String, Object>) sourceAsMap.get("metadata");
        final var pfieldNew = pfield.substring(9);
        Object key = sourceAsMap.get(pfieldNew);
        assertTrue(key instanceof java.util.ArrayList);
    }
}
