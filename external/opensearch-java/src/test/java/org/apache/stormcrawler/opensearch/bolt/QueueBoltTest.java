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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hc.core5.http.HttpHost;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.opensearch.persistence.QueueBolt;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueueBoltTest extends AbstractOpenSearchTest {

    private QueueBolt bolt;

    protected TestOutputCollector output;

    protected OpenSearchClient client;

    private OpenSearchTransport transport;

    private static final Logger LOG = LoggerFactory.getLogger(QueueBoltTest.class);

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
    void setupQueueBolt() throws IOException {
        transport =
                ApacheHttpClient5TransportBuilder.builder(
                                new HttpHost(
                                        "http",
                                        opensearchContainer.getHost(),
                                        opensearchContainer.getMappedPort(9200)))
                        .setMapper(new JacksonJsonpMapper())
                        .build();
        client = new OpenSearchClient(transport);
        output = new TestOutputCollector();
        bolt = prepareQueueBolt();
    }

    private QueueBolt prepareQueueBolt() {
        QueueBolt queueBolt = new QueueBolt();
        Map<String, Object> conf = new HashMap<>();
        conf.put(
                "opensearch.queues.addresses",
                opensearchContainer.getHost() + ":" + opensearchContainer.getFirstMappedPort());
        queueBolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        return queueBolt;
    }

    @AfterEach
    void close() {
        LOG.info("Closing queue bolt and Opensearch container");
        super.close();
        bolt.cleanup();
        output = null;
        try {
            transport.close();
        } catch (IOException e) {
        }
    }

    private Future<Integer> store(String key, Metadata metadata) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getStringByField("key")).thenReturn(key);
        when(tuple.getValueByField("metadata")).thenReturn(metadata);
        bolt.execute(tuple);
        return executorService.submit(
                () -> {
                    await().atMost(30, TimeUnit.SECONDS)
                            .until(() -> output.getAckedTuples().size() > 0);
                    return output.getAckedTuples().size();
                });
    }

    private void awaitIndexed(String key) {
        String id = org.apache.commons.codec.digest.DigestUtils.sha256Hex(key);
        await().atMost(30, TimeUnit.SECONDS)
                .until(
                        () -> {
                            try {
                                GetResponse<Map> result =
                                        client.get(g -> g.index("queues").id(id), Map.class);
                                return result.found();
                            } catch (IOException e) {
                                return false;
                            }
                        });
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getSource(String key) throws IOException {
        String id = org.apache.commons.codec.digest.DigestUtils.sha256Hex(key);
        return client.get(g -> g.index("queues").id(id), Map.class).source();
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void checkQueueIndex()
            throws IOException, ExecutionException, InterruptedException, TimeoutException {
        String key = "www.url.net";
        Metadata md = new Metadata();
        md.addValue("someKey", "someValue");
        store(key, md).get(10, TimeUnit.SECONDS);
        assertEquals(1, output.getAckedTuples().size());

        // Wait until document is indexed in OpenSearch
        awaitIndexed(key);

        Map<String, Object> sourceAsMap = getSource(key);
        assertEquals(key, sourceAsMap.get("key"));
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void checkExistingEntryIsNotOverwritten()
            throws IOException, ExecutionException, InterruptedException, TimeoutException {
        String key = "www.url.net";
        Metadata md = new Metadata();
        md.addValue("someKey", "someValue");
        store(key, md).get(10, TimeUnit.SECONDS);
        awaitIndexed(key);
        Object lastUpdated = getSource(key).get("lastUpdated");

        // a fresh bolt instance (e.g. after a worker restart) has an empty cache
        // and re-sends the same key - the resulting version conflict must be
        // handled gracefully and the existing entry left untouched
        QueueBolt restartedBolt = prepareQueueBolt();
        try {
            Tuple sameKey = mock(Tuple.class);
            when(sameKey.getStringByField("key")).thenReturn(key);
            restartedBolt.execute(sameKey);

            // a new key sent in the same bulk signals when the bulk has been processed
            String otherKey = "www.other.net";
            Tuple other = mock(Tuple.class);
            when(other.getStringByField("key")).thenReturn(otherKey);
            restartedBolt.execute(other);
            awaitIndexed(otherKey);

            Map<String, Object> sourceAsMap = getSource(key);
            assertEquals(key, sourceAsMap.get("key"));
            assertEquals(lastUpdated, sourceAsMap.get("lastUpdated"));
        } finally {
            restartedBolt.cleanup();
        }
    }
}
