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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.solr.SolrCloudContainerTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Same as {@link StatusBoltTest} with the SolrCloud client and a status collection of 2 shards. */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class StatusBoltCloudTest extends SolrCloudContainerTest {

    private static Map<String, Object> conf;
    private static CloudSolrClient client;

    @BeforeAll
    static void setup() {
        createCollection("status", 2, 1);

        String zkConnect = getZkConnect();

        conf = new HashMap<>();
        conf.put("scheduler.class", "org.apache.stormcrawler.persistence.DefaultScheduler");
        conf.put("status.updater.cache.spec", "maximumSize=10000,expireAfterAccess=1h");
        conf.put("solr.status.zkhost", zkConnect);
        conf.put("solr.status.collection", "status");
        // the known URL and the new one go to the leader in one request
        conf.put("solr.status.batchUpdateSize", 2);
        conf.put("solr.status.flushAfterNoUpdatesMillis", 1_000);

        client =
                new CloudSolrClient.Builder(Collections.singletonList(zkConnect), Optional.empty())
                        .withDefaultCollection("status")
                        .build();
    }

    @AfterAll
    static void close() throws Exception {
        client.close();
    }

    /** A URL discovered again must keep its status, see #2304. */
    @Test
    void discoveredDoesNotOverwriteKnownUrl() throws Exception {
        String url = "https://www.url.net/missing.html";
        String newUrl = sameShard(url);

        StatusUpdaterBolt bolt = new StatusUpdaterBolt();
        TestOutputCollector output = new TestOutputCollector();
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        try {
            execute(bolt, url, Status.FETCH_ERROR);
            await().atMost(60, TimeUnit.SECONDS).until(() -> client.getById(url) != null);
        } finally {
            bolt.cleanup();
        }
        SolrDocument before = client.getById(url);

        // a second bolt stands for a restarted topology, so that the status cache of the
        // first one does not hold the URL
        StatusUpdaterBolt restarted = new StatusUpdaterBolt();
        TestOutputCollector restartedOutput = new TestOutputCollector();
        restarted.prepare(
                conf, TestUtil.getMockedTopologyContext(), new OutputCollector(restartedOutput));
        try {
            execute(restarted, url, Status.DISCOVERED);
            execute(restarted, newUrl, Status.DISCOVERED);
            // the new URL is in the same request, which a version conflict must not fail
            await().atMost(60, TimeUnit.SECONDS).until(() -> client.getById(newUrl) != null);
        } finally {
            restarted.cleanup();
        }

        SolrDocument after = client.getById(url);
        assertEquals(Status.FETCH_ERROR.name(), after.getFieldValue("status"));
        assertEquals(before.getFieldValue("nextFetchDate"), after.getFieldValue("nextFetchDate"));
        assertEquals(
                before.getFieldValues("metadata.fetch.error.count"),
                after.getFieldValues("metadata.fetch.error.count"));
        assertEquals(Status.DISCOVERED.name(), client.getById(newUrl).getFieldValue("status"));
    }

    /** Returns a new URL that goes to the same shard as the given one. */
    private static String sameShard(String url) {
        DocCollection col = client.getClusterState().getCollection("status");
        String shard = col.getRouter().getTargetSlice(url, null, null, null, col).getName();
        for (int i = 0; ; i++) {
            String candidate = "https://www.url.net/new-" + i + ".html";
            if (col.getRouter()
                    .getTargetSlice(candidate, null, null, null, col)
                    .getName()
                    .equals(shard)) {
                return candidate;
            }
        }
    }

    private static void execute(StatusUpdaterBolt bolt, String url, Status status) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getValueByField("status")).thenReturn(status);
        when(tuple.getStringByField("url")).thenReturn(url);
        when(tuple.getValueByField("metadata")).thenReturn(new Metadata());
        bolt.execute(tuple);
    }
}
