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
package org.apache.stormcrawler.solr;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CloudConnectionTest extends SolrCloudContainerTest {
    private static final Logger LOG = LoggerFactory.getLogger(CloudConnectionTest.class);

    private static SolrConnection connection;
    private static CloudHttp2SolrClient client;

    @BeforeAll
    static void setup() {
        createCollection("docs", 1, 2);

        Map<String, Object> conf = new HashMap<>();
        conf.put("solr.indexer.zkhost", "localhost:2181");
        conf.put("solr.indexer.collection", "docs");
        conf.put("solr.indexer.batchUpdateSize", 1);
        conf.put("solr.indexer.flushAfterNoUpdatesMillis", 5_000);

        connection = SolrConnection.getConnection(conf, "indexer");
        client =
                new CloudHttp2SolrClient.Builder(
                                Collections.singletonList("localhost:2181"), Optional.empty())
                        .withDefaultCollection("docs")
                        .build();
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void consistencyTest() throws InterruptedException {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("url", "test");
        doc.addField("content", "test");
        connection.addAsync(doc);

        int leader = getLiveLeader().getNodeName().contains("8983") ? 8983 : 8984;

        // Kill the leader node
        stopNode(leader);

        // Wait for recovery
        while (getLiveLeader() == null) {
            Thread.sleep(100);
        }

        assertEquals(1, getNumFound());
    }

    private Replica getLiveLeader() {
        DocCollection col = client.getClusterState().getCollection("docs");
        Replica leader = col.getActiveSlices().stream().findFirst().orElse(null).getLeader();

        if (leader == null) {
            return null;
        }

        if (!client.getClusterState().getLiveNodes().contains(leader.getNodeName())) {
            return null;
        }

        return leader;
    }

    private void startNode(int port) {
        environment
                .getContainerByServiceName("solr")
                .ifPresent(
                        container -> {
                            try {
                                container.execInContainer(
                                        "/opt/solr/bin/solr",
                                        "start",
                                        "-c",
                                        "-p",
                                        Integer.toString(port),
                                        "-s",
                                        "/var/solr/" + port,
                                        "-z",
                                        "zookeeper:2181");
                            } catch (Exception e) {
                                LOG.error("Error while starting Solr node {}", port, e);
                            }
                        });
    }

    private void stopNode(int port) {
        environment
                .getContainerByServiceName("solr")
                .ifPresent(
                        container -> {
                            try {
                                container.execInContainer(
                                        "/opt/solr/bin/solr", "stop", "-p", Integer.toString(port));
                            } catch (Exception e) {
                                LOG.error("Error while stopping Solr node {}", port, e);
                            }
                        });
    }

    private long getNumFound() {
        SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        QueryRequest queryRequest = new QueryRequest(query);
        QueryResponse response = connection.requestAsync(queryRequest).join();
        return response.getResults().getNumFound();
    }
}
