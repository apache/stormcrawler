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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrConnection {
    private static final Logger LOG = LoggerFactory.getLogger(SolrConnection.class);

    private final SolrClient client;
    private final SolrClient updateClient;

    private final boolean cloud;
    private final String collection;

    private long lastUpdate;
    private final Map<String, List<Update>> updateQueues;
    private final Object lock = new Object();

    private final int updateQueueSize;

    // Maximum time (ms) without updates before we flush all the queues
    private final long noUpdateThreshold;

    // true if we deal with the sharded status collection
    private final boolean statusCollection;

    private final ScheduledExecutorService executor;

    private SolrConnection(
            SolrClient client,
            SolrClient updateClient,
            boolean cloud,
            String collection,
            boolean statusCollection,
            int updateQueueSize,
            long noUpdateThreshold) {
        this.client = client;
        this.updateClient = updateClient;
        this.cloud = cloud;
        this.collection = collection;
        this.statusCollection = statusCollection;

        this.updateQueues = new HashMap<>();
        this.lastUpdate = System.currentTimeMillis();
        this.executor = Executors.newSingleThreadScheduledExecutor();

        this.updateQueueSize = updateQueueSize;
        this.noUpdateThreshold = noUpdateThreshold;

        if (cloud) {
            // Periodically check if we should flush
            executor.scheduleAtFixedRate(
                    () -> flushAllUpdates(false),
                    noUpdateThreshold,
                    noUpdateThreshold,
                    TimeUnit.MILLISECONDS);
        }
    }

    private void flushAllUpdates(boolean force) {
        synchronized (lock) {
            if (!force && System.currentTimeMillis() - lastUpdate < noUpdateThreshold) return;

            CloudHttp2SolrClient cloudHttp2SolrClient = (CloudHttp2SolrClient) client;
            DocCollection col = cloudHttp2SolrClient.getClusterState().getCollection(collection);

            // Flush all slices
            for (var entry : updateQueues.entrySet()) {
                List<Update> waitingUpdates = entry.getValue();
                if (waitingUpdates.isEmpty()) continue;

                Slice slice = col.getSlice(entry.getKey());
                Replica leader = slice.getLeader();

                if (leader == null) {
                    LOG.error("Could not find the leader for slice {}", slice.getName());
                    return;
                }

                flushUpdates(leader, waitingUpdates, cloudHttp2SolrClient);
            }
        }
    }

    private void updateAsync(Update update) {
        synchronized (lock) {
            lastUpdate = System.currentTimeMillis();

            CloudHttp2SolrClient cloudHttp2SolrClient = (CloudHttp2SolrClient) client;
            DocCollection col = cloudHttp2SolrClient.getClusterState().getCollection(collection);

            // Find slice for this update
            Slice slice = null;

            if (statusCollection) {
                // We have multiple shards, find the correct one
                slice = getSlice(((DocUpdate) update).doc, col);
            } else {
                slice = col.getActiveSlices().stream().findFirst().orElse(null);
            }

            if (slice == null) {
                LOG.error("Could not find an active slice for update {}", update);
                return;
            }

            // Get the queue for this slice
            List<Update> waitingUpdates =
                    updateQueues.computeIfAbsent(slice.getName(), k -> new ArrayList<>());
            waitingUpdates.add(update);

            if (waitingUpdates.size() >= updateQueueSize) {
                Replica leader = slice.getLeader();

                if (leader == null) {
                    LOG.error("Could not find the leader for slice {}", slice.getName());
                    return;
                }

                flushUpdates(leader, waitingUpdates, cloudHttp2SolrClient);
            }
        }
    }

    /**
     * Flush all waiting updates for this slice to the slice leader. The request will fail, if the
     * leader goes down before handling it.
     */
    private void flushUpdates(
            Replica leader,
            List<Update> waitingUpdates,
            CloudHttp2SolrClient cloudHttp2SolrClient) {

        List<LBSolrClient.Endpoint> endpoints = new ArrayList<>();
        endpoints.add(new LBSolrClient.Endpoint(leader.getBaseUrl(), leader.getCoreName()));

        // Separate deletions and documents
        List<String> deletionIds = new ArrayList<>();
        List<SolrInputDocument> docs = new ArrayList<>();

        for (Update u : waitingUpdates) {
            if (u instanceof DeleteUpdate deleteUpdate) {
                deletionIds.add(deleteUpdate.id);
            } else if (u instanceof DocUpdate docUpdate) {
                docs.add(docUpdate.doc);
            }
        }

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.add(docs);
        updateRequest.deleteById(deletionIds);

        List<Update> batch = new ArrayList<>(waitingUpdates);
        waitingUpdates.clear();

        // Get the async client
        LBHttp2SolrClient lbHttp2SolrClient = cloudHttp2SolrClient.getLbClient();
        LBSolrClient.Req req = new LBSolrClient.Req(updateRequest, endpoints);

        lbHttp2SolrClient
                .requestAsync(req)
                .whenComplete(
                        (futureResponse, throwable) -> {
                            if (throwable != null) {
                                LOG.error("Exception caught while updating", throwable);

                                // The request failed => add the batch back to the pending updates
                                synchronized (lock) {
                                    waitingUpdates.addAll(batch);
                                }
                            }
                        });
    }

    private Slice getSlice(SolrInputDocument doc, DocCollection col) {
        return col.getRouter().getTargetSlice(null, doc, null, null, col);
    }

    public void addAsync(SolrInputDocument doc) {
        if (cloud) {
            updateAsync(new DocUpdate(doc));
        } else {
            try {
                updateClient.add(doc);
            } catch (Exception e) {
                LOG.error("Exception caught while updating", e);
            }
        }
    }

    public void deleteByIdAsync(String id) {
        if (cloud) {
            updateAsync(new DeleteUpdate(id));
        } else {
            try {
                updateClient.deleteById(id);
            } catch (Exception e) {
                LOG.error("Exception caught while deleting", e);
            }
        }
    }

    public CompletableFuture<QueryResponse> requestAsync(QueryRequest request) {
        if (cloud) {
            CloudHttp2SolrClient cloudHttp2SolrClient = (CloudHttp2SolrClient) client;

            // Find the shard to route the request to
            String shardId = request.getParams().get("shards");
            if (shardId == null) {
                shardId = "shard1";
            }

            Slice slice =
                    cloudHttp2SolrClient
                            .getClusterState()
                            .getCollection(collection)
                            .getSlice(shardId);

            // Will get results from the first successful replica of this shard
            List<LBSolrClient.Endpoint> endpoints = new ArrayList<>();

            for (Replica replica : slice.getReplicas()) {
                if (replica.getState() == Replica.State.ACTIVE) {
                    endpoints.add(
                            new LBSolrClient.Endpoint(replica.getBaseUrl(), replica.getCoreName()));
                }
            }

            // Shuffle the endpoints for basic load balancing
            Collections.shuffle(endpoints);

            // Get the async client
            LBHttp2SolrClient lbHttp2SolrClient = cloudHttp2SolrClient.getLbClient();
            LBSolrClient.Req req = new LBSolrClient.Req(request, endpoints);

            return lbHttp2SolrClient
                    .requestAsync(req)
                    .thenApply(rsp -> new QueryResponse(rsp.getResponse(), lbHttp2SolrClient));
        } else {
            return ((Http2SolrClient) client)
                    .requestAsync(request)
                    .thenApply(nl -> new QueryResponse(nl, client));
        }
    }

    public static SolrConnection getConnection(Map<String, Object> stormConf, String boltType) {
        String collection =
                ConfUtils.getString(
                        stormConf, Constants.PARAMPREFIX + boltType + ".collection", null);
        String zkHost =
                ConfUtils.getString(stormConf, Constants.PARAMPREFIX + boltType + ".zkhost", null);

        String solrUrl =
                ConfUtils.getString(stormConf, Constants.PARAMPREFIX + boltType + ".url", null);
        int queueSize =
                ConfUtils.getInt(stormConf, Constants.PARAMPREFIX + boltType + ".queueSize", 100);

        int updateQueueSize =
                ConfUtils.getInt(
                        stormConf, Constants.PARAMPREFIX + boltType + ".batchUpdateSize", 10);
        long noUpdateThreshold =
                ConfUtils.getLong(
                        stormConf,
                        Constants.PARAMPREFIX + boltType + ".flushAfterNoUpdatesMillis",
                        20_000);

        boolean statusCollection = boltType.equals("status");

        if (StringUtils.isNotBlank(zkHost)) {

            CloudHttp2SolrClient.Builder builder =
                    new CloudHttp2SolrClient.Builder(
                            Collections.singletonList(zkHost), Optional.empty());

            if (StringUtils.isNotBlank(collection)) {
                builder.withDefaultCollection(collection);
            }

            CloudHttp2SolrClient cloudHttp2SolrClient = builder.build();

            return new SolrConnection(
                    cloudHttp2SolrClient,
                    cloudHttp2SolrClient,
                    true,
                    collection,
                    statusCollection,
                    updateQueueSize,
                    noUpdateThreshold);

        } else if (StringUtils.isNotBlank(solrUrl)) {

            Http2SolrClient http2SolrClient = new Http2SolrClient.Builder(solrUrl).build();

            ConcurrentUpdateHttp2SolrClient concurrentUpdateHttp2SolrClient =
                    new ConcurrentUpdateHttp2SolrClient.Builder(solrUrl, http2SolrClient, true)
                            .withQueueSize(queueSize)
                            .build();

            return new SolrConnection(
                    http2SolrClient,
                    concurrentUpdateHttp2SolrClient,
                    false,
                    collection,
                    statusCollection,
                    updateQueueSize,
                    noUpdateThreshold);

        } else {
            throw new RuntimeException("SolrClient should have zk or solr URL set up");
        }
    }

    public void close() throws IOException, SolrServerException {
        executor.shutdown();

        if (updateClient != null) {
            if (cloud) {
                flushAllUpdates(true);
            }

            client.commit();
            updateClient.commit();
            updateClient.close();
        }
    }

    // Helper classes to hold pending updates
    public static interface Update {}

    public static final class DocUpdate implements Update {
        public final SolrInputDocument doc;

        public DocUpdate(SolrInputDocument doc) {
            this.doc = doc;
        }
    }

    public static final class DeleteUpdate implements Update {
        public final String id;

        public DeleteUpdate(String id) {
            this.id = id;
        }
    }
}
