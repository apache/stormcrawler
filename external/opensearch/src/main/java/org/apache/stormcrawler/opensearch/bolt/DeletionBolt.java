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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.opensearch.BulkItemResponseToFailedFlag;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.apache.stormcrawler.util.ConfUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkProcessor.Listener;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.core.rest.RestStatus;
import org.slf4j.LoggerFactory;

/**
 * Deletes documents in OpenSearch. This should be connected to the StatusUpdaterBolt via the
 * 'deletion' stream and will remove the documents with a status of ERROR. Note that this component
 * will also try to delete documents even though they were never indexed and it currently won't
 * delete documents which were indexed under the canonical URL.
 */
public class DeletionBolt extends BaseRichBolt
        implements RemovalListener<String, List<Tuple>>, Listener {

    static final org.slf4j.Logger LOG =
            LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String BOLT_TYPE = "indexer";

    private OutputCollector _collector;

    private String indexName;

    private OpenSearchConnection connection;

    private Cache<String, List<Tuple>> waitAck;

    // Be fair due to cache timeout
    private final ReentrantLock waitAckLock = new ReentrantLock(true);

    public DeletionBolt() {}

    /** Sets the index name instead of taking it from the configuration. * */
    public DeletionBolt(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public void prepare(
            Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        if (indexName == null) {
            indexName = ConfUtils.getString(conf, IndexerBolt.OSIndexNameParamName, "content");
        }

        try {
            connection = OpenSearchConnection.getConnection(conf, BOLT_TYPE, this);
        } catch (Exception e1) {
            LOG.error("Can't connect to opensearch", e1);
            throw new RuntimeException(e1);
        }

        waitAck =
                Caffeine.newBuilder()
                        .expireAfterWrite(60, TimeUnit.SECONDS)
                        .removalListener(this)
                        .build();

        context.registerMetric("waitAck", () -> waitAck.estimatedSize(), 10);
    }

    public void onRemoval(
            @Nullable String key, @Nullable List<Tuple> value, @NotNull RemovalCause cause) {
        if (!cause.wasEvicted()) {
            return;
        }
        if (value != null) {
            LOG.error("Purged from waitAck {} with {} values", key, value.size());
            for (Tuple t : value) {
                _collector.fail(t);
            }
        } else {
            // This should never happen, but log it anyway.
            LOG.error("Purged from waitAck {} with no values", key);
        }
    }

    @Override
    public void cleanup() {
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        // keep it simple for now and ignore cases where the canonical URL was
        // used

        final String docID = getDocumentID(metadata, url);
        DeleteRequest dr = new DeleteRequest(getIndexName(metadata), docID);
        connection.addToProcessor(dr);

        waitAckLock.lock();
        try {
            List<Tuple> tt = waitAck.getIfPresent(docID);
            if (tt == null) {
                tt = new LinkedList<>();
                waitAck.put(docID, tt);
            }
            tt.add(tuple);
            LOG.debug("Added to waitAck {} with ID {} total {}", url, docID, tt.size());
        } finally {
            waitAckLock.unlock();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        // none
    }

    /**
     * Must be overridden for implementing custom index names based on some metadata information By
     * Default, indexName coming from config is used
     */
    protected String getIndexName(Metadata m) {
        return indexName;
    }

    /**
     * Get the document id.
     *
     * @param metadata The {@link Metadata}.
     * @param url The normalised url.
     * @return Return the normalised url SHA-256 digest as String.
     */
    protected String getDocumentID(Metadata metadata, String url) {
        return org.apache.commons.codec.digest.DigestUtils.sha256Hex(url);
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {}

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        var idsToBulkItemsWithFailedFlag =
                Arrays.stream(response.getItems())
                        .map(
                                bir -> {
                                    String id = bir.getId();
                                    BulkItemResponse.Failure f = bir.getFailure();
                                    boolean failed = false;
                                    if (f != null) {
                                        if (f.getStatus().equals(RestStatus.CONFLICT)) {
                                            LOG.debug("Doc conflict ID {}", id);
                                        } else {
                                            failed = true;
                                        }
                                    }
                                    return new BulkItemResponseToFailedFlag(bir, failed);
                                })
                        .collect(
                                // https://github.com/apache/stormcrawler/issues/832
                                Collectors.groupingBy(
                                        idWithFailedFlagTuple -> idWithFailedFlagTuple.id,
                                        Collectors.toUnmodifiableList()));
        Map<String, List<Tuple>> presentTuples;
        long estimatedSize;
        waitAckLock.lock();
        try {
            presentTuples = waitAck.getAllPresent(idsToBulkItemsWithFailedFlag.keySet());
            if (!presentTuples.isEmpty()) {
                waitAck.invalidateAll(presentTuples.keySet());
            }
            estimatedSize = waitAck.estimatedSize();
        } finally {
            waitAckLock.unlock();
        }

        int ackCount = 0;
        int failureCount = 0;

        for (var entry : presentTuples.entrySet()) {
            final var id = entry.getKey();
            final var associatedTuple = entry.getValue();
            final var bulkItemsWithFailedFlag = idsToBulkItemsWithFailedFlag.get(id);

            BulkItemResponseToFailedFlag selected;

            if (bulkItemsWithFailedFlag.size() == 1) {
                selected = bulkItemsWithFailedFlag.get(0);
            } else {
                // Fallback if there are multiple responses for the same id
                BulkItemResponseToFailedFlag tmp = null;
                var ctFailed = 0;
                for (var buwff : bulkItemsWithFailedFlag) {
                    if (tmp == null) {
                        tmp = buwff;
                    }
                    if (buwff.failed) {
                        ctFailed++;
                    } else {
                        tmp = buwff;
                    }
                }
                if (ctFailed != bulkItemsWithFailedFlag.size()) {
                    LOG.warn(
                            "The id {} would result in an ack and a failure. Using only the ack for processing.",
                            id);
                }
                selected = Objects.requireNonNull(tmp);
            }

            if (associatedTuple != null) {
                LOG.debug("Found {} tuple(s) for ID {}", associatedTuple.size(), id);
                for (Tuple t : associatedTuple) {
                    String url = (String) t.getValueByField("url");

                    Metadata metadata = (Metadata) t.getValueByField("metadata");

                    if (!selected.failed) {
                        ackCount++;
                        _collector.ack(t);
                    } else {
                        failureCount++;
                        var failure = selected.getFailure();
                        LOG.error("update ID {}, URL {}, failure: {}", id, url, failure);
                        _collector.fail(t);
                    }
                }
            } else {
                LOG.warn("Could not find unacked tuples for {}", entry.getKey());
            }
        }

        LOG.info(
                "Bulk response [{}] : items {}, waitAck {}, acked {}, failed {}",
                executionId,
                idsToBulkItemsWithFailedFlag.size(),
                estimatedSize,
                ackCount,
                failureCount);
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        LOG.error("Exception with bulk {} - failing the whole lot ", executionId, failure);

        final var failedIds =
                request.requests().stream()
                        .map(DocWriteRequest::id)
                        .collect(Collectors.toUnmodifiableSet());
        Map<String, List<Tuple>> failedTupleLists;
        waitAckLock.lock();
        try {
            failedTupleLists = waitAck.getAllPresent(failedIds);
            if (!failedTupleLists.isEmpty()) {
                waitAck.invalidateAll(failedTupleLists.keySet());
            }
        } finally {
            waitAckLock.unlock();
        }

        for (var id : failedIds) {
            var failedTuples = failedTupleLists.get(id);
            if (failedTuples != null) {
                LOG.debug("Failed {} tuple(s) for ID {}", failedTuples.size(), id);
                for (Tuple x : failedTuples) {
                    // fail it
                    _collector.fail(x);
                }
            } else {
                LOG.warn("Could not find unacked tuple for {}", id);
            }
        }
    }
}
