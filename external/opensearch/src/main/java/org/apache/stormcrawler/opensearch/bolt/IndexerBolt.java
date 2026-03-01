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

import static org.apache.stormcrawler.Constants.StatusStreamName;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.indexing.AbstractIndexerBolt;
import org.apache.stormcrawler.opensearch.BulkItemResponseToFailedFlag;
import org.apache.stormcrawler.opensearch.IndexCreation;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.PerSecondReducer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends documents to opensearch. Indexes all the fields from the tuples or a Map
 * &lt;String,Object&gt; from a named field.
 */
public class IndexerBolt extends AbstractIndexerBolt
        implements RemovalListener<String, List<Tuple>>, BulkProcessor.Listener {

    private static final Logger LOG = LoggerFactory.getLogger(IndexerBolt.class);

    private static final String OSBoltType = "indexer";

    static final String OSIndexNameParamName =
            org.apache.stormcrawler.opensearch.Constants.PARAMPREFIX + OSBoltType + ".index.name";
    private static final String OSCreateParamName =
            org.apache.stormcrawler.opensearch.Constants.PARAMPREFIX + OSBoltType + ".create";
    private static final String OSIndexPipelineParamName =
            org.apache.stormcrawler.opensearch.Constants.PARAMPREFIX + OSBoltType + ".pipeline";

    private OutputCollector _collector;

    private String indexName;

    private String pipeline;

    // whether the document will be created only if it does not exist or
    // overwritten
    private boolean create = false;

    private MultiCountMetric eventCounter;

    private OpenSearchConnection connection;

    private MultiReducedMetric perSecMetrics;

    private Cache<String, List<Tuple>> waitAck;

    // Be fair due to cache timeout
    private final ReentrantLock waitAckLock = new ReentrantLock(true);

    public IndexerBolt() {}

    /** Sets the index name instead of taking it from the configuration. * */
    public IndexerBolt(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public void prepare(
            Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
        _collector = collector;
        if (indexName == null) {
            indexName = ConfUtils.getString(conf, IndexerBolt.OSIndexNameParamName, "content");
        }

        create = ConfUtils.getBoolean(conf, IndexerBolt.OSCreateParamName, false);
        pipeline = ConfUtils.getString(conf, IndexerBolt.OSIndexPipelineParamName);

        try {
            connection = OpenSearchConnection.getConnection(conf, OSBoltType, this);
        } catch (Exception e1) {
            LOG.error("Can't connect to opensearch", e1);
            throw new RuntimeException(e1);
        }

        this.eventCounter = context.registerMetric("OpensearchIndexer", new MultiCountMetric(), 10);

        this.perSecMetrics =
                context.registerMetric(
                        "Indexer_average_persec",
                        new MultiReducedMetric(new PerSecondReducer()),
                        10);

        waitAck =
                Caffeine.newBuilder()
                        .expireAfterWrite(60, TimeUnit.SECONDS)
                        .removalListener(this)
                        .build();

        context.registerMetric("waitAck", () -> waitAck.estimatedSize(), 10);

        // use the default status schema if none has been specified
        try {
            IndexCreation.checkOrCreateIndex(connection.getClient(), indexName, OSBoltType, LOG);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

        final String url = tuple.getStringByField("url");

        // Distinguish the value used for indexing
        // from the one used for the status
        final String normalisedurl = valueForURL(tuple);

        LOG.info("Indexing {} as {}", url, normalisedurl);

        final Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        if (!filterDocument(metadata)) {
            LOG.info("Filtered {}", url);
            eventCounter.scope("Filtered").incrBy(1);
            // treat it as successfully processed even if
            // we do not index it
            _collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.FETCHED));
            _collector.ack(tuple);
            return;
        }

        final String docID = getDocumentID(metadata, normalisedurl);

        try {
            final XContentBuilder builder = jsonBuilder().startObject();

            // display text of the document?
            if (StringUtils.isNotBlank(fieldNameForText())) {
                final String text = trimText(tuple.getStringByField("text"));
                if (!ignoreEmptyFields() || StringUtils.isNotBlank(text)) {
                    builder.field(fieldNameForText(), trimText(text));
                }
            }

            // send URL as field?
            if (StringUtils.isNotBlank(fieldNameForURL())) {
                builder.field(fieldNameForURL(), normalisedurl);
            }

            // which metadata to display?
            final Map<String, String[]> keyVals = filterMetadata(metadata);

            for (Entry<String, String[]> entry : keyVals.entrySet()) {
                if (entry.getValue().length == 1) {
                    final String value = entry.getValue()[0];
                    if (!ignoreEmptyFields() || StringUtils.isNotBlank(value)) {
                        builder.field(entry.getKey(), value);
                    }
                } else if (entry.getValue().length > 1) {
                    builder.array(entry.getKey(), entry.getValue());
                }
            }

            builder.endObject();

            final IndexRequest indexRequest =
                    new IndexRequest(getIndexName(metadata))
                            .source(builder)
                            .id(docID)
                            .create(create);

            if (pipeline != null) {
                indexRequest.setPipeline(pipeline);
            }

            connection.addToProcessor(indexRequest);

            eventCounter.scope("Indexed").incrBy(1);
            perSecMetrics.scope("Indexed").update(1);

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
        } catch (IOException e) {
            LOG.error("Error building document for OpenSearch", e);
            // do not send to status stream so that it gets replayed
            _collector.fail(tuple);

            waitAckLock.lock();
            try {
                waitAck.invalidate(docID);
            } finally {
                waitAckLock.unlock();
            }
        }
    }

    /**
     * Must be overridden for implementing custom index names based on some metadata information By
     * Default, indexName coming from config is used
     */
    protected String getIndexName(Metadata m) {
        return indexName;
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        eventCounter.scope("bulks_sent").incrBy(1);
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        eventCounter.scope("bulks_received").incrBy(1);
        eventCounter.scope("bulk_msec").incrBy(response.getTook().getMillis());

        var idsToBulkItemsWithFailedFlag =
                Arrays.stream(response.getItems())
                        .map(
                                bir -> {
                                    String id = bir.getId();
                                    BulkItemResponse.Failure f = bir.getFailure();
                                    boolean failed = false;
                                    if (f != null) {
                                        if (f.getStatus().equals(RestStatus.CONFLICT)) {
                                            eventCounter.scope("doc_conflicts").incrBy(1);
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
        Set<String> debugInfo = null;
        waitAckLock.lock();
        try {
            presentTuples = waitAck.getAllPresent(idsToBulkItemsWithFailedFlag.keySet());
            if (!presentTuples.isEmpty()) {
                waitAck.invalidateAll(presentTuples.keySet());
            }
            estimatedSize = waitAck.estimatedSize();
            // Only if we have to.
            if (LOG.isDebugEnabled() && estimatedSize > 0L) {
                debugInfo = new HashSet<>(waitAck.asMap().keySet());
            }
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
                        _collector.emit(
                                StatusStreamName, t, new Values(url, metadata, Status.FETCHED));
                        _collector.ack(t);
                    } else {
                        failureCount++;
                        var failure = selected.getFailure();
                        LOG.error("update ID {}, URL {}, failure: {}", id, url, failure);
                        // there is something wrong with the content we should
                        // treat
                        // it as an ERROR
                        if (selected.getFailure().getStatus().equals(RestStatus.BAD_REQUEST)) {
                            metadata.setValue(Constants.STATUS_ERROR_SOURCE, "OpenSearch indexing");
                            metadata.setValue(Constants.STATUS_ERROR_MESSAGE, "invalid content");
                            _collector.emit(
                                    StatusStreamName, t, new Values(url, metadata, Status.ERROR));
                            _collector.ack(t);
                            LOG.debug("Acked {} with ID {}", url, id);
                        } else {
                            LOG.error("update ID {}, URL {}, failure: {}", id, url, failure);
                            // there is something wrong with the content we
                            // should
                            // treat
                            // it as an ERROR
                            if (failure.getStatus().equals(RestStatus.BAD_REQUEST)) {
                                metadata.setValue(
                                        Constants.STATUS_ERROR_SOURCE, "OpenSearch indexing");
                                metadata.setValue(
                                        Constants.STATUS_ERROR_MESSAGE, "invalid content");
                                _collector.emit(
                                        StatusStreamName,
                                        t,
                                        new Values(url, metadata, Status.ERROR));
                                _collector.ack(t);
                            } else {
                                // otherwise just fail it
                                _collector.fail(t);
                            }
                        }
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
        if (debugInfo != null) {
            for (String kinaw : debugInfo) {
                LOG.debug("Still in wait ack after bulk response [{}] => {}", executionId, kinaw);
            }
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        eventCounter.scope("bulks_received").incrBy(1);
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
                    eventCounter.scope("failed").incrBy(1);
                    _collector.fail(x);
                }
            } else {
                LOG.warn("Could not find unacked tuple for {}", id);
            }
        }
    }
}
