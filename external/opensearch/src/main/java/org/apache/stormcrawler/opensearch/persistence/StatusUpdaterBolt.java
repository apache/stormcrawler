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
package org.apache.stormcrawler.opensearch.persistence;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.opensearch.BulkItemResponseToFailedFlag;
import org.apache.stormcrawler.opensearch.Constants;
import org.apache.stormcrawler.opensearch.IndexCreation;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.apache.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.PerSecondReducer;
import org.apache.stormcrawler.util.URLPartitioner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple bolt which stores the status of URLs into OpenSearch. Takes the tuples coming from the
 * 'status' stream. To be used in combination with a Spout to read from the index.
 */
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt
        implements RemovalListener<String, List<Tuple>>, BulkProcessor.Listener {

    private static final Logger LOG = LoggerFactory.getLogger(StatusUpdaterBolt.class);

    private String OSBoltType = "status";

    private static final String OSStatusIndexNameParamName =
            Constants.PARAMPREFIX + "%s.index.name";
    private static final String OSStatusRoutingParamName = Constants.PARAMPREFIX + "%s.routing";
    private static final String OSStatusRoutingFieldParamName =
            Constants.PARAMPREFIX + "%s.routing.fieldname";

    private boolean routingFieldNameInMetadata = false;

    private String indexName;

    private URLPartitioner partitioner;

    /** whether to apply the same partitioning logic used for politeness for routing, e.g byHost */
    private boolean doRouting;

    /** Store the key used for routing explicitly as a field in metadata * */
    private String fieldNameForRoutingKey = null;

    private OpenSearchConnection connection;

    private Cache<String, List<Tuple>> waitAck;

    // Be fair due to cache timeout
    private final ReentrantLock waitAckLock = new ReentrantLock(true);

    private MultiCountMetric eventCounter;

    private MultiReducedMetric receivedPerSecMetrics;

    public StatusUpdaterBolt() {
        super();
    }

    /**
     * Loads the configuration using a substring different from the default value 'status' in order
     * to distinguish it from the spout configurations
     */
    public StatusUpdaterBolt(String boltType) {
        super();
        OSBoltType = boltType;
    }

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {

        super.prepare(stormConf, context, collector);

        indexName =
                ConfUtils.getString(
                        stormConf,
                        String.format(
                                Locale.ROOT,
                                StatusUpdaterBolt.OSStatusIndexNameParamName,
                                OSBoltType),
                        "status");

        doRouting =
                ConfUtils.getBoolean(
                        stormConf,
                        String.format(
                                Locale.ROOT,
                                StatusUpdaterBolt.OSStatusRoutingParamName,
                                OSBoltType),
                        false);

        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);

        fieldNameForRoutingKey =
                ConfUtils.getString(
                        stormConf,
                        String.format(
                                Locale.ROOT,
                                StatusUpdaterBolt.OSStatusRoutingFieldParamName,
                                OSBoltType));
        if (StringUtils.isNotBlank(fieldNameForRoutingKey)) {
            if (fieldNameForRoutingKey.startsWith("metadata.")) {
                routingFieldNameInMetadata = true;
                fieldNameForRoutingKey = fieldNameForRoutingKey.substring("metadata.".length());
            }
            // periods are not allowed in - replace with %2E
            fieldNameForRoutingKey = fieldNameForRoutingKey.replaceAll("\\.", "%2E");
        }

        String defaultSpec =
                String.format(
                        Locale.ROOT,
                        "expireAfterWrite=%ds",
                        ConfUtils.getInt(stormConf, "topology.message.timeout.secs", 300));

        String waitAckSpec =
                ConfUtils.getString(stormConf, "opensearch.status.waitack.cache.spec", defaultSpec);

        waitAck = Caffeine.from(waitAckSpec).removalListener(this).build();

        int metrics_time_bucket_secs = 30;

        // create gauge for waitAck
        context.registerMetric("waitAck", () -> waitAck.estimatedSize(), metrics_time_bucket_secs);

        // benchmarking - average number of items received back by Elastic per second
        this.receivedPerSecMetrics =
                context.registerMetric(
                        "average_persec",
                        new MultiReducedMetric(new PerSecondReducer()),
                        metrics_time_bucket_secs);

        this.eventCounter =
                context.registerMetric(
                        "counters", new MultiCountMetric(), metrics_time_bucket_secs);

        try {
            connection = OpenSearchConnection.getConnection(stormConf, OSBoltType, this);
        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }

        // use the default status schema if none has been specified
        try {
            IndexCreation.checkOrCreateIndex(connection.getClient(), indexName, OSBoltType, LOG);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanup() {
        if (connection == null) {
            return;
        }
        connection.close();
        connection = null;
    }

    @Override
    public void store(
            String url, Status status, Metadata metadata, Optional<Date> nextFetch, Tuple tuple)
            throws Exception {

        String documentID = getDocumentID(metadata, url);

        boolean isAlreadySentAndDiscovered;
        // need to synchronize: otherwise it might get added to the cache
        // without having been sent to OpenSearch
        waitAckLock.lock();
        try {
            // check that the same URL is not being sent to OpenSearch
            final var alreadySent = waitAck.getIfPresent(documentID);
            isAlreadySentAndDiscovered = status.equals(Status.DISCOVERED) && alreadySent != null;
        } finally {
            waitAckLock.unlock();
        }

        if (isAlreadySentAndDiscovered) {
            // if this object is discovered - adding another version of it
            // won't make any difference
            LOG.debug(
                    "Already being sent to OpenSearch {} with status {} and ID {}",
                    url,
                    status,
                    documentID);
            // ack straight away!
            eventCounter.scope("skipped").incrBy(1);
            super.ack(tuple, url);
            return;
        }

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.field("url", url);
        builder.field("status", status);

        builder.startObject("metadata");
        for (String mdKey : metadata.keySet()) {
            String[] values = metadata.getValues(mdKey);
            // periods are not allowed - replace with %2E
            mdKey = mdKey.replaceAll("\\.", "%2E");
            builder.array(mdKey, values);
        }

        String partitionKey = partitioner.getPartition(url, metadata);
        if (partitionKey == null) {
            partitionKey = "_DEFAULT_";
        }

        // store routing key in metadata?
        if (StringUtils.isNotBlank(fieldNameForRoutingKey) && routingFieldNameInMetadata) {
            builder.field(fieldNameForRoutingKey, partitionKey);
        }

        builder.endObject();

        // store routing key outside metadata?
        if (StringUtils.isNotBlank(fieldNameForRoutingKey) && !routingFieldNameInMetadata) {
            builder.field(fieldNameForRoutingKey, partitionKey);
        }

        if (nextFetch.isPresent()) {
            builder.timeField("nextFetchDate", nextFetch.get());
        }

        builder.endObject();

        IndexRequest request = new IndexRequest(getIndexName(metadata));

        // check that we don't overwrite an existing entry
        // When create is used, the index operation will fail if a document
        // by that id already exists in the index.
        final boolean create = status.equals(Status.DISCOVERED);
        request.source(builder).id(documentID).create(create);

        if (doRouting) {
            request.routing(partitionKey);
        }

        waitAckLock.lock();
        try {
            final List<Tuple> tt = waitAck.get(documentID, k -> new LinkedList<>());
            tt.add(tuple);
            LOG.debug("Added to waitAck {} with ID {} total {}", url, documentID, tt.size());
        } finally {
            waitAckLock.unlock();
        }

        LOG.debug("Sending to OpenSearch buffer {} with ID {}", url, documentID);

        connection.addToProcessor(request);
    }

    @Override
    public void onRemoval(
            @Nullable String key, @Nullable List<Tuple> value, @NotNull RemovalCause cause) {
        if (!cause.wasEvicted()) return;
        LOG.error("Purged from waitAck {} with {} values", key, value.size());
        for (Tuple t : value) {
            eventCounter.scope("purged").incrBy(1);
            collector.fail(t);
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        LOG.debug("afterBulk [{}] with {} responses", executionId, request.numberOfActions());
        eventCounter.scope("bulks_received").incrBy(1);
        eventCounter.scope("bulk_msec").incrBy(response.getTook().getMillis());
        eventCounter.scope("received").incrBy(request.numberOfActions());
        receivedPerSecMetrics.scope("received").update(request.numberOfActions());

        var idsToBulkItemsWithFailedFlag =
                Arrays.stream(response.getItems())
                        .map(
                                bir -> {
                                    String id = bir.getId();
                                    BulkItemResponse.Failure f = bir.getFailure();
                                    boolean failed = false;
                                    if (f != null) {
                                        // already discovered
                                        if (f.getStatus().equals(RestStatus.CONFLICT)) {
                                            eventCounter.scope("doc_conflicts").incrBy(1);
                                            LOG.debug("Doc conflict ID {}", id);
                                        } else {
                                            LOG.error("Update ID {}, failure: {}", id, f);
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
                    if (buwff.failed) ctFailed++;
                    else tmp = buwff;
                }
                if (ctFailed != bulkItemsWithFailedFlag.size()) {
                    LOG.warn(
                            "The id {} would result in an ack and a failure. Using only the ack for processing.",
                            id);
                }
                selected = Objects.requireNonNull(tmp);
            }

            if (associatedTuple != null) {
                LOG.debug("Acked {} tuple(s) for ID {}", associatedTuple.size(), id);
                for (Tuple tuple : associatedTuple) {
                    if (!selected.failed) {
                        String url = tuple.getStringByField("url");
                        ackCount++;
                        // ack and put in cache
                        LOG.debug("Acked {} with ID {}", url, id);
                        eventCounter.scope("acked").incrBy(1);
                        super.ack(tuple, url);
                    } else {
                        failureCount++;
                        eventCounter.scope("failed").incrBy(1);
                        collector.fail(tuple);
                    }
                }
            } else {
                LOG.warn("Could not find unacked tuple for {}", id);
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
    public void afterBulk(long executionId, BulkRequest request, Throwable throwable) {
        eventCounter.scope("bulks_received").incrBy(1);
        eventCounter.scope("received").incrBy(request.numberOfActions());
        receivedPerSecMetrics.scope("received").update(request.numberOfActions());
        LOG.error("Exception with bulk {} - failing the whole lot ", executionId, throwable);

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
                    collector.fail(x);
                }
            } else {
                LOG.warn("Could not find unacked tuple for {}", id);
            }
        }
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        LOG.debug("beforeBulk {} with {} actions", executionId, request.numberOfActions());
        eventCounter.scope("bulks_sent").incrBy(1);
    }

    /**
     * Must be overridden for implementing custom index names based on some metadata information By
     * Default, indexName coming from config is used
     */
    protected String getIndexName(Metadata m) {
        return indexName;
    }
}
