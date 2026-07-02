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

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.metrics.CrawlerMetrics;
import org.apache.stormcrawler.metrics.ScopedCounter;
import org.apache.stormcrawler.metrics.ScopedReducedMetric;
import org.apache.stormcrawler.opensearch.AsyncBulkProcessor;
import org.apache.stormcrawler.opensearch.Constants;
import org.apache.stormcrawler.opensearch.IndexCreation;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.apache.stormcrawler.opensearch.WaitAckCache;
import org.apache.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.URLPartitioner;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple bolt which stores the status of URLs into OpenSearch. Takes the tuples coming from the
 * 'status' stream. To be used in combination with a Spout to read from the index.
 */
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt
        implements AsyncBulkProcessor.Listener {

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

    private WaitAckCache waitAck;

    private ScopedCounter eventCounter;

    private ScopedReducedMetric receivedPerSecMetrics;

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

        int metrics_time_bucket_secs = 30;

        // benchmarking - average number of items received back from OpenSearch per second
        this.receivedPerSecMetrics =
                CrawlerMetrics.registerPerSecMetric(
                        context, stormConf, "average_persec", metrics_time_bucket_secs);

        // eventCounter MUST be registered before WaitAckCache — the eviction lambda captures it
        this.eventCounter =
                CrawlerMetrics.registerCounter(
                        context, stormConf, "counters", metrics_time_bucket_secs);

        String defaultSpec =
                String.format(
                        Locale.ROOT,
                        "expireAfterWrite=%ds",
                        ConfUtils.getInt(stormConf, "topology.message.timeout.secs", 300));

        String waitAckSpec =
                ConfUtils.getString(stormConf, "opensearch.status.waitack.cache.spec", defaultSpec);

        waitAck =
                new WaitAckCache(
                        waitAckSpec,
                        LOG,
                        t -> {
                            eventCounter.scope("purged").incrBy(1);
                            collector.fail(t);
                        });
        CrawlerMetrics.registerGauge(
                context, stormConf, "waitAck", waitAck::estimatedSize, metrics_time_bucket_secs);

        try {
            connection = OpenSearchConnection.getConnection(stormConf, OSBoltType, this);
        } catch (Exception e1) {
            LOG.error("Can't connect to OpenSearch", e1);
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
        waitAck.shutdown();
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

        boolean isAlreadySentAndDiscovered =
                status.equals(Status.DISCOVERED) && waitAck.contains(documentID);

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

        Map<String, Object> doc = new HashMap<>();
        doc.put("url", url);
        doc.put("status", status.name());

        Map<String, Object> metadataMap = new HashMap<>();
        for (String mdKey : metadata.keySet()) {
            String[] values = metadata.getValues(mdKey);
            // periods are not allowed - replace with %2E
            mdKey = mdKey.replaceAll("\\.", "%2E");
            metadataMap.put(mdKey, List.of(values));
        }

        String partitionKey = partitioner.getPartition(url, metadata);
        if (partitionKey == null) {
            partitionKey = "_DEFAULT_";
        }

        // store routing key in metadata?
        if (StringUtils.isNotBlank(fieldNameForRoutingKey) && routingFieldNameInMetadata) {
            metadataMap.put(fieldNameForRoutingKey, partitionKey);
        }

        doc.put("metadata", metadataMap);

        // store routing key outside metadata?
        if (StringUtils.isNotBlank(fieldNameForRoutingKey) && !routingFieldNameInMetadata) {
            doc.put(fieldNameForRoutingKey, partitionKey);
        }

        if (nextFetch.isPresent()) {
            doc.put("nextFetchDate", nextFetch.get().toInstant().toString());
        }
        // check that we don't overwrite an existing entry
        // When create is used, the index operation will fail if a document
        // by that id already exists in the index.
        final boolean create = status.equals(Status.DISCOVERED);
        final String targetIndex = getIndexName(metadata);
        final String routing = doRouting ? partitionKey : null;

        BulkOperation op;
        if (create) {
            op =
                    BulkOperation.of(
                            b ->
                                    b.create(
                                            c -> {
                                                c.index(targetIndex).id(documentID).document(doc);
                                                if (routing != null) {
                                                    c.routing(routing);
                                                }
                                                return c;
                                            }));
        } else {
            op =
                    BulkOperation.of(
                            b ->
                                    b.index(
                                            idx -> {
                                                idx.index(targetIndex).id(documentID).document(doc);
                                                if (routing != null) {
                                                    idx.routing(routing);
                                                }
                                                return idx;
                                            }));
        }

        waitAck.addTuple(documentID, tuple);

        LOG.debug("Sending to OpenSearch buffer {} with ID {}", url, documentID);

        connection.addToProcessor(op);
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        LOG.debug("afterBulk [{}] with {} responses", executionId, request.operations().size());
        eventCounter.scope("bulks_received").incrBy(1);
        eventCounter.scope("bulk_msec").incrBy(response.took());
        eventCounter.scope("received").incrBy(request.operations().size());
        receivedPerSecMetrics.scope("received").update(request.operations().size());

        waitAck.processBulkResponse(
                response,
                executionId,
                eventCounter,
                (id, tuple, selected) -> {
                    if (!selected.failed()) {
                        String url = tuple.getStringByField("url");
                        LOG.debug("Acked {} with ID {}", url, id);
                        eventCounter.scope("acked").incrBy(1);
                        super.ack(tuple, url);
                    } else {
                        eventCounter.scope("failed").incrBy(1);
                        collector.fail(tuple);
                    }
                });
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable throwable) {
        eventCounter.scope("bulks_received").incrBy(1);
        eventCounter.scope("received").incrBy(request.operations().size());
        receivedPerSecMetrics.scope("received").update(request.operations().size());

        waitAck.processFailedBulk(
                request,
                executionId,
                throwable,
                t -> {
                    eventCounter.scope("failed").incrBy(1);
                    collector.fail(t);
                });
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        LOG.debug("beforeBulk {} with {} actions", executionId, request.operations().size());
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
