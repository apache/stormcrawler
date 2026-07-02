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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.indexing.AbstractIndexerBolt;
import org.apache.stormcrawler.metrics.CrawlerMetrics;
import org.apache.stormcrawler.metrics.ScopedCounter;
import org.apache.stormcrawler.metrics.ScopedReducedMetric;
import org.apache.stormcrawler.opensearch.AsyncBulkProcessor;
import org.apache.stormcrawler.opensearch.IndexCreation;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.apache.stormcrawler.opensearch.WaitAckCache;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.ConfUtils;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends documents to opensearch. Indexes all the fields from the tuples or a Map
 * &lt;String,Object&gt; from a named field.
 */
public class IndexerBolt extends AbstractIndexerBolt implements AsyncBulkProcessor.Listener {

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

    private ScopedCounter eventCounter;

    private OpenSearchConnection connection;

    private ScopedReducedMetric perSecMetrics;

    private WaitAckCache waitAck;

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
            LOG.error("Can't connect to OpenSearch", e1);
            throw new RuntimeException(e1);
        }

        this.eventCounter = CrawlerMetrics.registerCounter(context, conf, "OpensearchIndexer", 10);

        this.perSecMetrics =
                CrawlerMetrics.registerPerSecMetric(context, conf, "Indexer_average_persec", 10);

        waitAck = new WaitAckCache(LOG, _collector::fail);
        CrawlerMetrics.registerGauge(context, conf, "waitAck", waitAck::estimatedSize, 10);

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
            final Map<String, Object> source = new HashMap<>();

            // display text of the document?
            if (StringUtils.isNotBlank(fieldNameForText())) {
                final String text = trimText(tuple.getStringByField("text"));
                if (!ignoreEmptyFields() || StringUtils.isNotBlank(text)) {
                    source.put(fieldNameForText(), trimText(text));
                }
            }

            // send URL as field?
            if (StringUtils.isNotBlank(fieldNameForURL())) {
                source.put(fieldNameForURL(), normalisedurl);
            }

            // which metadata to display?
            final Map<String, String[]> keyVals = filterMetadata(metadata);

            for (Entry<String, String[]> entry : keyVals.entrySet()) {
                if (entry.getValue().length == 1) {
                    final String value = entry.getValue()[0];
                    if (!ignoreEmptyFields() || StringUtils.isNotBlank(value)) {
                        source.put(entry.getKey(), value);
                    }
                } else if (entry.getValue().length > 1) {
                    source.put(entry.getKey(), List.of(entry.getValue()));
                }
            }

            final String targetIndex = getIndexName(metadata);
            final BulkOperation op;
            if (create) {
                op =
                        BulkOperation.of(
                                b ->
                                        b.create(
                                                c -> {
                                                    c.index(targetIndex).id(docID).document(source);
                                                    if (pipeline != null) {
                                                        c.pipeline(pipeline);
                                                    }
                                                    return c;
                                                }));
            } else {
                op =
                        BulkOperation.of(
                                b ->
                                        b.index(
                                                idx -> {
                                                    idx.index(targetIndex)
                                                            .id(docID)
                                                            .document(source);
                                                    if (pipeline != null) {
                                                        idx.pipeline(pipeline);
                                                    }
                                                    return idx;
                                                }));
            }

            waitAck.addTuple(docID, tuple);

            connection.addToProcessor(op);

            eventCounter.scope("Indexed").incrBy(1);
            perSecMetrics.scope("Indexed").update(1);
        } catch (Exception e) {
            LOG.error("Error building document for OpenSearch", e);
            // do not send to status stream so that it gets replayed
            _collector.fail(tuple);

            waitAck.invalidate(docID);
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
        eventCounter.scope("bulk_msec").incrBy(response.took());

        waitAck.processBulkResponse(
                response,
                executionId,
                eventCounter,
                (id, t, selected) -> {
                    String url = (String) t.getValueByField("url");
                    Metadata metadata = (Metadata) t.getValueByField("metadata");

                    if (!selected.failed()) {
                        _collector.emit(
                                StatusStreamName, t, new Values(url, metadata, Status.FETCHED));
                        _collector.ack(t);
                    } else {
                        var failure = selected.getFailure();
                        LOG.error("update ID {}, URL {}, failure: {}", id, url, failure);
                        // there is something wrong with the content we should
                        // treat it as an ERROR
                        if (selected.getStatus() == 400) {
                            metadata.setValue(Constants.STATUS_ERROR_SOURCE, "OpenSearch indexing");
                            metadata.setValue(Constants.STATUS_ERROR_MESSAGE, "invalid content");
                            _collector.emit(
                                    StatusStreamName, t, new Values(url, metadata, Status.ERROR));
                            _collector.ack(t);
                            LOG.debug("Acked {} with ID {}", url, id);
                        } else {
                            _collector.fail(t);
                            LOG.debug("Failed {} with ID {}", url, id);
                        }
                    }
                });
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        eventCounter.scope("bulks_received").incrBy(1);
        waitAck.processFailedBulk(
                request,
                executionId,
                failure,
                t -> {
                    eventCounter.scope("failed").incrBy(1);
                    _collector.fail(t);
                });
    }
}
