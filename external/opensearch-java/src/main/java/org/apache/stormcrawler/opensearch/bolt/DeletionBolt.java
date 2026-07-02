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

import java.lang.invoke.MethodHandles;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.metrics.CrawlerMetrics;
import org.apache.stormcrawler.opensearch.AsyncBulkProcessor;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.apache.stormcrawler.opensearch.WaitAckCache;
import org.apache.stormcrawler.util.ConfUtils;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.slf4j.LoggerFactory;

/**
 * Deletes documents in OpenSearch. This should be connected to the StatusUpdaterBolt via the
 * 'deletion' stream and will remove the documents with a status of ERROR. Note that this component
 * will also try to delete documents even though they were never indexed and it currently won't
 * delete documents which were indexed under the canonical URL.
 */
public class DeletionBolt extends BaseRichBolt implements AsyncBulkProcessor.Listener {

    static final org.slf4j.Logger LOG =
            LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String BOLT_TYPE = "indexer";

    private OutputCollector _collector;

    private String indexName;

    private OpenSearchConnection connection;

    private WaitAckCache waitAck;

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
            LOG.error("Can't connect to OpenSearch", e1);
            throw new RuntimeException(e1);
        }

        waitAck = new WaitAckCache(LOG, _collector::fail);
        CrawlerMetrics.registerGauge(context, conf, "waitAck", waitAck::estimatedSize, 10);
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
        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        // keep it simple for now and ignore cases where the canonical URL was
        // used

        final String docID = getDocumentID(metadata, url);
        final String targetIndex = getIndexName(metadata);
        BulkOperation op = BulkOperation.of(b -> b.delete(d -> d.index(targetIndex).id(docID)));

        waitAck.addTuple(docID, tuple);

        connection.addToProcessor(op);
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
        waitAck.processBulkResponse(
                response,
                executionId,
                null,
                (id, t, selected) -> {
                    if (!selected.failed()) {
                        _collector.ack(t);
                    } else {
                        String url = (String) t.getValueByField("url");
                        LOG.error(
                                "update ID {}, URL {}, failure: {}",
                                id,
                                url,
                                selected.getFailure());
                        _collector.fail(t);
                    }
                });
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        waitAck.processFailedBulk(request, executionId, failure, _collector::fail);
    }
}
